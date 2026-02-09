// Copyright 2018 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package metanode

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/depends/tiglabs/raft"
	raftproto "github.com/cubefs/cubefs/depends/tiglabs/raft/proto"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
)

// Apply applies the given operational commands.
func (mp *metaPartition) Apply(command []byte, index uint64) (resp interface{}, err error) {
	msg := &MetaItem{}
	defer func() {
		if r := recover(); r != nil {
			panicMsg := fmt.Sprintf("[metaPartition.Apply] mpId(%v) op(%v) occurred panic, err(%v), ",
				mp.config.PartitionId, msg.Op, r)
			log.LogWarn(panicMsg)
			panic(panicMsg)
		}

		if err == nil {
			mp.uploadApplyID(index)
		}
	}()
	if err = msg.UnmarshalJson(command); err != nil {
		return
	}

	mp.nonIdempotent.Lock()
	defer mp.nonIdempotent.Unlock()

	var dbWriteHandle interface{}
	if dbWriteHandle, err = mp.inodeTree.CreateBatchWriteHandle(); err != nil {
		log.LogErrorf("action[Apply] create write batch handle failed:%v", err)
		return
	}

	mp.inodeTree.SetApplyID(index)

	// NOTE: commit changes
	defer func() {
		if err != nil {
			_ = mp.inodeTree.ReleaseBatchWriteHandle(dbWriteHandle)
			return
		}

		log.LogDebugf("[Apply] mp(%v) commit write handle", mp.config.PartitionId)
		err = mp.inodeTree.CommitAndReleaseBatchWriteHandle(dbWriteHandle, true)
		if err != nil {
			log.LogErrorf("[Apply] failed to commit write batch, is disk broken? err(%v)", err)
		}
	}()

	switch msg.Op {
	case opFSMCreateInode:
		ino := NewInode(0, 0)
		if err = ino.Unmarshal(msg.V); err != nil {
			return
		}
		if mp.config.Cursor < ino.Inode {
			mp.config.Cursor = ino.Inode
			mp.inodeTree.SetCursor(ino.Inode)
		}
		resp, err = mp.fsmCreateInode(dbWriteHandle, ino)
	case opFSMCreateInodeQuota:
		qinode := &MetaQuotaInode{}
		if err = qinode.Unmarshal(msg.V); err != nil {
			return
		}
		ino := qinode.inode
		if mp.config.Cursor < ino.Inode {
			mp.config.Cursor = ino.Inode
			mp.inodeTree.SetCursor(ino.Inode)
		}
		if len(qinode.quotaIds) > 0 {
			mp.setInodeQuota(dbWriteHandle, qinode.quotaIds, ino.Inode)
		}
		resp, err = mp.fsmCreateInode(dbWriteHandle, ino)
	case opFSMUnlinkInode:
		ino := NewInode(0, 0)
		if err = ino.Unmarshal(msg.V); err != nil {
			return
		}
		var status uint8
		status, err = mp.inodeInTx(ino.Inode)
		if err != nil {
			status = proto.OpErr
		}
		if status != proto.OpOk {
			resp = &InodeResponse{Status: status}
			return
		}
		resp, err = mp.fsmUnlinkInode(dbWriteHandle, ino, 0)
	case opFSMUnlinkInodeOnce:
		var inoOnceWithVersion *InodeOnceWithVersion
		if inoOnceWithVersion, err = InodeOnceUnmarshal(msg.V); err != nil {
			return
		}

		var status uint8
		status, err = mp.inodeInTx(inoOnceWithVersion.Inode)
		if status != proto.OpOk {
			resp = &InodeResponse{Status: status}
			return
		}
		ino := NewInode(inoOnceWithVersion.Inode, 0)
		ino.setVer(inoOnceWithVersion.VerSeq)
		resp, err = mp.fsmUnlinkInode(dbWriteHandle, ino, inoOnceWithVersion.UniqID)
	case opFSMUnlinkInodeBatch:
		var inodes InodeBatch
		inodes, err = InodeBatchUnmarshal(msg.V)
		if err != nil {
			return nil, err
		}
		resp, err = mp.fsmUnlinkInodeBatch(dbWriteHandle, inodes)
	case opFSMExtentTruncate:
		ino := NewInode(0, 0)
		if err = ino.Unmarshal(msg.V); err != nil {
			return
		}
		resp, err = mp.fsmExtentsTruncate(dbWriteHandle, ino)
	case opFSMCreateLinkInode:
		var status uint8
		ino := NewInode(0, 0)
		if err = ino.Unmarshal(msg.V); err != nil {
			return
		}
		status, err = mp.inodeInTx(ino.Inode)
		if err != nil {
			status = proto.OpErr
		}
		if status != proto.OpOk {
			resp = &InodeResponse{Status: status}
			return
		}
		resp, err = mp.fsmCreateLinkInode(dbWriteHandle, ino, 0)
	case opFSMCreateLinkInodeOnce:
		var inoOnceWithVersion *InodeOnceWithVersion
		var status uint8
		if inoOnceWithVersion, err = InodeOnceUnmarshal(msg.V); err != nil {
			return
		}
		status, err = mp.inodeInTx(inoOnceWithVersion.Inode)
		if err != nil {
			status = proto.OpErr
		}
		if status != proto.OpOk {
			resp = &InodeResponse{Status: status}
			return
		}
		ino := NewInode(inoOnceWithVersion.Inode, 0)
		ino.setVer(inoOnceWithVersion.VerSeq)
		resp, err = mp.fsmCreateLinkInode(dbWriteHandle, ino, inoOnceWithVersion.UniqID)
	case opFSMEvictInode:
		var status uint8
		ino := NewInode(0, 0)
		if err = ino.Unmarshal(msg.V); err != nil {
			return
		}
		status, err = mp.inodeInTx(ino.Inode)
		if err != nil {
			status = proto.OpErr
		}
		if status != proto.OpOk {
			resp = &InodeResponse{Status: status}
			return
		}
		resp, err = mp.fsmEvictInode(dbWriteHandle, ino)
	case opFSMEvictInodeBatch:
		var inodes InodeBatch
		inodes, err = InodeBatchUnmarshal(msg.V)
		if err != nil {
			return nil, err
		}
		resp, err = mp.fsmBatchEvictInode(dbWriteHandle, inodes)
	case opFSMSetAttr:
		req := &SetattrRequest{}
		err = json.Unmarshal(msg.V, req)
		if err != nil {
			return
		}
		err = mp.fsmSetAttr(dbWriteHandle, req)
	case opFSMCreateDentry:
		var status uint8
		den := &Dentry{}
		if err = den.Unmarshal(msg.V); err != nil {
			return
		}

		status, err = mp.dentryInTx(den.ParentId, den.Name)
		if err != nil {
			status = proto.OpErr
		}
		if status != proto.OpOk {
			resp = status
			return
		}

		resp, err = mp.fsmCreateDentry(dbWriteHandle, den, false)
	case opFSMDeleteDentry:
		var status uint8
		den := &Dentry{}
		if err = den.Unmarshal(msg.V); err != nil {
			return
		}

		status, err = mp.dentryInTx(den.ParentId, den.Name)
		if err != nil {
			status = proto.OpErr
		}
		if status != proto.OpOk {
			resp = &DentryResponse{
				Status: status,
			}
			return
		}

		resp, err = mp.fsmDeleteDentry(dbWriteHandle, den, false)
	case opFSMDeleteDentryBatch:
		var db DentryBatch
		db, err = DentryBatchUnmarshal(msg.V)
		if err != nil {
			return nil, err
		}
		resp, err = mp.fsmBatchDeleteDentry(dbWriteHandle, db)
	case opFSMUpdateDentry:
		var status uint8
		den := &Dentry{}
		if err = den.Unmarshal(msg.V); err != nil {
			return
		}

		status, err = mp.dentryInTx(den.ParentId, den.Name)
		if err != nil {
			status = proto.OpErr
		}
		if status != proto.OpOk {
			resp = &DentryResponse{Status: status}
			return
		}

		resp, err = mp.fsmUpdateDentry(dbWriteHandle, den)
	case opFSMUpdatePartition:
		req := &UpdatePartitionReq{}
		if err = json.Unmarshal(msg.V, req); err != nil {
			return
		}
		resp, err = mp.fsmUpdatePartition(req.End)
	case opFSMExtentsAdd:
		ino := NewInode(0, 0)
		if err = ino.Unmarshal(msg.V); err != nil {
			return
		}
		resp, err = mp.fsmAppendExtents(dbWriteHandle, ino)
	case opFSMExtentsAddWithCheck:
		ino := NewInode(0, 0)
		if err = ino.Unmarshal(msg.V); err != nil {
			return
		}
		resp, err = mp.fsmAppendExtentsWithCheck(dbWriteHandle, ino, false)
	case opFSMExtentSplit:
		ino := NewInode(0, 0)
		if err = ino.Unmarshal(msg.V); err != nil {
			return
		}
		resp, err = mp.fsmAppendExtentsWithCheck(dbWriteHandle, ino, true)
	case opFSMObjExtentsAdd:
		ino := NewInode(0, 0)
		if err = ino.Unmarshal(msg.V); err != nil {
			return
		}
		resp, err = mp.fsmAppendObjExtents(dbWriteHandle, ino)
	case opFSMSentToChan:
		resp = mp.fsmSendToChan(msg.V, false)
	case opFSMSentToChanWithVer:
		resp = mp.fsmSendToChan(msg.V, true)
	case opFSMStoreTick:
		quotaRebuild := mp.mqMgr.statisticRebuildStart()
		uidRebuild := mp.acucumRebuildStart()
		uniqId := mp.GetUniqId()
		uniqChecker := mp.uniqChecker.clone()
		// NOTE: already got lock
		var snap Snapshot
		snap, err = mp.GetSnapShot()
		if err != nil {
			log.LogErrorf("[Apply]: failed to open snapshot for mp(%v), store(%v), err(%v)", mp.config.PartitionId, mp.config.StoreMode, err)
			return
		}

		msg := &storeMsg{
			command:      opFSMStoreTick,
			snap:         snap,
			quotaRebuild: quotaRebuild,
			uidRebuild:   uidRebuild,
			uniqId:       uniqId,
			uniqChecker:  uniqChecker,
			multiVerList: mp.GetAllVerList(),
			applyIndex:   index,
		}
		log.LogDebugf("opFSMStoreTick: quotaRebuild [%v] uidRebuild [%v]", quotaRebuild, uidRebuild)
		mp.storeChan <- msg
	case opFSMInternalDeleteInode:
		err = mp.internalDelete(msg.V)
	case opFSMInternalDeleteInodeBatch:
		err = mp.internalDeleteBatch(dbWriteHandle, msg.V)
	case opFSMInternalDelExtentFile:
		err = mp.delOldExtentFile(msg.V)
	case opFSMInternalDelExtentCursor:
		err = mp.setExtentDeleteFileCursor(msg.V)
	case opFSMSetXAttr:
		var extend *Extend
		if extend, err = NewExtendFromBytes(msg.V); err != nil {
			return
		}
		err = mp.fsmSetXAttr(dbWriteHandle, extend)
	case opFSMRemoveXAttr:
		var extend *Extend
		if extend, err = NewExtendFromBytes(msg.V); err != nil {
			return
		}
		err = mp.fsmRemoveXAttr(dbWriteHandle, extend)
	case opFSMUpdateXAttr:
		var extend *Extend
		if extend, err = NewExtendFromBytes(msg.V); err != nil {
			return
		}
		err = mp.fsmSetXAttr(dbWriteHandle, extend)
	case opFSMLockDir:
		req := &proto.LockDirRequest{}
		if err = json.Unmarshal(msg.V, req); err != nil {
			return
		}
		resp = mp.fsmLockDir(dbWriteHandle, req)
	case opFSMCreateMultipart:
		var multipart *Multipart
		multipart = MultipartFromBytes(msg.V)
		resp, err = mp.fsmCreateMultipart(dbWriteHandle, multipart)
	case opFSMRemoveMultipart:
		var multipart *Multipart
		multipart = MultipartFromBytes(msg.V)
		resp, err = mp.fsmRemoveMultipart(dbWriteHandle, multipart)
	case opFSMAppendMultipart:
		var multipart *Multipart
		multipart = MultipartFromBytes(msg.V)
		resp, err = mp.fsmAppendMultipart(dbWriteHandle, multipart)
	case opFSMSyncCursor:
		var cursor uint64
		cursor = binary.BigEndian.Uint64(msg.V)
		if cursor > mp.config.Cursor {
			mp.config.Cursor = cursor
			mp.inodeTree.SetCursor(cursor)
		}
	case opFSMSyncTxID:
		var txID uint64
		txID = binary.BigEndian.Uint64(msg.V)
		if txID > mp.txProcessor.txManager.txIdAlloc.getTransactionID() {
			mp.txProcessor.txManager.txIdAlloc.setTransactionID(txID)
			mp.txProcessor.txManager.txTree.SetTxId(txID)
		}
	case opFSMTxInit:
		txInfo := proto.NewTransactionInfo(0, 0)
		if err = txInfo.Unmarshal(msg.V); err != nil {
			return
		}
		resp, err = mp.fsmTxInit(dbWriteHandle, txInfo)
	case opFSMTxCreateInode:
		txIno := NewTxInode(0, 0, nil)
		if err = txIno.Unmarshal(msg.V); err != nil {
			return
		}
		if mp.config.Cursor < txIno.Inode.Inode {
			mp.config.Cursor = txIno.Inode.Inode
			mp.inodeTree.SetCursor(txIno.Inode.Inode)
		}
		resp, err = mp.fsmTxCreateInode(dbWriteHandle, txIno, []uint32{})
	case opFSMTxCreateInodeQuota:
		qinode := &TxMetaQuotaInode{}
		if err = qinode.Unmarshal(msg.V); err != nil {
			return
		}
		txIno := qinode.txinode
		if mp.config.Cursor < txIno.Inode.Inode {
			mp.config.Cursor = txIno.Inode.Inode
			mp.inodeTree.SetCursor(txIno.Inode.Inode)
		}
		if len(qinode.quotaIds) > 0 {
			mp.setInodeQuota(dbWriteHandle, qinode.quotaIds, txIno.Inode.Inode)
		}
		resp, err = mp.fsmTxCreateInode(dbWriteHandle, txIno, qinode.quotaIds)
	case opFSMTxCreateDentry:
		txDen := NewTxDentry(0, "", 0, 0, nil, nil)
		if err = txDen.Unmarshal(msg.V); err != nil {
			return
		}
		resp, err = mp.fsmTxCreateDentry(dbWriteHandle, txDen)
	case opFSMTxSetState:
		req := &proto.TxSetStateRequest{}
		if err = json.Unmarshal(msg.V, req); err != nil {
			return
		}
		resp, err = mp.fsmTxSetState(dbWriteHandle, req)
	case opFSMTxCommitRM:
		req := &proto.TransactionInfo{}
		if err = req.Unmarshal(msg.V); err != nil {
			return
		}
		resp, err = mp.fsmTxCommitRM(dbWriteHandle, req)
	case opFSMTxRollbackRM:
		req := &proto.TransactionInfo{}
		if err = req.Unmarshal(msg.V); err != nil {
			return
		}
		resp, err = mp.fsmTxRollbackRM(dbWriteHandle, req)
	case opFSMTxCommit:
		req := &proto.TxApplyRequest{}
		if err = json.Unmarshal(msg.V, req); err != nil {
			return
		}
		resp, err = mp.fsmTxCommit(dbWriteHandle, req.TxID)
	case opFSMTxRollback:
		req := &proto.TxApplyRequest{}
		if err = json.Unmarshal(msg.V, req); err != nil {
			return
		}
		resp, err = mp.fsmTxRollback(dbWriteHandle, req.TxID)
	case opFSMTxDelete:
		req := &proto.TxApplyRequest{}
		if err = json.Unmarshal(msg.V, req); err != nil {
			return
		}
		resp, err = mp.fsmTxDelete(dbWriteHandle, req.TxID)
	case opFSMTxDeleteDentry:
		txDen := NewTxDentry(0, "", 0, 0, nil, nil)
		if err = txDen.Unmarshal(msg.V); err != nil {
			return
		}
		resp, err = mp.fsmTxDeleteDentry(dbWriteHandle, txDen)
	case opFSMTxUnlinkInode:
		txIno := NewTxInode(0, 0, nil)
		if err = txIno.Unmarshal(msg.V); err != nil {
			return
		}
		resp, err = mp.fsmTxUnlinkInode(dbWriteHandle, txIno)
	case opFSMTxUpdateDentry:
		// txDen := NewTxDentry(0, "", 0, 0, nil)
		txUpdateDen := NewTxUpdateDentry(nil, nil, nil)
		if err = txUpdateDen.Unmarshal(msg.V); err != nil {
			return
		}
		resp, err = mp.fsmTxUpdateDentry(dbWriteHandle, txUpdateDen)
	case opFSMTxCreateLinkInode:
		txIno := NewTxInode(0, 0, nil)
		if err = txIno.Unmarshal(msg.V); err != nil {
			return
		}
		resp, err = mp.fsmTxCreateLinkInode(dbWriteHandle, txIno)
	case opFSMSetInodeQuotaBatch:
		req := &proto.BatchSetMetaserverQuotaReuqest{}
		if err = json.Unmarshal(msg.V, req); err != nil {
			return
		}
		resp = mp.fsmSetInodeQuotaBatch(dbWriteHandle, req)
	case opFSMDeleteInodeQuotaBatch:
		req := &proto.BatchDeleteMetaserverQuotaReuqest{}
		if err = json.Unmarshal(msg.V, req); err != nil {
			return
		}
		resp = mp.fsmDeleteInodeQuotaBatch(dbWriteHandle, req)
	case opFSMUniqID:
		resp = mp.fsmUniqID(msg.V)
	case opFSMUniqCheckerEvict:
		req := &fsmEvictUniqCheckerRequest{}
		if err = json.Unmarshal(msg.V, req); err != nil {
			return
		}
		err = mp.fsmUniqCheckerEvict(req)
	case opFSMVersionOp:
		if index <= mp.multiVerApplyId {
			log.LogWarnf("action[opFSMVersionOp] mp[%v] applyId [%v] <= multiVerApplyId [%v], skip",
				mp.config.PartitionId, index, mp.multiVerApplyId)
			return
		}
		err = mp.fsmVersionOp(msg.V)
	case opFSMRenewalForbiddenMigration:
		ino := NewInode(0, 0)
		if err = ino.Unmarshal(msg.V); err != nil {
			return
		}
		resp = mp.fsmRenewalInodeForbiddenMigration(ino)
	case opFSMUpdateExtentKeyAfterMigration:
		ino := NewInode(0, 0)
		if err = ino.Unmarshal(msg.V); err != nil {
			log.LogWarnf("[Apply] mp(%v) opFSMUpdateExtentKeyAfterMigration Unmarshal inode failed: %v",
				mp.config.PartitionId, err.Error())
			return
		}
		resp = mp.fsmUpdateExtentKeyAfterMigration(ino)
	case opFSMSetInodeCreateTime:
		req := &SetCreateTimeRequest{}
		err = json.Unmarshal(msg.V, req)
		if err != nil {
			return
		}
		err = mp.fsmSetCreateTime(dbWriteHandle, req)
	case opFSMInternalBatchFreeInodeMigrationExtentKey:
		err = mp.fsmInternalBatchFreeMigrationExtentKey(msg.V)
	case opFSMSetMigrationExtentKeyDeleteImmediately:
		ino := NewInode(0, 0)
		if err = ino.Unmarshal(msg.V); err != nil {
			log.LogWarnf("[Apply] mp(%v) opFSMSetMigrationExtentKeyDeleteImmediately Unmarshal inode failed: %v",
				mp.config.PartitionId, err.Error())
			return
		}
		resp = mp.fsmSetMigrationExtentKeyDeleteImmediately(ino)
	case opFSMUpdateInodeMeta:
		req := &UpdateInodeMetaRequest{}
		err = json.Unmarshal(msg.V, req)
		if err != nil {
			return
		}
		err = mp.fsmUpdateInodeMeta(req)
	case opFSMSetFreeze:
		req := &SetFreezeReq{}
		if err = json.Unmarshal(msg.V, req); err != nil {
			return
		}
		resp, err = mp.fsmSetFreeze(req.Freeze)
	case opFSMCalcMetaPartitionMd5Sum:
		uniqId := mp.GetUniqId()
		uniqChecker := mp.uniqChecker.clone()
		// NOTE: already got lock
		var snap Snapshot
		snap, err = mp.GetSnapShot()
		if err != nil {
			log.LogErrorf("[Apply]: failed to open snapshot for mp(%v), store(%v), err(%v)", mp.config.PartitionId, mp.config.StoreMode, err)
			return
		}
		msg := &storeMsg{
			command:      opFSMCalcMetaPartitionMd5Sum,
			snap:         snap,
			quotaRebuild: false,
			uidRebuild:   false,
			uniqId:       uniqId,
			uniqChecker:  uniqChecker,
			multiVerList: mp.GetAllVerList(),
			applyIndex:   index,
		}
		mp.storeChan <- msg
	default:
		// do nothing
	case opFSMSyncInodeAccessTime:
		ino := NewInode(0, 0)
		if err = ino.Unmarshal(msg.V); err != nil {
			return
		}
		resp = mp.fsmSyncInodeAccessTime(dbWriteHandle, ino)
	case opFSMBatchSyncInodeATime:
		if len(msg.V) < 8 || len(msg.V)%8 != 0 {
			err = fmt.Errorf("opFSMBatchSyncInodeATime: msg is not valid, mp %d, len(%d)", mp.config.PartitionId, len(msg.V))
			return
		}

		resp = mp.fsmBatchSyncInodeAccessTime(dbWriteHandle, msg.V)
	}
	return
}

func (mp *metaPartition) runVersionOp() {
	mp.verUpdateChan = make(chan []byte, 100)
	for {
		select {
		case verData := <-mp.verUpdateChan:
			mp.submit(opFSMVersionOp, verData)
		case <-mp.stopC:
			log.LogWarnf("runVersionOp exit!")
			return
		}
	}
}

func (mp *metaPartition) fsmVersionOp(reqData []byte) (err error) {
	if mp.manager != nil && mp.manager.metaNode != nil && !mp.manager.metaNode.clusterEnableSnapshot {
		err = fmt.Errorf("clusterEnableSnapshot not enabled")
		log.LogErrorf("action[fsmVersionOp] mp[%v] err %v", mp.config.PartitionId, err)
		return nil
	}
	mp.multiVersionList.RWLock.Lock()
	defer mp.multiVersionList.RWLock.Unlock()

	var opData VerOpData
	if err = json.Unmarshal(reqData, &opData); err != nil {
		log.LogErrorf("action[fsmVersionOp] mp[%v] unmarshal error %v", mp.config.PartitionId, err)
		return
	}

	log.LogInfof("action[fsmVersionOp] volname [%v] mp[%v] seq [%v], op [%v]", mp.config.VolName, mp.config.PartitionId, opData.VerSeq, opData.Op)
	if opData.Op == proto.CreateVersionPrepare {
		cnt := len(mp.multiVersionList.VerList)
		if cnt > 0 {
			lastVersion := mp.multiVersionList.VerList[cnt-1]
			if lastVersion.Ver > opData.VerSeq {
				log.LogWarnf("action[HandleVersionOp] createVersionPrepare reqeust seq [%v] less than last exist snapshot seq [%v]", opData.VerSeq, lastVersion.Ver)
				return
			} else if lastVersion.Ver == opData.VerSeq {
				log.LogWarnf("action[HandleVersionOp] CreateVersionPrepare request seq [%v] already exist status [%v]", opData.VerSeq, lastVersion.Status)
				return
			}
		}
		newVer := &proto.VolVersionInfo{
			Status: proto.VersionPrepare,
			Ver:    opData.VerSeq,
		}
		mp.verSeq = opData.VerSeq
		mp.multiVersionList.VerList = append(mp.multiVersionList.VerList, newVer)

		log.LogInfof("action[fsmVersionOp] updateVerList mp[%v] seq [%v], op [%v], seqArray size %v", mp.config.PartitionId, opData.VerSeq, opData.Op, len(mp.multiVersionList.VerList))
	} else if opData.Op == proto.CreateVersionCommit {
		cnt := len(mp.multiVersionList.VerList)
		if cnt > 0 {
			if mp.multiVersionList.VerList[cnt-1].Ver > opData.VerSeq {
				log.LogWarnf("action[fsmVersionOp] mp[%v] reqeust seq [%v] less than last exist snapshot seq [%v]", mp.config.PartitionId,
					opData.VerSeq, mp.multiVersionList.VerList[cnt-1].Ver)
				return
			}
			if mp.multiVersionList.VerList[cnt-1].Ver == opData.VerSeq {
				if mp.multiVersionList.VerList[cnt-1].Status != proto.VersionPrepare {
					log.LogWarnf("action[fsmVersionOp] mp[%v] reqeust seq [%v] Equal last exist snapshot seq [%v] but with status [%v]", mp.config.PartitionId,
						mp.multiVersionList.VerList[cnt-1].Ver, opData.VerSeq, mp.multiVersionList.VerList[cnt-1].Status)
				}
				mp.multiVersionList.VerList[cnt-1].Status = proto.VersionNormal
				return
			}
		}
		newVer := &proto.VolVersionInfo{
			Status: proto.VersionNormal,
			Ver:    opData.VerSeq,
		}
		mp.verSeq = opData.VerSeq
		mp.multiVersionList.VerList = append(mp.multiVersionList.VerList, newVer)

		log.LogInfof("action[fsmVersionOp] updateVerList mp[%v] seq [%v], op [%v], seqArray size %v", mp.config.PartitionId, opData.VerSeq, opData.Op, len(mp.multiVersionList.VerList))
	} else if opData.Op == proto.DeleteVersion {
		for i, ver := range mp.multiVersionList.VerList {
			if i == len(mp.multiVersionList.VerList)-1 {
				log.LogWarnf("action[fsmVersionOp] mp[%v] seq [%v], op [%v], seqArray size %v newest ver [%v] reque ver [%v]",
					mp.config.PartitionId, opData.VerSeq, opData.Op, len(mp.multiVersionList.VerList), ver.Ver, opData.VerSeq)
				break
			}
			if ver.Ver == opData.VerSeq {
				log.LogInfof("action[fsmVersionOp] updateVerList mp[%v] seq [%v], op [%v], VerList %v",
					mp.config.PartitionId, opData.VerSeq, opData.Op, mp.multiVersionList.VerList)
				// mp.multiVersionList = append(mp.multiVersionList[:i], mp.multiVersionList[i+1:]...)
				mp.multiVersionList.VerList = append(mp.multiVersionList.VerList[:i], mp.multiVersionList.VerList[i+1:]...)
				log.LogInfof("action[fsmVersionOp] updateVerList mp[%v] seq [%v], op [%v], VerList %v",
					mp.config.PartitionId, opData.VerSeq, opData.Op, mp.multiVersionList.VerList)
				break
			}
		}
	} else if opData.Op == proto.SyncBatchVersionList {
		log.LogInfof("action[fsmVersionOp] mp[%v] before update:with seq [%v] verlist %v opData.VerList %v",
			mp.config.PartitionId, mp.verSeq, mp.multiVersionList.VerList, opData.VerList)

		lastVer := mp.multiVersionList.GetLastVer()
		for _, info := range opData.VerList {
			if info.Ver > lastVer {
				mp.multiVersionList.VerList = append(mp.multiVersionList.VerList, info)
				log.LogInfof("action[fsmVersionOp] updateVerList mp[%v] after update:with seq [%v] verlist %v",
					mp.config.PartitionId, mp.verSeq, mp.multiVersionList.VerList)
			}
		}
		mp.verSeq = mp.multiVersionList.GetLastVer()
		log.LogInfof("action[fsmVersionOp] updateVerList mp[%v] after update:with seq [%v] verlist %v",
			mp.config.PartitionId, mp.verSeq, mp.multiVersionList.VerList)
	} else {
		log.LogErrorf("action[fsmVersionOp] mp[%v] with seq [%v] process op type %v seq [%v] not found",
			mp.config.PartitionId, mp.verSeq, opData.Op, opData.VerSeq)
	}
	return
}

// ApplyMemberChange  apply changes to the raft member.
func (mp *metaPartition) ApplyMemberChange(confChange *raftproto.ConfChange, index uint64) (resp interface{}, err error) {
	mp.nonIdempotent.Lock()
	defer mp.nonIdempotent.Unlock()

	defer func() {
		if err == nil {
			mp.uploadApplyID(index)
		}
	}()

	log.LogWarnf("action[ApplyMemberChange] mp[%v] confChange[%v]", mp.config.PartitionId, confChange)
	// change memory status
	var (
		updated bool
	)
	switch confChange.Type {
	case raftproto.ConfAddNode:
		req := &proto.AddMetaPartitionRaftMemberRequest{}
		if err = json.Unmarshal(confChange.Context, req); err != nil {
			return
		}
		updated, err = mp.confAddNode(req, index)
		log.LogWarnf("action[ApplyMemberChange] mp[%v] confAddNode updated[%v] err[%v]", mp.config.PartitionId, updated, err)
	case raftproto.ConfRemoveNode:
		req := &proto.RemoveMetaPartitionRaftMemberRequest{}
		if err = json.Unmarshal(confChange.Context, req); err != nil {
			return
		}
		updated, err = mp.confRemoveNode(req, index)
		log.LogWarnf("action[ApplyMemberChange] mp[%v] confRemoveNode updated[%v] err[%v]", mp.config.PartitionId, updated, err)
	case raftproto.ConfAddLearner:
		req := &proto.AddMetaPartitionRaftMemberRequest{}
		if err = json.Unmarshal(confChange.Context, req); err != nil {
			return
		}
		updated, err = mp.confAddLearner(req, index)
		log.LogWarnf("action[ApplyMemberChange] mp[%v] confAddLearner updated[%v] err[%v]", mp.config.PartitionId, updated, err)
	case raftproto.ConfPromoteLearner:
		req := &proto.AddMetaPartitionRaftMemberRequest{}
		if err = json.Unmarshal(confChange.Context, req); err != nil {
			return
		}
		updated, err = mp.confPromoteLearner(req, index)
		log.LogWarnf("action[ApplyMemberChange] mp[%v] confPromoteLearner updated[%v] err[%v]", mp.config.PartitionId, updated, err)
	case raftproto.ConfUpdateNode:
		// updated, err = mp.confUpdateNode(req, index)
	default:
		log.LogWarnf("action[ApplyMemberChange] mp[%v] unknown conf change type %v.", mp.config.PartitionId, confChange.Type)
		// do nothing
	}
	if err != nil {
		return
	}
	if updated {
		mp.config.sortPeers()
		if err = mp.persistMetadata(); err != nil {
			log.LogErrorf("action[ApplyMemberChange] err[%v].", err)
			return
		}
	}
	return
}

// Snapshot returns the snapshot of the current meta partition.
func (mp *metaPartition) Snapshot() (snap raftproto.Snapshot, err error) {
	snap, err = newMetaItemIterator(mp)
	return
}

const (
	// Reduce snapshot apply tail-latency by batching RocksDB commits.
	// This avoids "each record commit" that can block for a long time (flush/compaction),
	// which in turn can stall iter.Next() and trigger leader-side write timeouts.
	applySnapBatchMaxItems       = 10000
	applySnapBatchMaxBytes       = 64 * 1024 * 1024
	applySnapSlowNextThreshold   = 5 * time.Second
	applySnapSlowCommitThreshold = 5 * time.Second
	itemChSize                   = 100 // Expanded channel buffer for concurrent consumption
	applySnapNumWorkers          = 4   // Number of concurrent workers
)

// snapshotItem represents a single snapshot entry with its index
type snapshotItem struct {
	data  []byte
	index int
}

// applySnapshotAggregator aggregates metadata from concurrent workers during snapshot apply
type applySnapshotAggregator struct {
	appIndexID uint64
	txID       uint64
	cursor     uint64
	uniqID     uint64
	verList    []*proto.VolVersionInfo
	uniqChk    *uniqChecker
}

func newApplySnapshotAggregator() *applySnapshotAggregator {
	return &applySnapshotAggregator{
		uniqChk: newUniqChecker(),
	}
}

func (a *applySnapshotAggregator) atomicMaxUint64(addr *uint64, val uint64) {
	for {
		old := atomic.LoadUint64(addr)
		if val <= old {
			break
		}
		if atomic.CompareAndSwapUint64(addr, old, val) {
			break
		}
	}
}

// applySnapshotProducer reads snapshot entries from iter and pushes them to dataCh
func (mp *metaPartition) applySnapshotProducer(
	iter raftproto.SnapIterator,
	dataCh chan<- *snapshotItem,
	errCh chan<- error,
	doneCh <-chan struct{},
) {
	defer close(dataCh)
	idx := 0

	for {
		// Check stop signal
		select {
		case <-doneCh:
			return
		default:
		}

		if mp.raftClosed() {
			select {
			case errCh <- fmt.Errorf("partition(%v) is closed", mp.config.PartitionId):
			default:
			}
			return
		}

		// Read next item
		nextStart := time.Now()
		data, err := iter.Next()
		if err != nil {
			if err == io.EOF {
				return
			}
			log.LogErrorf("ApplySnapshot: iter.Next failed, partitionID(%v) index(%v) err(%v)",
				mp.config.PartitionId, idx, err)
			select {
			case errCh <- err:
			default:
			}
			return
		}

		nextCost := time.Since(nextStart)
		if nextCost >= applySnapSlowNextThreshold {
			log.LogWarnf("ApplySnapshot: iter.Next slow, partitionID(%v) index(%v) cost(%s)",
				mp.config.PartitionId, idx, nextCost.String())
		}

		// Special handling for index==0 (compatibility: may be uint64 appIndexID)
		if idx == 0 {
			appIndexID := binary.BigEndian.Uint64(data)
			if log.EnableDebug() {
				log.LogDebugf("ApplySnapshot: partitionID(%v), temporary uint64 appIndexID:%v",
					mp.config.PartitionId, appIndexID)
			}
		}

		// Push to channel
		select {
		case dataCh <- &snapshotItem{data: data, index: idx}:
			idx++
		case <-doneCh:
			return
		}
	}
}

// processSnapshotItem processes a single snapshot item based on its operation type
func (mp *metaPartition) processSnapshotItem(
	workerID int,
	snap *MetaItem,
	agg *applySnapshotAggregator,
	dbWriteHandle interface{},
	leaderSnapFormatVer *uint32,
) error {
	switch snap.Op {
	case opFSMApplyId:
		val := binary.BigEndian.Uint64(snap.V)
		atomic.StoreUint64(&agg.appIndexID, val)
		if log.EnableDebug() {
			log.LogDebugf("ApplySnapshot: worker(%d) partitionID(%v) appIndexID:%v", workerID, mp.config.PartitionId, val)
		}

	case opFSMTxId:
		val := binary.BigEndian.Uint64(snap.V)
		agg.atomicMaxUint64(&agg.txID, val)
		if log.EnableDebug() {
			log.LogDebugf("ApplySnapshot: worker(%d) partitionID(%v) txID:%v", workerID, mp.config.PartitionId, val)
		}

	case opFSMCursor:
		val := binary.BigEndian.Uint64(snap.V)
		agg.atomicMaxUint64(&agg.cursor, val)
		if log.EnableDebug() {
			log.LogDebugf("ApplySnapshot: worker(%d) partitionID(%v) cursor:%v", workerID, mp.config.PartitionId, val)
		}

	case opFSMUniqIDSnap:
		val := binary.BigEndian.Uint64(snap.V)
		agg.atomicMaxUint64(&agg.uniqID, val)
		if log.EnableDebug() {
			log.LogDebugf("ApplySnapshot: worker(%d) partitionID(%v) uniqId:%v", workerID, mp.config.PartitionId, val)
		}

	case opFSMCreateInode:
		// Extract inode ID for cursor tracking
		inodeID := binary.BigEndian.Uint64(snap.K)
		agg.atomicMaxUint64(&agg.cursor, inodeID)

		// For RocksDB follower receiving from memory leader, use zero-copy path
		if mp.inodeTree.GetStoreMode() == proto.StoreModeRocksDb {
			if inodeRocks, ok := mp.inodeTree.(*InodeRocks); ok {
				if err := inodeRocks.InsertForMemoryToRocksbSnapshot(dbWriteHandle, snap.K, snap.V); err != nil {
					log.LogErrorf("ApplySnapshot: worker(%d) create inode failed, partitionID(%v) inodeID(%v) err(%v)",
						workerID, mp.config.PartitionId, inodeID, err)
					return err
				}
				if log.EnableDebug() {
					log.LogDebugf("ApplySnapshot: worker(%d) create inode: partitonID(%v) inodeID[%v].",
						workerID, mp.config.PartitionId, inodeID)
				}
				return nil
			}
		}

		// Fallback to unmarshal path for memory mode or if type assertion fails
		ino := NewInode(0, 0)
		if err := ino.UnmarshalKey(snap.K); err != nil {
			return err
		}
		if err := ino.UnmarshalValue(snap.V); err != nil {
			return err
		}
		if err := mp.inodeTree.Insert(dbWriteHandle, ino); err != nil {
			log.LogErrorf("ApplySnapshot: worker(%d) create inode failed, partitionID(%v) inode(%v)",
				workerID, mp.config.PartitionId, ino)
			return err
		}
		if log.EnableDebug() {
			log.LogDebugf("ApplySnapshot: worker(%d) create inode: partitonID(%v) inode[%v].",
				workerID, mp.config.PartitionId, ino)
		}

	case opFSMCreateDentry:
		// For RocksDB follower receiving from memory leader, use zero-copy path
		if mp.dentryTree.GetStoreMode() == proto.StoreModeRocksDb {
			if dentryRocks, ok := mp.dentryTree.(*DentryRocks); ok {
				if err := dentryRocks.InsertForMemoryToRocksbSnapshot(dbWriteHandle, snap.K, snap.V); err != nil {
					log.LogErrorf("ApplySnapshot: worker(%d) create dentry failed, partitionID(%v) err(%v)",
						workerID, mp.config.PartitionId, err)
					return err
				}
				if log.EnableDebug() {
					log.LogDebugf("ApplySnapshot: worker(%d) create dentry: partitionID(%v)",
						workerID, mp.config.PartitionId)
				}
				return nil
			}
		}

		// Fallback to unmarshal path for memory mode or if type assertion fails
		dentry := &Dentry{}
		if err := dentry.UnmarshalKey(snap.K); err != nil {
			return err
		}
		if err := dentry.UnmarshalValue(snap.V); err != nil {
			return err
		}
		if err := mp.dentryTree.Insert(dbWriteHandle, dentry); err != nil {
			log.LogErrorf("ApplySnapshot: worker(%d) create dentry failed, partitionID(%v) dentry(%v) error(%v)",
				workerID, mp.config.PartitionId, dentry, err)
			return err
		}
		if log.EnableDebug() {
			log.LogDebugf("ApplySnapshot: worker(%d) create dentry: partitionID(%v) dentry(%v)",
				workerID, mp.config.PartitionId, dentry)
		}

	case opFSMSetXAttr:
		extend, err := NewExtendFromBytes(snap.V)
		if err != nil {
			return err
		}
		if err := mp.extendTree.Insert(dbWriteHandle, extend); err != nil {
			return err
		}
		if log.EnableDebug() {
			log.LogDebugf("ApplySnapshot: worker(%d) set extend attributes: partitionID(%v) extend(%v)",
				workerID, mp.config.PartitionId, extend)
		}

	case opFSMCreateMultipart:
		multipart := MultipartFromBytes(snap.V)
		if err := mp.multipartTree.Insert(dbWriteHandle, multipart); err != nil {
			return err
		}
		if log.EnableDebug() {
			log.LogDebugf("ApplySnapshot: worker(%d) create multipart: partitionID(%v) multipart(%v)",
				workerID, mp.config.PartitionId, multipart)
		}

	case opFSMTxSnapshot:
		txInfo := proto.NewTransactionInfo(0, proto.TxTypeUndefined)
		if err := txInfo.Unmarshal(snap.V); err != nil {
			log.LogErrorf("[ApplySnapshot] worker(%d) mp(%v) failed to unmarshal tx, err(%v)",
				workerID, mp.config.PartitionId, err)
			return err
		}
		if err := mp.txProcessor.txManager.txTree.Insert(dbWriteHandle, txInfo); err != nil {
			log.LogErrorf("ApplySnapshot: worker(%d) put tx failed, partitionID(%v) tx(%v) err(%v)",
				workerID, mp.config.PartitionId, txInfo, err)
			return err
		}
		if log.EnableDebug() {
			log.LogDebugf("ApplySnapshot: worker(%d) create transaction: partitionID(%v) txInfo(%v)",
				workerID, mp.config.PartitionId, txInfo)
		}

	case opFSMTxRbInodeSnapshot:
		txRbInode := NewTxRollbackInode(nil, []uint32{}, nil, 0)
		if err := txRbInode.Unmarshal(snap.V); err != nil {
			log.LogErrorf("[ApplySnapshot] worker(%d) mp(%v) failed to unmarshal tx rb inode, err(%v)",
				workerID, mp.config.PartitionId, err)
			return err
		}
		if err := mp.txProcessor.txResource.txRbInodeTree.Insert(dbWriteHandle, txRbInode); err != nil {
			log.LogErrorf("ApplySnapshot: worker(%d) put rb inode failed, partitionID(%v) rb inode(%v) err(%v)",
				workerID, mp.config.PartitionId, txRbInode, err)
			return err
		}
		if log.EnableDebug() {
			log.LogDebugf("ApplySnapshot: worker(%d) create txRbInode: partitionID(%v) txRbinode[%v]",
				workerID, mp.config.PartitionId, txRbInode)
		}

	case opFSMTxRbDentrySnapshot:
		txRbDentry := NewTxRollbackDentry(nil, nil, 0)
		if err := txRbDentry.Unmarshal(snap.V); err != nil {
			log.LogErrorf("[ApplySnapshot] worker(%d) mp(%v) failed to unmarshal tx rb dentry, err(%v)",
				workerID, mp.config.PartitionId, err)
			return err
		}
		if err := mp.txProcessor.txResource.txRbDentryTree.Insert(dbWriteHandle, txRbDentry); err != nil {
			log.LogErrorf("ApplySnapshot: worker(%d) put rb dentry failed, partitionID(%v) rb dentry(%v) err(%v)",
				workerID, mp.config.PartitionId, txRbDentry, err)
			return err
		}
		if log.EnableDebug() {
			log.LogDebugf("ApplySnapshot: worker(%d) create txRbDentry: partitionID(%v) txRbDentry(%v)",
				workerID, mp.config.PartitionId, txRbDentry)
		}

	case opFSMVerListSnapShot:
		var verList []*proto.VolVersionInfo
		json.Unmarshal(snap.V, &verList)
		agg.verList = verList
		if log.EnableDebug() {
			log.LogDebugf("ApplySnapshot: worker(%d) create verList: partitionID(%v) verList(%v)",
				workerID, mp.config.PartitionId, verList)
		}

	case opExtentFileSnapshot:
		fileName := string(snap.K)
		fileName = path.Join(mp.config.RootDir, fileName)
		if err := os.WriteFile(fileName, snap.V, 0o644); err != nil {
			log.LogErrorf("ApplySnapshot: worker(%d) write snap extent delete file fail: partitionID(%v) err(%v)",
				workerID, mp.config.PartitionId, err)
		}
		if log.EnableDebug() {
			log.LogDebugf("ApplySnapshot: worker(%d) write snap extent delete file: partitonID(%v) filename(%v).",
				workerID, mp.config.PartitionId, fileName)
		}

	case opFSMUniqCheckerSnap:
		// Direct unmarshal - only one uniqChecker per snapshot, no lock needed
		if err := agg.uniqChk.UnMarshal(snap.V); err != nil {
			log.LogErrorf("ApplyUniqChecker: worker(%d) write snap uniqChecker fail", workerID)
			return err
		}
		if log.EnableDebug() {
			log.LogDebugf("ApplySnapshot: worker(%d) write snap uniqChecker", workerID)
		}

	// Zero-copy raw data operations
	case opFSMRawInodeData:
		// PutRaw will handle unmarshaling for memory mode or direct write for RocksDB mode
		if err := mp.inodeTree.PutRaw(dbWriteHandle, snap.K, snap.V); err != nil {
			log.LogErrorf("ApplySnapshot: worker(%d) put raw inode failed, partitionID(%v) err(%v)",
				workerID, mp.config.PartitionId, err)
			return err
		}
		// Extract inode ID from key for cursor tracking
		var inodeID uint64
		if len(snap.K) == 17 && snap.K[8] == byte(InodeTable) {
			// RocksDB full format: partitionId (8) + tableType (1) + inode_id (8)
			inodeID = binary.BigEndian.Uint64(snap.K[9:17])
			agg.atomicMaxUint64(&agg.cursor, inodeID)
		} else {
			return errors.New("invalid inode key")
		}

	case opFSMRawDentryData:
		// PutRaw will handle unmarshaling for memory mode or direct write for RocksDB mode
		if err := mp.dentryTree.PutRaw(dbWriteHandle, snap.K, snap.V); err != nil {
			log.LogErrorf("ApplySnapshot: worker(%d) put raw dentry failed, partitionID(%v) err(%v)",
				workerID, mp.config.PartitionId, err)
			return err
		}

	case opFSMRawExtendData:
		// PutRaw will handle unmarshaling for memory mode or direct write for RocksDB mode
		if err := mp.extendTree.PutRaw(dbWriteHandle, snap.K, snap.V); err != nil {
			log.LogErrorf("ApplySnapshot: worker(%d) put raw extend failed, partitionID(%v) err(%v)",
				workerID, mp.config.PartitionId, err)
			return err
		}

	case opFSMRawMultipartData:
		// PutRaw will handle unmarshaling for memory mode or direct write for RocksDB mode
		if err := mp.multipartTree.PutRaw(dbWriteHandle, snap.K, snap.V); err != nil {
			log.LogErrorf("ApplySnapshot: worker(%d) put raw multipart failed, partitionID(%v) err(%v)",
				workerID, mp.config.PartitionId, err)
			return err
		}

	case opFSMRawTxData:
		// PutRaw will handle unmarshaling for memory mode or direct write for RocksDB mode
		if err := mp.txProcessor.txManager.txTree.PutRaw(dbWriteHandle, snap.K, snap.V); err != nil {
			log.LogErrorf("ApplySnapshot: worker(%d) put raw tx failed, partitionID(%v) err(%v)",
				workerID, mp.config.PartitionId, err)
			return err
		}

	case opFSMRawTxRbInodeData:
		// PutRaw will handle unmarshaling for memory mode or direct write for RocksDB mode
		if err := mp.txProcessor.txResource.txRbInodeTree.PutRaw(dbWriteHandle, snap.K, snap.V); err != nil {
			log.LogErrorf("ApplySnapshot: worker(%d) put raw tx rb inode failed, partitionID(%v) err(%v)",
				workerID, mp.config.PartitionId, err)
			return err
		}

	case opFSMRawTxRbDentryData:
		// PutRaw will handle unmarshaling for memory mode or direct write for RocksDB mode
		if err := mp.txProcessor.txResource.txRbDentryTree.PutRaw(dbWriteHandle, snap.K, snap.V); err != nil {
			log.LogErrorf("ApplySnapshot: worker(%d) put raw tx rb dentry failed, partitionID(%v) err(%v)",
				workerID, mp.config.PartitionId, err)
			return err
		}

	default:
		ver := atomic.LoadUint32(leaderSnapFormatVer)
		if ver != math.MaxUint32 && ver > mp.manager.metaNode.raftSyncSnapFormatVersion {
			log.LogWarnf("ApplySnapshot: worker(%d) unknown op=%d, leaderSnapFormatVer:%v, mySnapFormatVer:%v, skip it",
				workerID, snap.Op, ver, mp.manager.metaNode.raftSyncSnapFormatVersion)
		} else {
			return fmt.Errorf("unknown Op=%d", snap.Op)
		}
	}

	return nil
}

// applySnapshotWorker is a concurrent worker that processes snapshot items
func (mp *metaPartition) applySnapshotWorker(
	workerID int,
	dataCh <-chan *snapshotItem,
	agg *applySnapshotAggregator,
	index0Done chan struct{},
	index0Once *sync.Once,
	leaderSnapFormatVer *uint32,
	errCh chan<- error,
	doneCh <-chan struct{},
) {
	// Create independent write batch handle
	dbWriteHandle, err := mp.inodeTree.CreateBatchWriteHandle()
	if err != nil {
		log.LogErrorf("ApplySnapshot: worker(%d) create batch write handle failed:%v", workerID, err)
		select {
		case errCh <- err:
		default:
		}
		return
	}
	defer mp.inodeTree.ReleaseBatchWriteHandle(dbWriteHandle)

	batchItems := 0
	batchBytes := 0

	flushBatch := func() error {
		if batchItems == 0 {
			return nil
		}
		start := time.Now()
		// Commit without applyID (will be committed once at the end)
		if err := mp.inodeTree.CommitBatchWrite(dbWriteHandle, false); err != nil {
			log.LogErrorf("ApplySnapshot: worker(%d) commit batch write failed, err(%v)", workerID, err)
			return err
		}
		if err := mp.inodeTree.ClearBatchWriteHandle(dbWriteHandle); err != nil {
			log.LogErrorf("ApplySnapshot: worker(%d) clear batch write handle failed, err(%v)", workerID, err)
			return err
		}
		cost := time.Since(start)
		if cost >= applySnapSlowCommitThreshold {
			log.LogWarnf("ApplySnapshot: worker(%d) slow commit, items(%d) bytes(%d) cost(%s)",
				workerID, batchItems, batchBytes, cost.String())
		}
		batchItems = 0
		batchBytes = 0
		return nil
	}

	signalErr := func(e error) {
		select {
		case errCh <- e:
		default:
		}
	}

	for item := range dataCh {
		// Check for errors
		select {
		case <-doneCh:
			return
		default:
		}

		index := item.index
		data := item.data

		// Handle index==0 (format version or compatibility)
		if index == 0 {
			var processed bool
			index0Once.Do(func() {
				processed = true
				snap := NewMetaItem(0, nil, nil)
				if err := snap.UnmarshalBinary(data); err != nil {
					// Compatibility: old format, index=0 is uint64 appIndexID
					log.LogInfof("ApplySnapshot: snap.UnmarshalBinary failed in index=0, partitionID(%v), assuming snapshot format version_0",
						mp.config.PartitionId)
					atomic.StoreUint32(leaderSnapFormatVer, SnapFormatVersion_0)
					atomic.StoreUint64(&agg.appIndexID, binary.BigEndian.Uint64(data))
					close(index0Done)
					return
				}

				if snap.Op != opFSMSnapFormatVersion {
					err := fmt.Errorf("snapshot format not match, partitionID(%v), index:%v, expect snap.Op:%v, actual snap.Op:%v",
						mp.config.PartitionId, index, opFSMSnapFormatVersion, snap.Op)
					log.LogWarnf("ApplySnapshot: %v", err.Error())
					signalErr(err)
					return
				}
				ver := binary.BigEndian.Uint32(snap.V)
				atomic.StoreUint32(leaderSnapFormatVer, ver)
				if ver != mp.manager.metaNode.raftSyncSnapFormatVersion {
					log.LogWarnf("ApplySnapshot: snapshot format not match, partitionID(%v), index:%v, expect ver:%v, actual ver:%v",
						mp.config.PartitionId, index, mp.manager.metaNode.raftSyncSnapFormatVersion, ver)
				}
				close(index0Done)
			})
			if processed {
				continue
			}
			// Other workers skip index==0
			continue
		}

		// Wait for index==0 processing to complete
		<-index0Done

		// Decode snapshot item
		snap := NewMetaItem(0, nil, nil)
		if err := snap.UnmarshalBinary(data); err != nil {
			log.LogErrorf("ApplySnapshot: worker(%d) unmarshal failed, partitionID(%v) index(%v) err(%v)",
				workerID, mp.config.PartitionId, index, err)
			signalErr(fmt.Errorf("unmarshal snap data failed: %w", err))
			return
		}

		// Process snapshot item
		if err := mp.processSnapshotItem(workerID, snap, agg, dbWriteHandle, leaderSnapFormatVer); err != nil {
			signalErr(err)
			return
		}

		batchItems++
		batchBytes += len(snap.K) + len(snap.V)

		needFlush := batchItems >= applySnapBatchMaxItems || batchBytes >= applySnapBatchMaxBytes
		if needFlush {
			if err := flushBatch(); err != nil {
				signalErr(err)
				return
			}
		}
	}

	// Flush remaining batch
	if err := flushBatch(); err != nil {
		signalErr(err)
		return
	}
}

func (mp *metaPartition) ApplySnapshot(peers []raftproto.Peer, iter raftproto.SnapIterator) (err error) {
	// Clear partition data
	err = mp.Clear()
	if err != nil {
		log.LogErrorf("[ApplySnapshot] mp(%v) failed to clear data, err(%v)", mp.config.PartitionId, err)
		return
	}

	log.LogWarnf("ApplySnapshot: start apply snapshot, partition(%v)", mp.config.PartitionId)

	// Start pipeline for memory mode (only for memory mode, managed directly here)
	var pipelinesStarted bool
	if mp.inodeTree.GetStoreMode() == proto.StoreModeMem {
		mp.startSnapshotPipelines()
		pipelinesStarted = true
	}

	// Initialize aggregator and channels
	agg := newApplySnapshotAggregator()
	dataCh := make(chan *snapshotItem, itemChSize)
	errCh := make(chan error, 1)
	doneCh := make(chan struct{})
	index0Done := make(chan struct{})
	var index0Once sync.Once
	leaderSnapFormatVer := uint32(math.MaxUint32)

	// Start producer
	go mp.applySnapshotProducer(iter, dataCh, errCh, doneCh)

	// Start workers
	var wg sync.WaitGroup
	for workerID := 0; workerID < applySnapNumWorkers; workerID++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			mp.applySnapshotWorker(id, dataCh, agg, index0Done, &index0Once, &leaderSnapFormatVer, errCh, doneCh)
		}(workerID)
	}

	// Wait for all workers to complete
	wg.Wait()

	// Stop pipelines before checking errors to ensure all data is inserted
	// This is critical because finalizeApplySnapshot may read from BTree (e.g., GetSnapShot)
	if pipelinesStarted {
		mp.stopSnapshotPipelines()
	}

	// Check for errors
	select {
	case err = <-errCh:
		log.LogErrorf("ApplySnapshot: stop with error: partitionID(%v) err(%v)", mp.config.PartitionId, err)
		return err
	default:
	}

	// Apply aggregated metadata
	return mp.finalizeApplySnapshot(agg)
}

// startSnapshotPipelines starts insert pipelines for all BTree instances in memory mode
func (mp *metaPartition) startSnapshotPipelines() {
	if mp.inodeTree.GetStoreMode() != proto.StoreModeMem {
		return
	}

	// Start pipeline for each BTree that will be used during snapshot apply
	if inodeBTree, ok := mp.inodeTree.(*InodeBTree); ok {
		inodeBTree.BTree.startPipelineForSnapshot()
	}
	if dentryBTree, ok := mp.dentryTree.(*DentryBTree); ok {
		dentryBTree.BTree.startPipelineForSnapshot()
	}
	if extendBTree, ok := mp.extendTree.(*ExtendBTree); ok {
		extendBTree.BTree.startPipelineForSnapshot()
	}
	if multipartBTree, ok := mp.multipartTree.(*MultipartBTree); ok {
		multipartBTree.BTree.startPipelineForSnapshot()
	}
	if txTree, ok := mp.txProcessor.txManager.txTree.(*TransactionBTree); ok {
		txTree.BTree.startPipelineForSnapshot()
	}
	if txRbInodeTree, ok := mp.txProcessor.txResource.txRbInodeTree.(*TransactionRollbackInodeBTree); ok {
		txRbInodeTree.BTree.startPipelineForSnapshot()
	}
	if txRbDentryTree, ok := mp.txProcessor.txResource.txRbDentryTree.(*TransactionRollbackDentryBTree); ok {
		txRbDentryTree.BTree.startPipelineForSnapshot()
	}
}

// stopSnapshotPipelines stops all insert pipelines and waits for completion
func (mp *metaPartition) stopSnapshotPipelines() {
	if mp.inodeTree.GetStoreMode() != proto.StoreModeMem {
		return
	}

	// Stop pipeline for each BTree
	if inodeBTree, ok := mp.inodeTree.(*InodeBTree); ok {
		inodeBTree.BTree.stopPipelineForSnapshot()
	}
	if dentryBTree, ok := mp.dentryTree.(*DentryBTree); ok {
		dentryBTree.BTree.stopPipelineForSnapshot()
	}
	if extendBTree, ok := mp.extendTree.(*ExtendBTree); ok {
		extendBTree.BTree.stopPipelineForSnapshot()
	}
	if multipartBTree, ok := mp.multipartTree.(*MultipartBTree); ok {
		multipartBTree.BTree.stopPipelineForSnapshot()
	}
	if txTree, ok := mp.txProcessor.txManager.txTree.(*TransactionBTree); ok {
		txTree.BTree.stopPipelineForSnapshot()
	}
	if txRbInodeTree, ok := mp.txProcessor.txResource.txRbInodeTree.(*TransactionRollbackInodeBTree); ok {
		txRbInodeTree.BTree.stopPipelineForSnapshot()
	}
	if txRbDentryTree, ok := mp.txProcessor.txResource.txRbDentryTree.(*TransactionRollbackDentryBTree); ok {
		txRbDentryTree.BTree.stopPipelineForSnapshot()
	}
}

// finalizeApplySnapshot applies aggregated metadata and commits final state
func (mp *metaPartition) finalizeApplySnapshot(agg *applySnapshotAggregator) error {
	appIndexID := atomic.LoadUint64(&agg.appIndexID)
	txID := atomic.LoadUint64(&agg.txID)
	cursor := atomic.LoadUint64(&agg.cursor)
	uniqID := atomic.LoadUint64(&agg.uniqID)

	verList := agg.verList
	uniqChecker := agg.uniqChk

	log.LogWarnf("ApplySnapshot: apply snapshot success, partition(%v), applyId(%v)", mp.config.PartitionId, appIndexID)

	// Update partition metadata
	mp.applyID = appIndexID
	mp.inodeTree.SetApplyID(appIndexID)
	mp.config.UniqId = uniqID
	mp.txProcessor.txManager.txIdAlloc.setTransactionID(txID)
	mp.txProcessor.txManager.txTree.SetTxId(txID)
	mp.config.Cursor = cursor
	mp.inodeTree.SetCursor(cursor)
	mp.uniqChecker = uniqChecker
	mp.multiVersionList.VerList = make([]*proto.VolVersionInfo, len(verList))
	copy(mp.multiVersionList.VerList, verList)
	mp.verSeq = mp.multiVersionList.GetLastVer()
	log.LogInfof("mp[%v] updateVerList (%v) seq [%v]", mp.config.PartitionId, mp.multiVersionList.VerList, mp.verSeq)

	// Final commit with applyID
	finalHandle, err := mp.inodeTree.CreateBatchWriteHandle()
	if err != nil {
		log.LogErrorf("[ApplySnapshot] mp(%v) failed to create final write handle", mp.config.PartitionId)
		return err
	}
	defer mp.inodeTree.ReleaseBatchWriteHandle(finalHandle)

	err = mp.inodeTree.CommitBatchWrite(finalHandle, true)
	if err != nil {
		log.LogErrorf("[ApplySnapshot] mp(%v) failed to write mp metadata", mp.config.PartitionId)
		return err
	}
	log.LogWarnf("ApplySnapshot: commit batch write success, partition(%v), applyId(%v)", mp.config.PartitionId, appIndexID)

	err = mp.inodeTree.ClearBatchWriteHandle(finalHandle)
	if err != nil {
		log.LogErrorf("[ApplySnapshot] mp(%v) failed to clear handle", mp.config.PartitionId)
		// Continue despite error
	}

	err = mp.flushAndCheckApplyID(appIndexID)
	if err != nil {
		log.LogErrorf("[ApplySnapshot] mp(%v) flush and check apply id failed, err(%v)", mp.config.PartitionId, err)
		return err
	}
	log.LogWarnf("ApplySnapshot: flush and check apply id success, partition(%v), applyId(%v)", mp.config.PartitionId, appIndexID)

	// Store snapshot
	snap, err := mp.GetSnapShot()
	if err != nil {
		log.LogErrorf("[ApplySnapshot]: failed to open snapshot for mp(%v), store(%v), err(%v)", mp.config.PartitionId, mp.config.StoreMode, err)
		return err
	}
	mp.storeChan <- &storeMsg{
		command:      opFSMStoreTick,
		uniqId:       mp.GetUniqId(),
		uniqChecker:  uniqChecker.clone(),
		multiVerList: mp.GetVerList(),
		snap:         snap,
	}

	// Block until store snapshot completes
	return mp.blockUntilStoreSnapshot(appIndexID)
}

// blockUntilStoreSnapshot waits for snapshot to be persisted to disk
func (mp *metaPartition) blockUntilStoreSnapshot(appIndexID uint64) error {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	log.LogWarnf("ApplySnapshot: start to block until store snapshot to disk, mp[%v], appid %d", mp.config.PartitionId, appIndexID)
	start := time.Now()

	for {
		select {
		case <-ticker.C:
			if time.Since(start) > time.Minute*20 {
				msg := fmt.Sprintf("ApplySnapshot: wait store snapshot timeout after 20 minutes, mp %d, appId %d, storeId %d",
					mp.config.PartitionId, appIndexID, mp.storedApplyId)
				log.LogErrorf(msg)
				return fmt.Errorf(msg)
			}

			if mp.raftClosed() {
				log.LogWarnf("ApplySnapshot-blockUntilStoreSnapshot partition(%v) is closed, exit now", mp.config.PartitionId)
				return fmt.Errorf("partition(%v) is closed", mp.config.PartitionId)
			}

			msg := fmt.Sprintf("ApplySnapshot: start check storedApplyId, mp %d appId %d, storeAppId %d, cost %s",
				mp.config.PartitionId, appIndexID, mp.storedApplyId, time.Since(start).String())
			if time.Since(start) > time.Minute {
				log.LogWarnf("still block after one minute, msg %s", msg)
			} else {
				log.LogInfo(msg)
			}

			if mp.storedApplyId >= appIndexID {
				log.LogWarnf("ApplySnapshot: store snapshot success, msg %s", msg)
				return nil
			}

		case <-mp.stopC:
			log.LogWarnf("ApplySnapshot: revice stop signal, exit now, partition(%d), applyId(%d)", mp.config.PartitionId, mp.applyID)
			return errors.New("server has been shutdown when block")
		}
	}
}

// HandleFatalEvent handles the fatal errors.
func (mp *metaPartition) HandleFatalEvent(err *raft.FatalError) {
	// Panic while fatal event happen.
	exporter.Warning(fmt.Sprintf("action[HandleFatalEvent] err[%v].", err))
	log.LogFatalf("action[HandleFatalEvent] err[%v].", err)
	panic(err.Err)
}

// HandleLeaderChange handles the leader changes.
func (mp *metaPartition) HandleLeaderChange(leader uint64) {
	msg := fmt.Sprintf("metaPartition(%v) changeLeader to (%v)", mp.config.PartitionId, leader)
	exporter.Warning(msg)
	log.LogDebugf(msg)
	if mp.config.NodeId == leader {
		localIp := mp.manager.metaNode.localAddr
		if localIp == "" {
			localIp = "127.0.0.1"
		}

		conn, err := net.DialTimeout("tcp", net.JoinHostPort(localIp, serverPort), time.Second)
		if err != nil {
			msg = fmt.Sprintf("mp[%v] HandleLeaderChange serverPort not exsit ,error %v", mp.config.PartitionId, err)
			log.LogErrorf(msg)
			exporter.Warning(msg)
			go mp.raftPartition.TryToLeader(mp.config.PartitionId)
			return
		}
		msg = fmt.Sprintf("[metaPartition]mp[%v] HandleLeaderChange close conn %v, nodeId: %v, leader: %v",
			mp.config.PartitionId, serverPort, mp.config.NodeId, leader)
		log.LogDebugf(msg)
		exporter.Warning(msg)
		conn.(*net.TCPConn).SetLinger(0)
		conn.Close()
	}
	if mp.config.NodeId != leader {
		msg = fmt.Sprintf("[metaPartition] pid: %v HandleLeaderChange become unleader nodeId: %v, leader: %v",
			mp.config.PartitionId, mp.config.NodeId, leader)
		log.LogDebugf(msg)
		exporter.Warning(msg)
		mp.storeChan <- &storeMsg{
			command: stopStoreTick,
		}
		return
	}
	mp.storeChan <- &storeMsg{
		command: startStoreTick,
	}

	msg = fmt.Sprintf("[metaPartition] pid: %v HandleLeaderChange become leader conn %v, nodeId: %v, leader: %v",
		mp.config.PartitionId, serverPort, mp.config.NodeId, leader)
	log.LogDebugf(msg)
	exporter.Warning(msg)
	if mp.config.Start == 0 && mp.config.Cursor == 0 {
		id, err := mp.nextInodeID()
		if err != nil {
			log.LogFatalf("[HandleLeaderChange] init root inode id: %s.", err.Error())
			exporter.Warning(fmt.Sprintf("[HandleLeaderChange] pid %v init root inode id: %s.", mp.config.PartitionId, err.Error()))
		}
		ino := NewInode(id, proto.Mode(os.ModePerm|os.ModeDir))
		ino.StorageClass = mp.GetVolStorageClass()
		ino.PoolId = mp.vol.GetDefaultPoolId()
		go mp.initInode(ino)
	}
}

// Put puts the given key-value pair (operation key and operation request) into the raft store.
func (mp *metaPartition) submit(op uint32, data []byte) (resp interface{}, err error) {
	log.LogDebugf("submit. op [%v]", op)
	snap := NewMetaItem(0, nil, nil)
	snap.Op = op
	if data != nil {
		snap.V = data
	}
	cmd, err := snap.MarshalJson()
	if err != nil {
		return
	}

	// submit to the raft store
	resp, err = mp.raftPartition.Submit(cmd)
	log.LogDebugf("submit. op [%v] done", op)
	return
}

func (mp *metaPartition) uploadApplyID(applyId uint64) {
	if applyId == 0 {
		return
	}
	atomic.StoreUint64(&mp.applyID, applyId)
}

func (mp *metaPartition) getApplyID() (applyId uint64) {
	return atomic.LoadUint64(&mp.applyID)
}

func (mp *metaPartition) getCommittedID() (committedId uint64) {
	status := mp.raftPartition.Status()
	return status.Commit
}

func (mp *metaPartition) flushAndCheckApplyID(appIndexID uint64) (err error) {
	if mp.inodeTree.GetStoreMode() == proto.StoreModeMem {
		return nil
	}

	var diskApplyID uint64
	for i := 0; i < TryFlushNum; i++ {
		err = mp.inodeTree.Flush(true)
		if err == nil {
			return nil
		}
		if err != ErrDoingFlush {
			log.LogErrorf("[flushAndCheckApplyID] mp(%v) flush err: %s", mp.config.PartitionId, err.Error())
			return err
		}

		// check apply id from disk
		diskApplyID, err = mp.inodeTree.GetApplyIdFromDisk()
		if err != nil {
			log.LogErrorf("[flushAndCheckApplyID] mp(%v) get apply id from disk err: %s", mp.config.PartitionId, err.Error())
			return err
		}

		// flush success
		if diskApplyID >= appIndexID {
			return nil
		}

		time.Sleep(FlushInterval)
	}

	return fmt.Errorf("[flushAndCheckApplyID] mp(%v) timeout, appIndexID: %d, diskApplyID: %d", mp.config.PartitionId, appIndexID, diskApplyID)
}
