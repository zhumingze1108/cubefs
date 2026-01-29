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
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"reflect"
	"strings"
	"sync"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
)

// MetaItem defines the structure of the metadata operations.
type MetaItem struct {
	Op uint32 `json:"Op"`
	K  []byte `json:"k"`
	V  []byte `json:"v"`
}

// MarshalJson
func (s *MetaItem) MarshalJson() ([]byte, error) {
	return json.Marshal(s)
}

// MarshalBinary marshals MetaItem to binary data.
// Binary frame structure:
//
//	+------+----+------+------+------+------+
//	| Item | Op | LenK |   K  | LenV |   V  |
//	+------+----+------+------+------+------+
//	| byte | 4  |  4   | LenK |  4   | LenV |
//	+------+----+------+------+------+------+
func (s *MetaItem) MarshalBinary() (result []byte, err error) {
	buff := bytes.NewBuffer(make([]byte, 0))
	buff.Grow(4 + len(s.K) + len(s.V))
	if err = binary.Write(buff, binary.BigEndian, s.Op); err != nil {
		return
	}
	if err = binary.Write(buff, binary.BigEndian, uint32(len(s.K))); err != nil {
		return
	}
	if _, err = buff.Write(s.K); err != nil {
		return
	}
	if err = binary.Write(buff, binary.BigEndian, uint32(len(s.V))); err != nil {
		return
	}
	if _, err = buff.Write(s.V); err != nil {
		return
	}
	result = buff.Bytes()
	return
}

// UnmarshalJson unmarshals binary data to MetaItem.
func (s *MetaItem) UnmarshalJson(data []byte) error {
	return json.Unmarshal(data, s)
}

// MarshalBinary unmarshal this MetaItem entity from binary data.
// Binary frame structure:
//
//	+------+----+------+------+------+------+
//	| Item | Op | LenK |   K  | LenV |   V  |
//	+------+----+------+------+------+------+
//	| byte | 4  |  4   | LenK |  4   | LenV |
//	+------+----+------+------+------+------+
func (s *MetaItem) UnmarshalBinary(raw []byte) (err error) {
	var (
		lenK uint32
		lenV uint32
	)
	buff := bytes.NewBuffer(raw)
	if err = binary.Read(buff, binary.BigEndian, &s.Op); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &lenK); err != nil {
		return
	}
	s.K = make([]byte, lenK)
	if _, err = buff.Read(s.K); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &lenV); err != nil {
		return
	}
	s.V = make([]byte, lenV)
	if _, err = buff.Read(s.V); err != nil {
		return
	}
	return
}

// NewMetaItem returns a new MetaItem.
func NewMetaItem(op uint32, key, value []byte) *MetaItem {
	return &MetaItem{
		Op: op,
		K:  key,
		V:  value,
	}
}

// RawMetaItem represents raw key-value data from RocksDB without unmarshaling (zero-copy)
type RawMetaItem struct {
	TableType byte   // Table type identifier
	Key       []byte // Raw key bytes
	Value     []byte // Raw value bytes
}
type fileData struct {
	filename string
	data     []byte
}

const (
	// initial version
	SnapFormatVersion_0 uint32 = iota

	// version since transaction feature, added formatVersion, txId and cursor in MetaItemIterator struct
	SnapFormatVersion_1

	snapshotDataChBufferSize = 100
)

// MetaItemIterator defines the iterator of the MetaItem.
type MetaItemIterator struct {
	fileRootDir       string
	SnapFormatVersion uint32
	applyID           uint64
	uniqID            uint64
	txId              uint64
	cursor            uint64
	treeSnap          Snapshot
	uniqChecker       *uniqChecker
	verList           []*proto.VolVersionInfo

	filenames []string

	dataCh    chan interface{}
	errorCh   chan error
	err       error
	closeCh   chan struct{}
	closeOnce sync.Once
}

// SnapItemWrapper key definition
const (
	SiwKeySnapFormatVer uint32 = iota
	SiwKeyApplyId
	SiwKeyTxId
	SiwKeyCursor
	SiwKeyUniqId
	SiwKeyVerList
)

type SnapItemWrapper struct {
	key   uint32
	value interface{}
}

func (siw *SnapItemWrapper) MarshalKey() (k []byte) {
	k = make([]byte, 8)
	binary.BigEndian.PutUint32(k, siw.key)
	return
}

func (siw *SnapItemWrapper) UnmarshalKey(k []byte) (err error) {
	siw.key = binary.BigEndian.Uint32(k)
	return
}

// newMetaItemIterator returns a new MetaItemIterator.
func newMetaItemIterator(mp *metaPartition) (si *MetaItemIterator, err error) {
	si = new(MetaItemIterator)
	si.fileRootDir = mp.config.RootDir
	si.SnapFormatVersion = mp.manager.metaNode.raftSyncSnapFormatVersion
	mp.nonIdempotent.Lock()
	si.applyID = mp.getApplyID()
	si.txId = mp.txProcessor.txManager.txIdAlloc.getTransactionID()
	si.cursor = mp.GetCursor()
	si.uniqID = mp.GetUniqId()
	si.treeSnap, err = mp.GetSnapShot()
	if err != nil {
		return
	}
	si.uniqChecker = mp.uniqChecker.clone()
	si.verList = mp.GetAllVerList()
	mp.nonIdempotent.Unlock()

	if si.treeSnap == nil {
		return nil, errors.NewErrorf("get mp[%v] tree snap failed", mp.config.PartitionId)
	}

	si.dataCh = make(chan interface{}, snapshotDataChBufferSize)
	si.errorCh = make(chan error, 1)
	si.closeCh = make(chan struct{})

	// collect extend del files
	filenames := make([]string, 0)
	var fileInfos []os.DirEntry
	if fileInfos, err = os.ReadDir(mp.config.RootDir); err != nil {
		si.treeSnap.Close()
		return
	}

	for _, fileInfo := range fileInfos {
		if !fileInfo.IsDir() && strings.HasPrefix(fileInfo.Name(), prefixDelExtent) {
			filenames = append(filenames, fileInfo.Name())
		}
		if !fileInfo.IsDir() && strings.HasPrefix(fileInfo.Name(), prefixDelExtentV2) {
			filenames = append(filenames, fileInfo.Name())
		}
	}
	si.filenames = filenames

	// start data producer
	go func(iter *MetaItemIterator) {
		defer func() {
			close(iter.dataCh)
			close(iter.errorCh)
			si.treeSnap.Close()
		}()
		produceItem := func(item interface{}) (success bool) {
			select {
			case iter.dataCh <- item:
				return true
			case <-iter.closeCh:
				return false
			}
		}
		produceError := func(err error) {
			select {
			case iter.errorCh <- err:
			default:
			}
		}
		checkClose := func() (closed bool) {
			select {
			case <-iter.closeCh:
				return true
			default:
				return false
			}
		}

		if si.SnapFormatVersion == SnapFormatVersion_0 {
			// process index ID
			produceItem(si.applyID)
			log.LogDebugf("newMetaItemIterator: SnapFormatVersion_0, partitionId(%v), applyID(%v)",
				mp.config.PartitionId, si.applyID)
		} else if si.SnapFormatVersion == SnapFormatVersion_1 {
			// process snapshot format version
			snapFormatVerWrapper := SnapItemWrapper{SiwKeySnapFormatVer, si.SnapFormatVersion}
			produceItem(snapFormatVerWrapper)

			// process apply index ID
			applyIdWrapper := SnapItemWrapper{SiwKeyApplyId, si.applyID}
			produceItem(applyIdWrapper)

			// process txId
			txIdWrapper := SnapItemWrapper{SiwKeyTxId, si.txId}
			produceItem(txIdWrapper)

			// process cursor
			cursorWrapper := SnapItemWrapper{SiwKeyCursor, si.cursor}
			produceItem(cursorWrapper)

			verListWrapper := SnapItemWrapper{SiwKeyVerList, si.verList}
			produceItem(verListWrapper)

			log.LogDebugf("newMetaItemIterator: SnapFormatVersion_1, partitionId(%v) applyID(%v) txId(%v) cursor(%v) uniqID(%v) verList(%v)",
				mp.config.PartitionId, si.applyID, si.txId, si.cursor, si.uniqID, si.verList)

			if si.uniqID != 0 {
				// process uniqId
				uniqIdWrapper := SnapItemWrapper{SiwKeyUniqId, si.uniqID}
				produceItem(uniqIdWrapper)
			}
		} else {
			panic(fmt.Sprintf("invalid raftSyncSnapFormatVersione: %v", si.SnapFormatVersion))
		}

		// NOTE: if using rocksdb, send base
		// process inodes
		log.LogDebugf("[newMetaItemIterator] start producing inodes, partitionID(%v)", mp.config.PartitionId)
		if mp.inodeTree.GetStoreMode() == proto.StoreModeMem {
			// Memory leader: use Range, send opFSMCreateInode
			if err = iter.treeSnap.Range(InodeType, func(item interface{}) bool {
				return produceItem(item.(*Inode))
			}); err != nil {
				produceError(err)
				return
			}
		} else {
			// RocksDB leader: use RangeRaw, send opFSMRawInodeData
			if err = iter.treeSnap.RangeRaw(InodeType, func(key, value []byte) bool {
				keyCopy := make([]byte, len(key))
				valueCopy := make([]byte, len(value))
				copy(keyCopy, key)
				copy(valueCopy, value)
				rawItem := &RawMetaItem{
					TableType: byte(InodeTable),
					Key:       keyCopy,
					Value:     valueCopy,
				}
				return produceItem(rawItem)
			}); err != nil {
				produceError(err)
				return
			}
		}
		if checkClose() {
			return
		}
		// process dentries
		log.LogDebugf("[newMetaItemIterator] start producing dentries, partitionID(%v)", mp.config.PartitionId)
		if mp.dentryTree.GetStoreMode() == proto.StoreModeMem {
			// Memory leader: use Range, send opFSMCreateDentry
			if err = iter.treeSnap.Range(DentryType, func(item interface{}) bool {
				return produceItem(item.(*Dentry))
			}); err != nil {
				produceError(err)
				return
			}
		} else {
			// RocksDB leader: use RangeRaw, send opFSMRawDentryData
			if err = iter.treeSnap.RangeRaw(DentryType, func(key, value []byte) bool {
				keyCopy := make([]byte, len(key))
				valueCopy := make([]byte, len(value))
				copy(keyCopy, key)
				copy(valueCopy, value)
				rawItem := &RawMetaItem{
					TableType: byte(DentryTable),
					Key:       keyCopy,
					Value:     valueCopy,
				}
				return produceItem(rawItem)
			}); err != nil {
				produceError(err)
				return
			}
		}
		if checkClose() {
			return
		}
		// process extends
		log.LogDebugf("[newMetaItemIterator] start producing extends, partitionID(%v)", mp.config.PartitionId)
		if mp.extendTree.GetStoreMode() == proto.StoreModeMem {
			// Memory leader: use Range, send opFSMSetXAttr
			if err = iter.treeSnap.Range(ExtendType, func(item interface{}) bool {
				return produceItem(item.(*Extend))
			}); err != nil {
				produceError(err)
				return
			}
		} else {
			// RocksDB leader: use RangeRaw, send opFSMRawExtendData
			if err = iter.treeSnap.RangeRaw(ExtendType, func(key, value []byte) bool {
				keyCopy := make([]byte, len(key))
				valueCopy := make([]byte, len(value))
				copy(keyCopy, key)
				copy(valueCopy, value)
				rawItem := &RawMetaItem{
					TableType: byte(ExtendTable),
					Key:       keyCopy,
					Value:     valueCopy,
				}
				return produceItem(rawItem)
			}); err != nil {
				produceError(err)
				return
			}
		}
		if checkClose() {
			return
		}

		log.LogDebugf("ApplySnapshot: start producing multiparts, partitionID(%v)", mp.config.PartitionId)
		if mp.multipartTree.GetStoreMode() == proto.StoreModeMem {
			// Memory leader: use Range, send opFSMCreateMultipart
			if err = iter.treeSnap.Range(MultipartType, func(item interface{}) bool {
				return produceItem(item.(*Multipart))
			}); err != nil {
				produceError(err)
				return
			}
		} else {
			// RocksDB leader: use RangeRaw, send opFSMRawMultipartData
			if err = iter.treeSnap.RangeRaw(MultipartType, func(key, value []byte) bool {
				keyCopy := make([]byte, len(key))
				valueCopy := make([]byte, len(value))
				copy(keyCopy, key)
				copy(valueCopy, value)
				rawItem := &RawMetaItem{
					TableType: byte(MultipartTable),
					Key:       keyCopy,
					Value:     valueCopy,
				}
				return produceItem(rawItem)
			}); err != nil {
				produceError(err)
				return
			}
		}
		if checkClose() {
			return
		}

		if si.SnapFormatVersion == SnapFormatVersion_1 {
			// Process transactions (single threaded, usually small)
			log.LogDebugf("ApplySnapshot: start producing transactions, partitionID(%v)", mp.config.PartitionId)
			if mp.txProcessor.txManager.txTree.GetStoreMode() == proto.StoreModeMem {
				// Memory leader: use Range, send opFSMTxSnapshot
				if err = iter.treeSnap.Range(TransactionType, func(item interface{}) bool {
					return produceItem(item.(*proto.TransactionInfo))
				}); err != nil {
					produceError(err)
					return
				}
			} else {
				// RocksDB leader: use RangeRaw, send opFSMRawTxData
				if err = iter.treeSnap.RangeRaw(TransactionType, func(key, value []byte) bool {
					keyCopy := make([]byte, len(key))
					valueCopy := make([]byte, len(value))
					copy(keyCopy, key)
					copy(valueCopy, value)
					rawItem := &RawMetaItem{
						TableType: byte(TransactionTable),
						Key:       keyCopy,
						Value:     valueCopy,
					}
					return produceItem(rawItem)
				}); err != nil {
					produceError(err)
					return
				}
			}
			if checkClose() {
				return
			}

			// Process transaction rollback inodes (single threaded, usually small)
			log.LogDebugf("ApplySnapshot: start producing tx rb inodes, partitionID(%v)", mp.config.PartitionId)
			if mp.txProcessor.txResource.txRbInodeTree.GetStoreMode() == proto.StoreModeMem {
				// Memory leader: use Range, send opFSMTxRbInodeSnapshot
				if err = iter.treeSnap.Range(TransactionRollbackInodeType, func(item interface{}) bool {
					return produceItem(item.(*TxRollbackInode))
				}); err != nil {
					produceError(err)
					return
				}
			} else {
				// RocksDB leader: use RangeRaw, send opFSMRawTxRbInodeData
				if err = iter.treeSnap.RangeRaw(TransactionRollbackInodeType, func(key, value []byte) bool {
					keyCopy := make([]byte, len(key))
					valueCopy := make([]byte, len(value))
					copy(keyCopy, key)
					copy(valueCopy, value)
					rawItem := &RawMetaItem{
						TableType: byte(TransactionRollbackInodeTable),
						Key:       keyCopy,
						Value:     valueCopy,
					}
					return produceItem(rawItem)
				}); err != nil {
					produceError(err)
					return
				}
			}
			if checkClose() {
				return
			}

			// Process transaction rollback dentries (single threaded, usually small)
			log.LogDebugf("ApplySnapshot: start producing tx rb dentries, partitionID(%v)", mp.config.PartitionId)
			if mp.txProcessor.txResource.txRbDentryTree.GetStoreMode() == proto.StoreModeMem {
				// Memory leader: use Range, send opFSMTxRbDentrySnapshot
				if err = iter.treeSnap.Range(TransactionRollbackDentryType, func(item interface{}) bool {
					return produceItem(item.(*TxRollbackDentry))
				}); err != nil {
					produceError(err)
					return
				}
			} else {
				// RocksDB leader: use RangeRaw, send opFSMRawTxRbDentryData
				if err = iter.treeSnap.RangeRaw(TransactionRollbackDentryType, func(key, value []byte) bool {
					keyCopy := make([]byte, len(key))
					valueCopy := make([]byte, len(value))
					copy(keyCopy, key)
					copy(valueCopy, value)
					rawItem := &RawMetaItem{
						TableType: byte(TransactionRollbackDentryTable),
						Key:       keyCopy,
						Value:     valueCopy,
					}
					return produceItem(rawItem)
				}); err != nil {
					produceError(err)
					return
				}
			}
			if checkClose() {
				return
			}

			if si.uniqID != 0 {
				produceItem(si.uniqChecker)
				if checkClose() {
					return
				}
			}
		}

		// process extent del files
		var err error
		var raw []byte
		for _, filename := range iter.filenames {
			if raw, err = os.ReadFile(path.Join(iter.fileRootDir, filename)); err != nil {
				produceError(err)
				return
			}
			if !produceItem(&fileData{filename: filename, data: raw}) {
				return
			}
		}
	}(si)

	return
}

// ApplyIndex returns the applyID of the iterator.
func (si *MetaItemIterator) ApplyIndex() uint64 {
	return si.applyID
}

// Close closes the iterator.
func (si *MetaItemIterator) Close() {
	si.closeOnce.Do(func() {
		close(si.closeCh)
	})
}

// Next returns the next item.
func (si *MetaItemIterator) Next() (data []byte, err error) {
	if si.err != nil {
		err = si.err
		return
	}
	var item interface{}
	for {
		var open bool
		select {
		case item, open = <-si.dataCh:
			if item == nil || !open {
				err, si.err = io.EOF, io.EOF
				si.Close()
				return
			}
		case err, open = <-si.errorCh:
			if !open {
				// Disable error channel after it's closed so remaining dataCh items can drain.
				si.errorCh = nil
				continue
			}
			if err != nil {
				si.err = err
				si.Close()
				return
			}
			continue
		}
		break
	}

	var snap *MetaItem
	switch typedItem := item.(type) {
	case uint64:
		applyIDBuf := make([]byte, 8)
		binary.BigEndian.PutUint64(applyIDBuf, si.applyID)
		data = applyIDBuf
		return
	case *RawMetaItem:
		// Zero-copy path: directly use raw data with appropriate op code
		var opCode uint32
		switch typedItem.TableType {
		case byte(InodeTable):
			opCode = opFSMRawInodeData
		case byte(DentryTable):
			opCode = opFSMRawDentryData
		case byte(ExtendTable):
			opCode = opFSMRawExtendData
		case byte(MultipartTable):
			opCode = opFSMRawMultipartData
		case byte(TransactionTable):
			opCode = opFSMRawTxData
		case byte(TransactionRollbackInodeTable):
			opCode = opFSMRawTxRbInodeData
		case byte(TransactionRollbackDentryTable):
			opCode = opFSMRawTxRbDentryData
		default:
			err = fmt.Errorf("unknown table type: %d", typedItem.TableType)
			si.err = err
			si.Close()
			return
		}
		snap = NewMetaItem(opCode, typedItem.Key, typedItem.Value)
	case SnapItemWrapper:
		if typedItem.key == SiwKeySnapFormatVer {
			snapFormatVerBuf := make([]byte, 8)
			binary.BigEndian.PutUint32(snapFormatVerBuf, si.SnapFormatVersion)
			snap = NewMetaItem(opFSMSnapFormatVersion, typedItem.MarshalKey(), snapFormatVerBuf)
		} else if typedItem.key == SiwKeyApplyId {
			applyIDBuf := make([]byte, 8)
			binary.BigEndian.PutUint64(applyIDBuf, si.applyID)
			snap = NewMetaItem(opFSMApplyId, typedItem.MarshalKey(), applyIDBuf)
		} else if typedItem.key == SiwKeyTxId {
			txIDBuf := make([]byte, 8)
			binary.BigEndian.PutUint64(txIDBuf, si.txId)
			snap = NewMetaItem(opFSMTxId, typedItem.MarshalKey(), txIDBuf)
		} else if typedItem.key == SiwKeyCursor {
			cursor := typedItem.value.(uint64)
			cursorBuf := make([]byte, 8)
			binary.BigEndian.PutUint64(cursorBuf, cursor)
			snap = NewMetaItem(opFSMCursor, typedItem.MarshalKey(), cursorBuf)
		} else if typedItem.key == SiwKeyUniqId {
			uniqId := typedItem.value.(uint64)
			uniqIdBuf := make([]byte, 8)
			binary.BigEndian.PutUint64(uniqIdBuf, uniqId)
			snap = NewMetaItem(opFSMUniqIDSnap, typedItem.MarshalKey(), uniqIdBuf)
		} else if typedItem.key == SiwKeyVerList {
			var verListBuf []byte
			if verListBuf, err = json.Marshal(typedItem.value.([]*proto.VolVersionInfo)); err != nil {
				return
			}
			snap = NewMetaItem(opFSMVerListSnapShot, typedItem.MarshalKey(), verListBuf)
			log.LogInfof("snapshot.fileRootDir %v verList %v", si.fileRootDir, verListBuf)
		} else {
			panic(fmt.Sprintf("MetaItemIterator.Next: unknown SnapItemWrapper key: %v", typedItem.key))
		}
	case *Inode:
		snap = NewMetaItem(opFSMCreateInode, typedItem.MarshalKey(), typedItem.MarshalValue())
	case *Dentry:
		snap = NewMetaItem(opFSMCreateDentry, typedItem.MarshalKey(), typedItem.MarshalValue())
	case *Extend:
		var raw []byte
		if raw, err = typedItem.Bytes(); err != nil {
			si.err = err
			si.Close()
			return
		}
		snap = NewMetaItem(opFSMSetXAttr, nil, raw)
	case *Multipart:
		var raw []byte
		if raw, err = typedItem.Bytes(); err != nil {
			si.err = err
			si.Close()
			return
		}
		snap = NewMetaItem(opFSMCreateMultipart, nil, raw)
	case *proto.TransactionInfo:
		var val []byte
		val, err = typedItem.Marshal()
		if err != nil {
			si.err = err
			si.Close()
			return
		}
		snap = NewMetaItem(opFSMTxSnapshot, []byte(typedItem.TxID), val)
	case *TxRollbackInode:
		var val []byte
		val, err = typedItem.Marshal()
		if err != nil {
			si.err = err
			si.Close()
			return
		}
		snap = NewMetaItem(opFSMTxRbInodeSnapshot, typedItem.inode.MarshalKey(), val)
	case *TxRollbackDentry:
		var val []byte
		val, err = typedItem.Marshal()
		if err != nil {
			si.err = err
			si.Close()
			return
		}
		snap = NewMetaItem(opFSMTxRbDentrySnapshot, []byte(typedItem.txDentryInfo.GetKey()), val)
	case *fileData:
		snap = NewMetaItem(opExtentFileSnapshot, []byte(typedItem.filename), typedItem.data)
	case *uniqChecker:
		var raw []byte
		/*
			In order to support software update from v1 to v2, we need to use the checkerVersionV1 here.
			For snapshot, it is not necessary to use the checkerVersionV2.
			All the raft fsm apply id will larger than current value. So it is safe to use the checkerVersionV1 here.
		*/
		if raw, _, err = typedItem.Marshal(checkerVersionV1); err != nil {
			si.err = err
			si.Close()
			return
		}
		snap = NewMetaItem(opFSMUniqCheckerSnap, nil, raw)
	default:
		panic(fmt.Sprintf("unknown item type: %v", reflect.TypeOf(item).Name()))
	}

	if data, err = snap.MarshalBinary(); err != nil {
		si.err = err
		si.Close()
		return
	}
	return
}
