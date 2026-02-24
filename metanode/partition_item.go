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
	"strings"
	"sync"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/buf"
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

// marshalMetaItem marshals MetaItem to binary data.
// Binary frame structure:
//
//	+------+----+------+------+------+------+
//	| Item | Op | LenK |   K  | LenV |   V  |
//	+------+----+------+------+------+------+
//	| byte | 4  |  4   | LenK |  4   | LenV |
//	+------+----+------+------+------+------+
func marshalMetaItemToBuf(b *buf.ByteBufExt, op uint32, key, value []byte) {
	b.Reset()
	if err := b.PutUint32(op); err != nil {
		panic(err)
	}
	if err := b.PutUint32(uint32(len(key))); err != nil {
		panic(err)
	}
	if _, err := b.Write(key); err != nil {
		panic(err)
	}
	if err := b.PutUint32(uint32(len(value))); err != nil {
		panic(err)
	}
	if _, err := b.Write(value); err != nil {
		panic(err)
	}
}

// NewMetaItem returns a new MetaItem.
func NewMetaItem(op uint32, key, value []byte) *MetaItem {
	return &MetaItem{
		Op: op,
		K:  key,
		V:  value,
	}
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

	dataCh    chan *buf.ByteBufExt
	errorCh   chan error
	err       error
	closeCh   chan struct{}
	closeOnce sync.Once
	lastBuf   *buf.ByteBufExt
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

func snapItemKey(key uint32) []byte {
	k := make([]byte, 8)
	binary.BigEndian.PutUint32(k, key)
	return k
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

	si.dataCh = make(chan *buf.ByteBufExt, snapshotDataChBufferSize)
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
		produceItem := func(b *buf.ByteBufExt) (success bool) {
			select {
			case iter.dataCh <- b:
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
			b := GetMetaItemBuf()
			if err := b.PutUint64(si.applyID); err != nil {
				panic(err)
			}
			if !produceItem(b) {
				PutMetaItemBuf(b)
				return
			}
			log.LogDebugf("newMetaItemIterator: SnapFormatVersion_0, partitionId(%v), applyID(%v)",
				mp.config.PartitionId, si.applyID)
		} else if si.SnapFormatVersion == SnapFormatVersion_1 {
			// process snapshot format version
			snapFormatVerKey := snapItemKey(SiwKeySnapFormatVer)
			snapFormatVerBuf := make([]byte, 8)
			binary.BigEndian.PutUint32(snapFormatVerBuf, si.SnapFormatVersion)
			b := GetMetaItemBuf()
			marshalMetaItemToBuf(b, opFSMSnapFormatVersion, snapFormatVerKey, snapFormatVerBuf)
			if !produceItem(b) {
				PutMetaItemBuf(b)
				return
			}

			// process apply index ID
			applyIdKey := snapItemKey(SiwKeyApplyId)
			applyIDBuf := make([]byte, 8)
			binary.BigEndian.PutUint64(applyIDBuf, si.applyID)
			b = GetMetaItemBuf()
			marshalMetaItemToBuf(b, opFSMApplyId, applyIdKey, applyIDBuf)
			if !produceItem(b) {
				PutMetaItemBuf(b)
				return
			}

			// process txId
			txIdKey := snapItemKey(SiwKeyTxId)
			txIDBuf := make([]byte, 8)
			binary.BigEndian.PutUint64(txIDBuf, si.txId)
			b = GetMetaItemBuf()
			marshalMetaItemToBuf(b, opFSMTxId, txIdKey, txIDBuf)
			if !produceItem(b) {
				PutMetaItemBuf(b)
				return
			}

			// process cursor
			cursorKey := snapItemKey(SiwKeyCursor)
			cursorBuf := make([]byte, 8)
			binary.BigEndian.PutUint64(cursorBuf, si.cursor)
			b = GetMetaItemBuf()
			marshalMetaItemToBuf(b, opFSMCursor, cursorKey, cursorBuf)
			if !produceItem(b) {
				PutMetaItemBuf(b)
				return
			}

			verListKey := snapItemKey(SiwKeyVerList)
			verListBuf, err := json.Marshal(si.verList)
			if err != nil {
				produceError(err)
				return
			}
			b = GetMetaItemBuf()
			marshalMetaItemToBuf(b, opFSMVerListSnapShot, verListKey, verListBuf)
			if !produceItem(b) {
				PutMetaItemBuf(b)
				return
			}

			log.LogDebugf("newMetaItemIterator: SnapFormatVersion_1, partitionId(%v) applyID(%v) txId(%v) cursor(%v) uniqID(%v) verList(%v)",
				mp.config.PartitionId, si.applyID, si.txId, si.cursor, si.uniqID, si.verList)

			if si.uniqID != 0 {
				// process uniqId
				uniqIdKey := snapItemKey(SiwKeyUniqId)
				uniqIdBuf := make([]byte, 8)
				binary.BigEndian.PutUint64(uniqIdBuf, si.uniqID)
				b = GetMetaItemBuf()
				marshalMetaItemToBuf(b, opFSMUniqIDSnap, uniqIdKey, uniqIdBuf)
				if !produceItem(b) {
					PutMetaItemBuf(b)
					return
				}
			}
		} else {
			panic(fmt.Sprintf("invalid raftSyncSnapFormatVersione: %v", si.SnapFormatVersion))
		}

		var wg sync.WaitGroup
		var errOnce sync.Once
		signalErr := func(err error) {
			errOnce.Do(func() {
				produceError(err)
				iter.Close()
			})
		}
		startProducer := func(fn func() error) {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if checkClose() {
					return
				}
				if err := fn(); err != nil {
					signalErr(err)
				}
			}()
		}

		// NOTE: if using rocksdb, send base
		// process inodes
		startProducer(func() error {
			log.LogDebugf("[newMetaItemIterator] start producing inodes, partitionID(%v)", mp.config.PartitionId)
			if mp.inodeTree.GetStoreMode() == proto.StoreModeMem {
				return iter.treeSnap.Range(InodeType, func(item interface{}) bool {
					if checkClose() {
						return false
					}
					inode := item.(*Inode)
					keyBuf := GetInodeBuf()
					inode.MarshalKeyV2(keyBuf)
					key := keyBuf.Bytes()
					valBuf := GetInodeBuf()
					inode.MarshalValueV2(valBuf)
					value := valBuf.Bytes()
					metaBuf := GetMetaItemBuf()
					marshalMetaItemToBuf(metaBuf, opFSMCreateInode, key, value)
					success := produceItem(metaBuf)
					if !success {
						PutMetaItemBuf(metaBuf)
					}
					PutInodeBuf(keyBuf)
					PutInodeBuf(valBuf)
					return success
				})
			}
			return iter.treeSnap.RangeRaw(InodeType, func(key, value []byte) bool {
				if checkClose() {
					return false
				}
				metaBuf := GetMetaItemBuf()
				marshalMetaItemToBuf(metaBuf, opFSMRawInodeData, key, value)
				success := produceItem(metaBuf)
				if !success {
					PutMetaItemBuf(metaBuf)
				}
				return success
			})
		})

		// process dentries
		startProducer(func() error {
			log.LogDebugf("[newMetaItemIterator] start producing dentries, partitionID(%v)", mp.config.PartitionId)
			if mp.dentryTree.GetStoreMode() == proto.StoreModeMem {
				return iter.treeSnap.Range(DentryType, func(item interface{}) bool {
					if checkClose() {
						return false
					}
					dentry := item.(*Dentry)
					keyBuf := GetDentryBuf()
					dentry.MarshalKeyV2(keyBuf)
					key := keyBuf.Bytes()
					valBuf := GetDentryBuf()
					dentry.MarshalValueV2(valBuf)
					value := valBuf.Bytes()
					metaBuf := GetMetaItemBuf()
					marshalMetaItemToBuf(metaBuf, opFSMCreateDentry, key, value)
					success := produceItem(metaBuf)
					if !success {
						PutMetaItemBuf(metaBuf)
					}
					PutDentryBuf(keyBuf)
					PutDentryBuf(valBuf)
					return success
				})
			}
			return iter.treeSnap.RangeRaw(DentryType, func(key, value []byte) bool {
				if checkClose() {
					return false
				}
				metaBuf := GetMetaItemBuf()
				marshalMetaItemToBuf(metaBuf, opFSMRawDentryData, key, value)
				success := produceItem(metaBuf)
				if !success {
					PutMetaItemBuf(metaBuf)
				}
				return success
			})
		})

		// process extends
		startProducer(func() error {
			log.LogDebugf("[newMetaItemIterator] start producing extends, partitionID(%v)", mp.config.PartitionId)
			if mp.extendTree.GetStoreMode() == proto.StoreModeMem {
				var callbackErr error
				rangeErr := iter.treeSnap.Range(ExtendType, func(item interface{}) bool {
					if checkClose() {
						return false
					}
					extend := item.(*Extend)
					raw, err := extend.Bytes()
					if err != nil {
						callbackErr = err
						return false
					}
					metaBuf := GetMetaItemBuf()
					marshalMetaItemToBuf(metaBuf, opFSMSetXAttr, nil, raw)
					success := produceItem(metaBuf)
					if !success {
						PutMetaItemBuf(metaBuf)
					}
					return success
				})
				if callbackErr != nil {
					return callbackErr
				}
				return rangeErr
			}
			return iter.treeSnap.RangeRaw(ExtendType, func(key, value []byte) bool {
				if checkClose() {
					return false
				}
				metaBuf := GetMetaItemBuf()
				marshalMetaItemToBuf(metaBuf, opFSMRawExtendData, key, value)
				success := produceItem(metaBuf)
				if !success {
					PutMetaItemBuf(metaBuf)
				}
				return success
			})
		})

		// process multiparts
		startProducer(func() error {
			log.LogDebugf("ApplySnapshot: start producing multiparts, partitionID(%v)", mp.config.PartitionId)
			if mp.multipartTree.GetStoreMode() == proto.StoreModeMem {
				var callbackErr error
				rangeErr := iter.treeSnap.Range(MultipartType, func(item interface{}) bool {
					if checkClose() {
						return false
					}
					multipart := item.(*Multipart)
					raw, err := multipart.Bytes()
					if err != nil {
						callbackErr = err
						return false
					}
					metaBuf := GetMetaItemBuf()
					marshalMetaItemToBuf(metaBuf, opFSMCreateMultipart, nil, raw)
					success := produceItem(metaBuf)
					if !success {
						PutMetaItemBuf(metaBuf)
					}
					return success
				})
				if callbackErr != nil {
					return callbackErr
				}
				return rangeErr
			}
			return iter.treeSnap.RangeRaw(MultipartType, func(key, value []byte) bool {
				if checkClose() {
					return false
				}
				metaBuf := GetMetaItemBuf()
				marshalMetaItemToBuf(metaBuf, opFSMRawMultipartData, key, value)
				success := produceItem(metaBuf)
				if !success {
					PutMetaItemBuf(metaBuf)
				}
				return success
			})
		})

		if si.SnapFormatVersion == SnapFormatVersion_1 {
			// Process transactions
			startProducer(func() error {
				log.LogDebugf("ApplySnapshot: start producing transactions, partitionID(%v)", mp.config.PartitionId)
				if mp.txProcessor.txManager.txTree.GetStoreMode() == proto.StoreModeMem {
					var callbackErr error
					rangeErr := iter.treeSnap.Range(TransactionType, func(item interface{}) bool {
						if checkClose() {
							return false
						}
						txInfo := item.(*proto.TransactionInfo)
						val, err := txInfo.Marshal()
						if err != nil {
							callbackErr = err
							return false
						}
						metaBuf := GetMetaItemBuf()
						marshalMetaItemToBuf(metaBuf, opFSMTxSnapshot, []byte(txInfo.TxID), val)
						success := produceItem(metaBuf)
						if !success {
							PutMetaItemBuf(metaBuf)
						}
						return success
					})
					if callbackErr != nil {
						return callbackErr
					}
					return rangeErr
				}
				return iter.treeSnap.RangeRaw(TransactionType, func(key, value []byte) bool {
					if checkClose() {
						return false
					}
					metaBuf := GetMetaItemBuf()
					marshalMetaItemToBuf(metaBuf, opFSMRawTxData, key, value)
					success := produceItem(metaBuf)
					if !success {
						PutMetaItemBuf(metaBuf)
					}
					return success
				})
			})

			// Process transaction rollback inodes
			startProducer(func() error {
				log.LogDebugf("ApplySnapshot: start producing tx rb inodes, partitionID(%v)", mp.config.PartitionId)
				if mp.txProcessor.txResource.txRbInodeTree.GetStoreMode() == proto.StoreModeMem {
					var callbackErr error
					rangeErr := iter.treeSnap.Range(TransactionRollbackInodeType, func(item interface{}) bool {
						if checkClose() {
							return false
						}
						txRbInode := item.(*TxRollbackInode)
						val, err := txRbInode.Marshal()
						if err != nil {
							callbackErr = err
							return false
						}
						keyBuf := GetInodeBuf()
						txRbInode.inode.MarshalKeyV2(keyBuf)
						key := keyBuf.Bytes()
						PutInodeBuf(keyBuf)
						metaBuf := GetMetaItemBuf()
						marshalMetaItemToBuf(metaBuf, opFSMTxRbInodeSnapshot, key, val)
						success := produceItem(metaBuf)
						if !success {
							PutMetaItemBuf(metaBuf)
						}
						return success
					})
					if callbackErr != nil {
						return callbackErr
					}
					return rangeErr
				}
				return iter.treeSnap.RangeRaw(TransactionRollbackInodeType, func(key, value []byte) bool {
					if checkClose() {
						return false
					}
					metaBuf := GetMetaItemBuf()
					marshalMetaItemToBuf(metaBuf, opFSMRawTxRbInodeData, key, value)
					success := produceItem(metaBuf)
					if !success {
						PutMetaItemBuf(metaBuf)
					}
					return success
				})
			})

			// Process transaction rollback dentries
			startProducer(func() error {
				log.LogDebugf("ApplySnapshot: start producing tx rb dentries, partitionID(%v)", mp.config.PartitionId)
				if mp.txProcessor.txResource.txRbDentryTree.GetStoreMode() == proto.StoreModeMem {
					var callbackErr error
					rangeErr := iter.treeSnap.Range(TransactionRollbackDentryType, func(item interface{}) bool {
						if checkClose() {
							return false
						}
						txRbDentry := item.(*TxRollbackDentry)
						val, err := txRbDentry.Marshal()
						if err != nil {
							callbackErr = err
							return false
						}
						metaBuf := GetMetaItemBuf()
						marshalMetaItemToBuf(metaBuf, opFSMTxRbDentrySnapshot, []byte(txRbDentry.txDentryInfo.GetKey()), val)
						success := produceItem(metaBuf)
						if !success {
							PutMetaItemBuf(metaBuf)
						}
						return success
					})
					if callbackErr != nil {
						return callbackErr
					}
					return rangeErr
				}
				return iter.treeSnap.RangeRaw(TransactionRollbackDentryType, func(key, value []byte) bool {
					if checkClose() {
						return false
					}
					metaBuf := GetMetaItemBuf()
					marshalMetaItemToBuf(metaBuf, opFSMRawTxRbDentryData, key, value)
					success := produceItem(metaBuf)
					if !success {
						PutMetaItemBuf(metaBuf)
					}
					return success
				})
			})
		}

		wg.Wait()
		if checkClose() {
			return
		}

		if si.SnapFormatVersion == SnapFormatVersion_1 && si.uniqID != 0 {
			raw, _, err := si.uniqChecker.Marshal(checkerVersionV1)
			if err != nil {
				produceError(err)
				return
			}
			metaBuf := GetMetaItemBuf()
			marshalMetaItemToBuf(metaBuf, opFSMUniqCheckerSnap, nil, raw)
			if !produceItem(metaBuf) {
				PutMetaItemBuf(metaBuf)
				return
			}
			if checkClose() {
				return
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
			metaBuf := GetMetaItemBuf()
			marshalMetaItemToBuf(metaBuf, opExtentFileSnapshot, []byte(filename), raw)
			if !produceItem(metaBuf) {
				PutMetaItemBuf(metaBuf)
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

	// Best-effort: reclaim pooled buffers even if snapshot send terminates early.
	if si.lastBuf != nil {
		PutMetaItemBuf(si.lastBuf)
		si.lastBuf = nil
	}
	for {
		select {
		case b, ok := <-si.dataCh:
			if !ok {
				return
			}
			if b != nil {
				PutMetaItemBuf(b)
			}
		default:
			return
		}
	}
}

// Next returns the next item.
func (si *MetaItemIterator) Next() (data []byte, err error) {
	if si.err != nil {
		err = si.err
		return
	}
	// Return the previous buffer to the pool. It's safe because the caller will
	// not access the previous []byte after calling Next() again.
	if si.lastBuf != nil {
		PutMetaItemBuf(si.lastBuf)
		si.lastBuf = nil
	}
	for {
		var open bool
		select {
		case b, open := <-si.dataCh:
			if b == nil || !open {
				err, si.err = io.EOF, io.EOF
				si.Close()
				return
			}
			si.lastBuf = b
			data = b.Bytes()
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
	return
}
