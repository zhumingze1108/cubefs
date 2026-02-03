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
	keyLen := len(s.K)
	valLen := len(s.V)
	result = make([]byte, 4+4+keyLen+4+valLen)

	offset := 0
	binary.BigEndian.PutUint32(result[offset:], s.Op)
	offset += 4

	binary.BigEndian.PutUint32(result[offset:], uint32(keyLen))
	offset += 4
	copy(result[offset:], s.K)
	offset += keyLen

	binary.BigEndian.PutUint32(result[offset:], uint32(valLen))
	offset += 4
	copy(result[offset:], s.V)
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

func marshalMetaItem(op uint32, key, value []byte) []byte {
	keyLen := len(key)
	valLen := len(value)
	result := make([]byte, 4+4+keyLen+4+valLen)
	offset := 0
	binary.BigEndian.PutUint32(result[offset:], op)
	offset += 4
	binary.BigEndian.PutUint32(result[offset:], uint32(keyLen))
	offset += 4
	copy(result[offset:], key)
	offset += keyLen
	binary.BigEndian.PutUint32(result[offset:], uint32(valLen))
	offset += 4
	copy(result[offset:], value)
	return result
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

	dataCh    chan []byte
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

	si.dataCh = make(chan []byte, snapshotDataChBufferSize)
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
		produceItem := func(data []byte) (success bool) {
			select {
			case iter.dataCh <- data:
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
			applyIDBuf := make([]byte, 8)
			binary.BigEndian.PutUint64(applyIDBuf, si.applyID)
			produceItem(applyIDBuf)
			log.LogDebugf("newMetaItemIterator: SnapFormatVersion_0, partitionId(%v), applyID(%v)",
				mp.config.PartitionId, si.applyID)
		} else if si.SnapFormatVersion == SnapFormatVersion_1 {
			// process snapshot format version
			snapFormatVerKey := (&SnapItemWrapper{key: SiwKeySnapFormatVer}).MarshalKey()
			snapFormatVerBuf := make([]byte, 8)
			binary.BigEndian.PutUint32(snapFormatVerBuf, si.SnapFormatVersion)
			produceItem(marshalMetaItem(opFSMSnapFormatVersion, snapFormatVerKey, snapFormatVerBuf))

			// process apply index ID
			applyIdKey := (&SnapItemWrapper{key: SiwKeyApplyId}).MarshalKey()
			applyIDBuf := make([]byte, 8)
			binary.BigEndian.PutUint64(applyIDBuf, si.applyID)
			produceItem(marshalMetaItem(opFSMApplyId, applyIdKey, applyIDBuf))

			// process txId
			txIdKey := (&SnapItemWrapper{key: SiwKeyTxId}).MarshalKey()
			txIDBuf := make([]byte, 8)
			binary.BigEndian.PutUint64(txIDBuf, si.txId)
			produceItem(marshalMetaItem(opFSMTxId, txIdKey, txIDBuf))

			// process cursor
			cursorKey := (&SnapItemWrapper{key: SiwKeyCursor}).MarshalKey()
			cursorBuf := make([]byte, 8)
			binary.BigEndian.PutUint64(cursorBuf, si.cursor)
			produceItem(marshalMetaItem(opFSMCursor, cursorKey, cursorBuf))

			verListKey := (&SnapItemWrapper{key: SiwKeyVerList}).MarshalKey()
			verListBuf, err := json.Marshal(si.verList)
			if err != nil {
				produceError(err)
				return
			}
			produceItem(marshalMetaItem(opFSMVerListSnapShot, verListKey, verListBuf))

			log.LogDebugf("newMetaItemIterator: SnapFormatVersion_1, partitionId(%v) applyID(%v) txId(%v) cursor(%v) uniqID(%v) verList(%v)",
				mp.config.PartitionId, si.applyID, si.txId, si.cursor, si.uniqID, si.verList)

			if si.uniqID != 0 {
				// process uniqId
				uniqIdKey := (&SnapItemWrapper{key: SiwKeyUniqId}).MarshalKey()
				uniqIdBuf := make([]byte, 8)
				binary.BigEndian.PutUint64(uniqIdBuf, si.uniqID)
				produceItem(marshalMetaItem(opFSMUniqIDSnap, uniqIdKey, uniqIdBuf))
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
					return produceItem(marshalMetaItem(opFSMCreateInode, inode.MarshalKey(), inode.MarshalValue()))
				})
			}
			return iter.treeSnap.RangeRaw(InodeType, func(key, value []byte) bool {
				if checkClose() {
					return false
				}
				return produceItem(marshalMetaItem(opFSMRawInodeData, key, value))
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
					return produceItem(marshalMetaItem(opFSMCreateDentry, dentry.MarshalKey(), dentry.MarshalValue()))
				})
			}
			return iter.treeSnap.RangeRaw(DentryType, func(key, value []byte) bool {
				if checkClose() {
					return false
				}
				return produceItem(marshalMetaItem(opFSMRawDentryData, key, value))
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
					return produceItem(marshalMetaItem(opFSMSetXAttr, nil, raw))
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
				return produceItem(marshalMetaItem(opFSMRawExtendData, key, value))
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
					return produceItem(marshalMetaItem(opFSMCreateMultipart, nil, raw))
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
				return produceItem(marshalMetaItem(opFSMRawMultipartData, key, value))
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
						return produceItem(marshalMetaItem(opFSMTxSnapshot, []byte(txInfo.TxID), val))
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
					return produceItem(marshalMetaItem(opFSMRawTxData, key, value))
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
						return produceItem(marshalMetaItem(opFSMTxRbInodeSnapshot, txRbInode.inode.MarshalKey(), val))
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
					return produceItem(marshalMetaItem(opFSMRawTxRbInodeData, key, value))
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
						return produceItem(marshalMetaItem(opFSMTxRbDentrySnapshot, []byte(txRbDentry.txDentryInfo.GetKey()), val))
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
					return produceItem(marshalMetaItem(opFSMRawTxRbDentryData, key, value))
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
			produceItem(marshalMetaItem(opFSMUniqCheckerSnap, nil, raw))
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
			if !produceItem(marshalMetaItem(opExtentFileSnapshot, []byte(filename), raw)) {
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
	for {
		var open bool
		select {
		case data, open = <-si.dataCh:
			if data == nil || !open {
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
	return
}
