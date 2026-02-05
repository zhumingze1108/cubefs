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
	"fmt"
	"sync"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/btree"
)

const defaultBTreeDegree = 32

type (
	// BtreeItem type alias google btree Item
	BtreeItem = btree.Item
)

var (
	// batchInsertChannels stores insert channels for each BTree instance (for pipeline insert)
	// Key: *BTree pointer, Value: chan BtreeItem
	// Only used during ApplySnapshot, managed directly in ApplySnapshot function
	batchInsertChannels sync.Map
	// batchInsertWaitGroups stores wait groups for each BTree instance to wait for insert completion
	batchInsertWaitGroups sync.Map
)

var _ Snapshot = &MemSnapShot{}

type MemSnapShot struct {
	applyID             uint64
	inode               *InodeBTree
	dentry              *DentryBTree
	extend              *ExtendBTree
	multipart           *MultipartBTree
	transaction         *TransactionBTree
	transactionRbInode  *TransactionRollbackInodeBTree
	transactionRbDentry *TransactionRollbackDentryBTree
	deletedExtents      *DeletedExtentsBTree
	deletedObjExtents   *DeletedObjExtentsBTree
	txID                uint64
}

func (b *MemSnapShot) Range(tp TreeType, cb func(item interface{}) bool) error {
	switch tp {
	case InodeType:
		callBackFunc := func(inode *Inode) bool {
			return cb(inode)
		}
		return b.inode.Range(nil, nil, callBackFunc)
	case DentryType:
		callBackFunc := func(dentry *Dentry) bool {
			return cb(dentry)
		}
		return b.dentry.Range(nil, nil, callBackFunc)
	case ExtendType:
		callBackFunc := func(extend *Extend) bool {
			return cb(extend)
		}
		return b.extend.Range(nil, nil, callBackFunc)
	case MultipartType:
		callBackFunc := func(multipart *Multipart) bool {
			return cb(multipart)
		}
		return b.multipart.Range(nil, nil, callBackFunc)
	case TransactionType:
		callBackFunc := func(tx *proto.TransactionInfo) bool {
			return cb(tx)
		}
		return b.transaction.Range(nil, nil, callBackFunc)
	case TransactionRollbackInodeType:
		callBackFunc := func(inode *TxRollbackInode) bool {
			return cb(inode)
		}
		return b.transactionRbInode.Range(nil, nil, callBackFunc)
	case TransactionRollbackDentryType:
		callBackFunc := func(dentry *TxRollbackDentry) bool {
			return cb(dentry)
		}
		return b.transactionRbDentry.Range(nil, nil, callBackFunc)
	default:
	}
	panic("out of type")
}

func (b *MemSnapShot) RangeReuseInode(cb func(item *Inode) bool) error {
	return b.inode.Range(nil, nil, cb)
}

func (b *MemSnapShot) RangeReuseDentry(cb func(item *Dentry) bool) error {
	return b.dentry.Range(nil, nil, cb)
}

// RangeRaw for memory mode - marshal items to bytes
func (b *MemSnapShot) RangeRaw(tp TreeType, cb func(key, value []byte) bool) error {
	return b.Range(tp, func(item interface{}) bool {
		// For memory mode, we need to marshal the item
		// This is not truly zero-copy, but maintains interface compatibility
		var key, value []byte
		switch tp {
		case InodeType:
			inode := item.(*Inode)
			key = inode.MarshalKey()
			value = inode.MarshalValue()
		case DentryType:
			dentry := item.(*Dentry)
			key = dentry.MarshalKey()
			value = dentry.MarshalValue()
		case ExtendType:
			extend := item.(*Extend)
			if bs, err := extend.Bytes(); err == nil {
				value = bs
			}
		case MultipartType:
			multipart := item.(*Multipart)
			if bs, err := multipart.Bytes(); err == nil {
				value = bs
			}
		case TransactionType:
			tx := item.(*proto.TransactionInfo)
			if bs, err := tx.Marshal(); err == nil {
				value = bs
			}
		case TransactionRollbackInodeType:
			rbInode := item.(*TxRollbackInode)
			if bs, err := rbInode.Marshal(); err == nil {
				value = bs
			}
		case TransactionRollbackDentryType:
			rbDentry := item.(*TxRollbackDentry)
			if bs, err := rbDentry.Marshal(); err == nil {
				value = bs
			}
		}
		return cb(key, value)
	})
}

func (b *MemSnapShot) Close() {}

func (b *MemSnapShot) Count(tp TreeType) uint64 {
	switch tp {
	case InodeType:
		return b.inode.Count()
	case DentryType:
		return b.dentry.Count()
	case ExtendType:
		return b.extend.Count()
	case MultipartType:
		return b.multipart.Count()
	case TransactionType:
		return b.transaction.Count()
	case TransactionRollbackInodeType:
		return b.transactionRbInode.Count()
	case TransactionRollbackDentryType:
		return b.transactionRbDentry.Count()
	case DeletedExtentsType:
		return b.deletedExtents.Count()
	case DeletedObjExtentsType:
		return b.deletedObjExtents.Count()
	default:
	}
	panic("out of type")
}

func (b *MemSnapShot) ApplyID() uint64 {
	return b.applyID
}

func (b *MemSnapShot) TxID() uint64 {
	return b.txID
}

var (
	_ InodeTree                     = &InodeBTree{}
	_ DentryTree                    = &DentryBTree{}
	_ ExtendTree                    = &ExtendBTree{}
	_ MultipartTree                 = &MultipartBTree{}
	_ TransactionTree               = &TransactionBTree{}
	_ TransactionRollbackInodeTree  = &TransactionRollbackInodeBTree{}
	_ TransactionRollbackDentryTree = &TransactionRollbackDentryBTree{}
)

type InodeBTree struct {
	*BTree
}

type DentryBTree struct {
	*BTree
}

type ExtendBTree struct {
	*BTree
}

type MultipartBTree struct {
	*BTree
}

type TransactionBTree struct {
	*BTree
}

type TransactionRollbackInodeBTree struct {
	*BTree
}

type TransactionRollbackDentryBTree struct {
	*BTree
}

type DeletedExtentsBTree struct {
	*BTree
}

type DeletedObjExtentsBTree struct {
	*BTree
}

func (i *InodeBTree) GetMaxInode() (uint64, error) {
	i.Lock()
	item := i.tree.Max()
	i.Unlock()
	if item == nil {
		return 0, nil
	}
	return item.(*Inode).Inode, nil
}

// get
func (i *InodeBTree) Get(ino *Inode) (*Inode, error) {
	item := i.BTree.Get(ino)
	if item != nil {
		return item.(*Inode), nil
	}
	return nil, nil
}

func (i *InodeBTree) CopyGet(ino *Inode) (*Inode, error) {
	item := i.BTree.CopyGet(ino)
	if item != nil {
		return item.(*Inode), nil
	}
	return nil, nil
}

func (i *DentryBTree) Get(dent *Dentry) (*Dentry, error) {
	item := i.BTree.Get(dent)
	if item != nil {
		return item.(*Dentry), nil
	}
	return nil, nil
}

func (i *DentryBTree) CopyGet(dent *Dentry) (*Dentry, error) {
	item := i.BTree.CopyGet(dent)
	if item != nil {
		return item.(*Dentry), nil
	}
	return nil, nil
}

func (i *ExtendBTree) Get(extent *Extend) (*Extend, error) {
	item := i.BTree.Get(extent)
	if item != nil {
		return item.(*Extend), nil
	}
	return nil, nil
}

func (i *ExtendBTree) CopyGet(extent *Extend) (*Extend, error) {
	item := i.BTree.CopyGet(extent)
	if item != nil {
		return item.(*Extend), nil
	}
	return nil, nil
}

func (i *MultipartBTree) Get(multi *Multipart) (*Multipart, error) {
	item := i.BTree.Get(multi)
	if item != nil {
		return item.(*Multipart), nil
	}
	return nil, nil
}

func (i *MultipartBTree) CopyGet(multi *Multipart) (*Multipart, error) {
	item := i.BTree.CopyGet(multi)
	if item != nil {
		return item.(*Multipart), nil
	}
	return nil, nil
}

func (i *TransactionBTree) Get(tx *proto.TransactionInfo) (*proto.TransactionInfo, error) {
	item := i.BTree.Get(tx)
	if item != nil {
		return item.(*proto.TransactionInfo), nil
	}
	return nil, nil
}

func (i *TransactionBTree) CopyGet(tx *proto.TransactionInfo) (*proto.TransactionInfo, error) {
	item := i.BTree.CopyGet(tx)
	if item != nil {
		return item.(*proto.TransactionInfo), nil
	}
	return nil, nil
}

func (i *TransactionRollbackInodeBTree) Get(inode *TxRollbackInode) (*TxRollbackInode, error) {
	item := i.BTree.Get(inode)
	if item != nil {
		return item.(*TxRollbackInode), nil
	}
	return nil, nil
}

func (i *TransactionRollbackInodeBTree) CopyGet(inode *TxRollbackInode) (*TxRollbackInode, error) {
	item := i.BTree.CopyGet(inode)
	if item != nil {
		return item.(*TxRollbackInode), nil
	}
	return nil, nil
}

func (i *TransactionRollbackDentryBTree) Get(dentry *TxRollbackDentry) (*TxRollbackDentry, error) {
	item := i.BTree.Get(dentry)
	if item != nil {
		return item.(*TxRollbackDentry), nil
	}
	return nil, nil
}

func (i *TransactionRollbackDentryBTree) CopyGet(dentry *TxRollbackDentry) (*TxRollbackDentry, error) {
	item := i.BTree.CopyGet(dentry)
	if item != nil {
		return item.(*TxRollbackDentry), nil
	}
	return nil, nil
}

// put
func (i *InodeBTree) Update(handle interface{}, inode *Inode) error {
	i.BTree.ReplaceOrInsert(inode, false)
	return nil
}

func (i *InodeBTree) Put(handle interface{}, inode *Inode) error {
	i.BTree.ReplaceOrInsert(inode, true)
	return nil
}

func (i *InodeBTree) Insert(handle interface{}, inode *Inode) error {
	i.BTree.Insert(inode)
	return nil
}

func (i *DentryBTree) Update(handle interface{}, dentry *Dentry) error {
	i.BTree.ReplaceOrInsert(dentry, false)
	return nil
}

func (i *DentryBTree) Put(handle interface{}, dentry *Dentry) error {
	i.BTree.ReplaceOrInsert(dentry, true)
	return nil
}

func (i *DentryBTree) Insert(handle interface{}, dentry *Dentry) error {
	i.BTree.Insert(dentry)
	return nil
}

func (i *ExtendBTree) Update(handle interface{}, extend *Extend) error {
	i.BTree.ReplaceOrInsert(extend, false)
	return nil
}

func (i *ExtendBTree) Put(handle interface{}, extend *Extend) error {
	i.BTree.ReplaceOrInsert(extend, true)
	return nil
}

func (i *ExtendBTree) Insert(handle interface{}, extend *Extend) error {
	i.BTree.Insert(extend)
	return nil
}

func (i *MultipartBTree) Update(handle interface{}, multipart *Multipart) error {
	i.BTree.ReplaceOrInsert(multipart, false)
	return nil
}

func (i *MultipartBTree) Put(handle interface{}, multipart *Multipart) error {
	i.BTree.ReplaceOrInsert(multipart, true)
	return nil
}

func (i *MultipartBTree) Insert(handle interface{}, multipart *Multipart) error {
	i.BTree.Insert(multipart)
	return nil
}

func (i *TransactionBTree) Update(handle interface{}, tx *proto.TransactionInfo) error {
	i.BTree.ReplaceOrInsert(tx, false)
	return nil
}

func (i *TransactionBTree) Put(handle interface{}, tx *proto.TransactionInfo) error {
	i.BTree.ReplaceOrInsert(tx, true)
	return nil
}

func (i *TransactionBTree) Insert(handle interface{}, tx *proto.TransactionInfo) error {
	i.BTree.Insert(tx)
	return nil
}

func (i *TransactionRollbackInodeBTree) Update(handle interface{}, inode *TxRollbackInode) error {
	i.BTree.ReplaceOrInsert(inode, false)
	return nil
}

func (i *TransactionRollbackInodeBTree) Put(handle interface{}, inode *TxRollbackInode) error {
	i.BTree.ReplaceOrInsert(inode, true)
	return nil
}

func (i *TransactionRollbackInodeBTree) Insert(handle interface{}, inode *TxRollbackInode) error {
	i.BTree.Insert(inode)
	return nil
}

func (i *TransactionRollbackDentryBTree) Update(handle interface{}, dentry *TxRollbackDentry) error {
	i.BTree.ReplaceOrInsert(dentry, false)
	return nil
}

func (i *TransactionRollbackDentryBTree) Put(handle interface{}, dentry *TxRollbackDentry) error {
	i.BTree.ReplaceOrInsert(dentry, true)
	return nil
}

func (i *TransactionRollbackDentryBTree) Insert(handle interface{}, dentry *TxRollbackDentry) error {
	i.BTree.Insert(dentry)
	return nil
}

// create
func (i *InodeBTree) ReplaceOrInsert(handle interface{}, inode *Inode, replace bool) (*Inode, bool, error) {
	item, ok := i.BTree.ReplaceOrInsert(inode, replace)
	if !ok {
		return item.(*Inode), ok, nil
	}
	return inode, ok, nil
}

func (i *InodeBTree) Insert(handle interface{}, inode *Inode) error {
	// Check if pipeline is active (managed in ApplySnapshot)
	if _, ok := batchInsertChannels.Load(i.BTree); ok {
		// Pipeline mode: send to channel
		return i.BTree.sendToPipeline(inode)
	}
	// Immediate insert mode
	i.BTree.Insert(inode)
	return nil
}

// PutRaw for memory mode: unmarshal key and value, then insert
// This is called when receiving opFSMRawInodeData from RocksDB leader
func (i *InodeBTree) PutRaw(handle interface{}, key, value []byte) error {
	ino := NewInode(0, 0)

	// Handle key format: RocksDB stores partitionId (8 bytes) + table_type (1 byte) + inode_id (8 bytes) = 17 bytes
	var keyData []byte
	if len(key) < 17 || key[8] != byte(InodeTable) {
		return fmt.Errorf("invalid inode key")
	}
	keyData = key[9:17]

	if err := ino.UnmarshalKey(keyData); err != nil {
		return fmt.Errorf("unmarshal inode key failed: %w", err)
	}

	// Handle value format: RocksDB stores full Marshal() data (keyLen + key + valLen + value)
	if err := ino.Unmarshal(value); err != nil {
		return fmt.Errorf("unmarshal inode (RocksDB format) failed: %w", err)
	}

	// Insert into memory tree
	return i.Insert(handle, ino)
}

func (i *DentryBTree) ReplaceOrInsert(handle interface{}, dentry *Dentry, replace bool) (*Dentry, bool, error) {
	item, ok := i.BTree.ReplaceOrInsert(dentry, replace)
	if !ok {
		return item.(*Dentry), ok, nil
	}
	return dentry, ok, nil
}

func (i *DentryBTree) Insert(handle interface{}, dentry *Dentry) error {
	if _, ok := batchInsertChannels.Load(i.BTree); ok {
		// Pipeline mode: send to channel
		return i.BTree.sendToPipeline(dentry)
	}
	// Immediate insert mode
	i.BTree.Insert(dentry)
	return nil
}

// PutRaw for memory mode: unmarshal key and value, then insert
// This is called when receiving opFSMRawDentryData from RocksDB leader
func (i *DentryBTree) PutRaw(handle interface{}, key, value []byte) error {
	dentry := &Dentry{}

	// Handle key format: RocksDB stores partitionId (8 bytes) + table_type (1 byte) + actual_key
	var keyData []byte
	if len(key) <= 9 || key[8] != byte(DentryTable) {
		return fmt.Errorf("invalid dentry key")
	}
	keyData = key[9:]

	if err := dentry.UnmarshalKey(keyData); err != nil {
		return fmt.Errorf("unmarshal dentry key failed: %w", err)
	}

	// Handle value format: RocksDB stores full Marshal() data (keyLen + key + valLen + value)
	if err := dentry.Unmarshal(value); err != nil {
		return fmt.Errorf("unmarshal dentry (RocksDB format) failed: %w", err)
	}

	// Insert into memory tree
	return i.Insert(handle, dentry)
}

func (i *ExtendBTree) ReplaceOrInsert(handle interface{}, extend *Extend, replace bool) (*Extend, bool, error) {
	item, ok := i.BTree.ReplaceOrInsert(extend, replace)
	if !ok {
		return item.(*Extend), ok, nil
	}
	return extend, ok, nil
}

func (i *ExtendBTree) Insert(handle interface{}, extend *Extend) error {
	if _, ok := batchInsertChannels.Load(i.BTree); ok {
		// Pipeline mode: send to channel
		return i.BTree.sendToPipeline(extend)
	}
	// Immediate insert mode
	i.BTree.Insert(extend)
	return nil
}

// PutRaw for memory mode: unmarshal key and value, then insert
// This is called when receiving opFSMRawExtendData from RocksDB leader
func (i *ExtendBTree) PutRaw(handle interface{}, key, value []byte) error {
	// Handle key format: RocksDB stores partitionId (8 bytes) + table_type (1 byte) + inode_id (8 bytes) = 17 bytes
	if len(key) < 17 || key[8] != byte(ExtendTable) {
		return fmt.Errorf("invalid extend key")
	}
	inodeID := binary.BigEndian.Uint64(key[9:17])

	extend, err := NewExtendFromBytes(value)
	if err != nil {
		return fmt.Errorf("unmarshal extend failed: %w", err)
	}
	extend.inode = inodeID

	// Insert into memory tree
	return i.Insert(handle, extend)
}

func (i *MultipartBTree) ReplaceOrInsert(handle interface{}, mul *Multipart, replace bool) (*Multipart, bool, error) {
	item, ok := i.BTree.ReplaceOrInsert(mul, replace)
	if !ok {
		return item.(*Multipart), ok, nil
	}
	return mul, ok, nil
}

func (i *MultipartBTree) Insert(handle interface{}, mul *Multipart) error {
	if _, ok := batchInsertChannels.Load(i.BTree); ok {
		// Pipeline mode: send to channel
		return i.BTree.sendToPipeline(mul)
	}
	// Immediate insert mode
	i.BTree.Insert(mul)
	return nil
}

// PutRaw for memory mode: unmarshal value, then insert
// This is called when receiving opFSMRawMultipartData from RocksDB leader
func (i *MultipartBTree) PutRaw(handle interface{}, key, value []byte) error {
	multipart := MultipartFromBytes(value)
	if multipart == nil {
		return fmt.Errorf("failed to unmarshal multipart from bytes")
	}

	// Insert into memory tree
	return i.Insert(handle, multipart)
}

func (i *TransactionBTree) ReplaceOrInsert(handle interface{}, tx *proto.TransactionInfo, replace bool) (*proto.TransactionInfo, bool, error) {
	item, ok := i.BTree.ReplaceOrInsert(tx, replace)
	if !ok {
		return item.(*proto.TransactionInfo), ok, nil
	}
	return tx, ok, nil
}

func (i *TransactionBTree) Insert(handle interface{}, tx *proto.TransactionInfo) error {
	if _, ok := batchInsertChannels.Load(i.BTree); ok {
		// Pipeline mode: send to channel
		return i.BTree.sendToPipeline(tx)
	}
	// Immediate insert mode
	i.BTree.Insert(tx)
	return nil
}

// PutRaw for memory mode: unmarshal value, then insert
// This is called when receiving opFSMRawTxData from RocksDB leader
func (i *TransactionBTree) PutRaw(handle interface{}, key, value []byte) error {
	txInfo := proto.NewTransactionInfo(0, proto.TxTypeUndefined)
	if err := txInfo.Unmarshal(value); err != nil {
		return fmt.Errorf("unmarshal transaction failed: %w", err)
	}

	// Insert into memory tree
	return i.Insert(handle, txInfo)
}

func (i *TransactionRollbackInodeBTree) ReplaceOrInsert(handle interface{}, inode *TxRollbackInode, replace bool) (*TxRollbackInode, bool, error) {
	item, ok := i.BTree.ReplaceOrInsert(inode, replace)
	if !ok {
		return item.(*TxRollbackInode), ok, nil
	}
	return inode, ok, nil
}

func (i *TransactionRollbackInodeBTree) Insert(handle interface{}, inode *TxRollbackInode) error {
	if _, ok := batchInsertChannels.Load(i.BTree); ok {
		// Pipeline mode: send to channel
		return i.BTree.sendToPipeline(inode)
	}
	// Immediate insert mode
	i.BTree.Insert(inode)
	return nil
}

// PutRaw for memory mode: unmarshal value, then insert
// This is called when receiving opFSMRawTxRbInodeData from RocksDB leader
func (i *TransactionRollbackInodeBTree) PutRaw(handle interface{}, key, value []byte) error {
	txRbInode := NewTxRollbackInode(nil, []uint32{}, nil, 0)
	if err := txRbInode.Unmarshal(value); err != nil {
		return fmt.Errorf("unmarshal tx rollback inode failed: %w", err)
	}

	// Insert into memory tree
	return i.Insert(handle, txRbInode)
}

func (i *TransactionRollbackDentryBTree) ReplaceOrInsert(handle interface{}, dentry *TxRollbackDentry, replace bool) (*TxRollbackDentry, bool, error) {
	item, ok := i.BTree.ReplaceOrInsert(dentry, replace)
	if !ok {
		return item.(*TxRollbackDentry), ok, nil
	}
	return dentry, ok, nil
}

func (i *TransactionRollbackDentryBTree) Insert(handle interface{}, dentry *TxRollbackDentry) error {
	if _, ok := batchInsertChannels.Load(i.BTree); ok {
		// Pipeline mode: send to channel
		return i.BTree.sendToPipeline(dentry)
	}
	// Immediate insert mode
	i.BTree.Insert(dentry)
	return nil
}

// PutRaw for memory mode: unmarshal value, then insert
// This is called when receiving opFSMRawTxRbDentryData from RocksDB leader
func (i *TransactionRollbackDentryBTree) PutRaw(handle interface{}, key, value []byte) error {
	txRbDentry := NewTxRollbackDentry(nil, nil, 0)
	if err := txRbDentry.Unmarshal(value); err != nil {
		return fmt.Errorf("unmarshal tx rollback dentry failed: %w", err)
	}

	// Insert into memory tree
	return i.Insert(handle, txRbDentry)
}

func (i *InodeBTree) Delete(handle interface{}, inode *Inode) (bool, error) {
	if v := i.BTree.Delete(inode); v == nil {
		return false, nil
	}
	return true, nil
}

func (i *DentryBTree) Delete(handle interface{}, dentry *Dentry) (bool, error) {
	if v := i.BTree.Delete(dentry); v == nil {
		return false, nil
	}
	return true, nil
}

func (i *ExtendBTree) Delete(handle interface{}, extend *Extend) (bool, error) {
	if v := i.BTree.Delete(extend); v == nil {
		return false, nil
	}
	return true, nil
}

func (i *MultipartBTree) Delete(handle interface{}, mutipart *Multipart) (bool, error) {
	if mul := i.BTree.Delete(mutipart); mul == nil {
		return false, nil
	}
	return true, nil
}

func (i *TransactionBTree) Delete(handle interface{}, txId string) (bool, error) {
	if tx := i.BTree.Delete(&proto.TransactionInfo{TxID: txId}); tx == nil {
		return false, nil
	}
	return true, nil
}

func (i *TransactionRollbackInodeBTree) Delete(handle interface{}, inode *TxRollbackInode) (bool, error) {
	if inode := i.BTree.Delete(inode); inode == nil {
		return false, nil
	}
	return true, nil
}

func (i *TransactionRollbackDentryBTree) Delete(handle interface{}, dentry *TxRollbackDentry) (bool, error) {
	if dentry := i.BTree.Delete(dentry); dentry == nil {
		return false, nil
	}
	return true, nil
}

// range
func (i *InodeBTree) Range(start, end *Inode, cb func(i *Inode) bool) error {
	var err error
	if start == nil {
		start = NewInode(0, 0)
	}

	callback := func(i BtreeItem) bool {
		return cb(i.(*Inode))
	}

	if end == nil {
		i.BTree.AscendGreaterOrEqual(start, callback)
	} else {
		i.BTree.AscendRange(start, end, callback)
	}
	return err
}

func (i *DentryBTree) Range(start, end *Dentry, cb func(d *Dentry) bool) error {
	var err error
	if start == nil {
		start = &Dentry{0, 0, "", 0, nil}
	}

	callback := func(i BtreeItem) bool {
		return cb(i.(*Dentry))
	}

	if end == nil {
		i.BTree.AscendGreaterOrEqual(start, callback)
	} else {
		i.BTree.AscendRange(start, end, callback)
	}
	return err
}

func (i *DentryBTree) RangeWithPrefix(prefix, start, end *Dentry, cb func(d *Dentry) bool) error {
	return i.Range(start, end, cb)
}

func (i *ExtendBTree) Range(start, end *Extend, cb func(e *Extend) bool) error {
	var err error
	if start == nil {
		start = &Extend{inode: 0}
	}

	callback := func(i BtreeItem) bool {
		return cb(i.(*Extend))
	}

	if end == nil {
		i.BTree.AscendGreaterOrEqual(start, callback)
	} else {
		i.BTree.AscendRange(start, end, callback)
	}

	return err
}

func (i *MultipartBTree) Range(start, end *Multipart, cb func(m *Multipart) bool) error {
	var err error
	callback := func(i BtreeItem) bool {
		return cb(i.(*Multipart))
	}

	if start == nil {
		start = &Multipart{key: "", id: ""}
	}

	if end == nil {
		i.BTree.AscendGreaterOrEqual(start, callback)
	} else {
		i.BTree.AscendRange(start, end, callback)
	}
	return err
}

func (i *MultipartBTree) RangeWithPrefix(prefix, start, end *Multipart, cb func(m *Multipart) bool) error {
	return i.Range(start, end, cb)
}

func (i *TransactionBTree) Range(start, end *proto.TransactionInfo, cb func(tx *proto.TransactionInfo) bool) error {
	var err error
	callback := func(i BtreeItem) bool {
		return cb(i.(*proto.TransactionInfo))
	}
	if start == nil {
		start = &proto.TransactionInfo{TxID: ""}
	}

	if end == nil {
		i.BTree.AscendGreaterOrEqual(start, callback)
	} else {
		i.BTree.AscendRange(start, end, callback)
	}
	return err
}

func (i *TransactionRollbackInodeBTree) Range(start, end *TxRollbackInode, cb func(inode *TxRollbackInode) bool) error {
	var err error
	callback := func(i BtreeItem) bool {
		return cb(i.(*TxRollbackInode))
	}
	if start == nil {
		start = &TxRollbackInode{
			inode: NewInode(0, 0),
		}
	}

	if end == nil {
		i.BTree.AscendGreaterOrEqual(start, callback)
	} else {
		i.BTree.AscendRange(start, end, callback)
	}
	return err
}

func (i *TransactionRollbackDentryBTree) Range(start, end *TxRollbackDentry, cb func(dentry *TxRollbackDentry) bool) error {
	var err error
	callback := func(i BtreeItem) bool {
		return cb(i.(*TxRollbackDentry))
	}

	if start == nil {
		start = &TxRollbackDentry{
			txDentryInfo: proto.NewTxDentryInfo("", 0, "", 0),
		}
	}

	if end == nil {
		i.BTree.AscendGreaterOrEqual(start, callback)
	} else {
		i.BTree.AscendRange(start, end, callback)
	}
	return err
}

func (i *TransactionRollbackDentryBTree) RangeWithPrefix(prefix, start, end *TxRollbackDentry, cb func(dentry *TxRollbackDentry) bool) error {
	return i.Range(start, end, cb)
}

// MaxItem returns the largest item in the btree.
func (i *InodeBTree) MaxItem() *Inode {
	i.RLock()
	item := i.tree.Max()
	i.RUnlock()
	if item == nil {
		return nil
	}
	return item.(*Inode)
}

// BTree is the wrapper of Google's btree.
type BTree struct {
	sync.RWMutex
	tree *btree.BTree
}

// NewBtree creates a new btree.
func NewBtree() *BTree {
	return &BTree{
		tree: btree.New(defaultBTreeDegree),
	}
}

// Get returns the object of the given key in the btree.
func (b *BTree) Get(key BtreeItem) (item BtreeItem) {
	b.RLock()
	item = b.tree.Get(key)
	b.RUnlock()
	return
}

func (b *BTree) CopyGet(key BtreeItem) (item BtreeItem) {
	b.Lock()
	item = b.tree.CopyGet(key)
	b.Unlock()
	return
}

// Find searches for the given key in the btree.
func (b *BTree) Find(key BtreeItem, fn func(i BtreeItem)) {
	b.RLock()
	item := b.tree.Get(key)
	b.RUnlock()
	if item == nil {
		return
	}
	fn(item)
}

func (b *BTree) CopyFind(key BtreeItem, fn func(i BtreeItem)) {
	b.Lock()
	item := b.tree.CopyGet(key)
	fn(item)
	b.Unlock()
}

// Has checks if the key exists in the btree.
func (b *BTree) Has(key BtreeItem) (ok bool) {
	b.RLock()
	ok = b.tree.Has(key)
	b.RUnlock()
	return
}

// Delete deletes the object by the given key.
func (b *BTree) Delete(key BtreeItem) (item BtreeItem) {
	b.Lock()
	item = b.tree.Delete(key)
	b.Unlock()
	return
}

func (b *BTree) Execute(fn func(tree interface{}) interface{}) interface{} {
	b.Lock()
	defer b.Unlock()
	return fn(b)
}

// ReplaceOrInsert is the wrapper of google's btree ReplaceOrInsert.
func (b *BTree) ReplaceOrInsert(key BtreeItem, replace bool) (item BtreeItem, ok bool) {
	b.Lock()
	if replace {
		item = b.tree.ReplaceOrInsert(key)
		b.Unlock()
		ok = true
		return
	}

	item = b.tree.Get(key)
	if item == nil {
		item = b.tree.ReplaceOrInsert(key)
		b.Unlock()
		ok = true
		return
	}
	ok = false
	b.Unlock()
	return
}

// Insert adds the item without existence check.
func (b *BTree) Insert(key BtreeItem) {
	b.Lock()
	b.tree.ReplaceOrInsert(key)
	b.Unlock()
}

// startInsertPipeline starts a goroutine that consumes items from channel and inserts them sequentially
// Uses batch insertion to reduce lock acquire/release overhead
func (b *BTree) startInsertPipeline(ch chan BtreeItem, wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		const batchSize = 100 // Batch size for reducing lock overhead
		batch := make([]BtreeItem, 0, batchSize)

		flushBatch := func() {
			if len(batch) == 0 {
				return
			}
			// Hold lock once for entire batch
			b.Lock()
			for _, item := range batch {
				b.tree.ReplaceOrInsert(item)
			}
			b.Unlock()
			batch = batch[:0] // Reset batch
		}

		for item := range ch {
			batch = append(batch, item)
			if len(batch) >= batchSize {
				flushBatch()
			}
		}
		// Flush remaining items
		flushBatch()
	}()
}

// startPipelineForSnapshot starts pipeline for a BTree instance (used in ApplySnapshot)
func (b *BTree) startPipelineForSnapshot() {
	ch := make(chan BtreeItem, 500)
	wg := &sync.WaitGroup{}

	// Start pipeline goroutine for sequential insert
	b.startInsertPipeline(ch, wg)

	// Store channel and wait group
	batchInsertChannels.Store(b, ch)
	batchInsertWaitGroups.Store(b, wg)
}

// stopPipelineForSnapshot stops pipeline for a BTree instance (used in ApplySnapshot)
func (b *BTree) stopPipelineForSnapshot() {
	ch, ok := batchInsertChannels.LoadAndDelete(b)
	if !ok {
		return
	}

	wg, _ := batchInsertWaitGroups.LoadAndDelete(b)

	// Close channel to signal no more items
	close(ch.(chan BtreeItem))

	// Wait for all pending inserts to complete
	if wg != nil {
		wg.(*sync.WaitGroup).Wait()
	}
}

// sendToPipeline sends an item to the insert pipeline channel if pipeline is active
// Blocks if channel is full to maintain pipeline ordering and avoid lock contention
func (b *BTree) sendToPipeline(item BtreeItem) error {
	ch, ok := batchInsertChannels.Load(b)
	if !ok {
		// No pipeline channel, fall back to immediate insert
		b.Insert(item)
		return nil
	}

	// Block until item is sent to maintain pipeline ordering
	// This ensures all inserts go through pipeline, avoiding lock contention
	ch.(chan BtreeItem) <- item
	return nil
}

// Ascend is the wrapper of the google's btree Ascend.
// This function scans the entire btree. When the data is huge, it is not recommended to use this function online.
// Instead, it is recommended to call GetTree to obtain the snapshot of the current btree, and then do the scan on the snapshot.
func (b *BTree) Ascend(fn func(i BtreeItem) bool) {
	b.RLock()
	b.tree.Ascend(fn)
	b.RUnlock()
}

// AscendRange is the wrapper of the google's btree AscendRange.
func (b *BTree) AscendRange(greaterOrEqual, lessThan BtreeItem, iterator func(i BtreeItem) bool) {
	b.RLock()
	b.tree.AscendRange(greaterOrEqual, lessThan, iterator)
	b.RUnlock()
}

// AscendGreaterOrEqual is the wrapper of the google's btree AscendGreaterOrEqual
func (b *BTree) AscendGreaterOrEqual(pivot BtreeItem, iterator func(i BtreeItem) bool) {
	b.RLock()
	b.tree.AscendGreaterOrEqual(pivot, iterator)
	b.RUnlock()
}

// GetTree returns the snapshot of a btree.
func (b *BTree) GetTree() *BTree {
	b.Lock()
	t := b.tree.Clone()
	b.Unlock()
	nb := NewBtree()
	nb.tree = t
	return nb
}

// Reset resets the current btree.
func (b *BTree) Reset() {
	b.Lock()
	b.tree.Clear(true)
	b.Unlock()
}

func (i *BTree) SetApplyID(index uint64) {
}

func (i *BTree) GetApplyID() uint64 {
	return 0
}

func (i *BTree) GetPersistentApplyID() uint64 {
	return 0
}

func (i *BTree) SetTxId(txId uint64) {
}

func (i *BTree) GetTxId() uint64 {
	return 0
}

func (i *BTree) PersistBaseInfo() error {
	return nil
}

func (i *BTree) CreateBatchWriteHandle() (interface{}, error) {
	return nil, nil
}

func (i *BTree) CommitBatchWrite(handle interface{}, needCommitApplyID bool) error {
	return nil
}

func (i *BTree) ReleaseBatchWriteHandle(handle interface{}) error {
	return nil
}

func (i *BTree) CommitAndReleaseBatchWriteHandle(handle interface{}, needCommitApplyID bool) error {
	return nil
}

func (i *BTree) CommitAndReleaseBatchWriteForClear(handle interface{}) error {
	return nil
}

func (i *BTree) ClearBatchWriteHandle(handle interface{}) error {
	return nil
}

func (i *BTree) SetCursor(cursor uint64) {
}

func (i *BTree) GetCursor() uint64 {
	return 0
}

func (i *BTree) Flush(block bool) error {
	return nil
}

func (i *BTree) Count() uint64 {
	return uint64(i.Len())
}

// real count by type
func (i *BTree) RealCount() uint64 {
	return uint64(i.Len())
}

// Len returns the total number of items in the btree.
func (b *BTree) Len() (size int) {
	b.RLock()
	size = b.tree.Len()
	b.RUnlock()
	return
}

func (b *BTree) Clear(handle interface{}) (err error) {
	b.Reset()
	return
}

func (b *BTree) DeleteMetadata(handle interface{}) (err error) {
	return
}

func (b *BTree) GetStoreMode() proto.StoreMode {
	return proto.StoreModeMem
}

func (b *BTree) GetUniqID() uint64 {
	return 0
}

func (b *BTree) SetUniqID(id uint64) {
}

func (b *BTree) GetApplyIdFromDisk() (uint64, error) {
	return 0, nil
}

func (i *InodeBTree) SetInodeCount(count uint64) {
}
