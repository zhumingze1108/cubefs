package metanode

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestInodeKeyEncodingCompatibility(t *testing.T) {
	partitionId := uint64(12345)
	inodeId := uint64(67890)

	// Construct RocksTree
	tree := &RocksTree{partitionId: partitionId}

	// Method 1: GetRocksdbNormalKey + inodeEncodingKey
	keyBuf := tree.GetRocksdbNormalKey(byte(InodeTable))
	defer PutRocksdbNormalKey(keyBuf)
	key1 := inodeEncodingKey(keyBuf, inodeId)

	// Method 2: inodeEncodingKeyV0 + warpKeyV0
	keyV0 := inodeEncodingKeyV0(inodeId)
	key2 := tree.warpKeyV0(keyV0)

	if !bytes.Equal(key1, key2) {
		t.Errorf("key1 != key2\nkey1: %v\nkey2: %v", key1, key2)
	}
}

func TestDentryKeyEncodingCompatibility(t *testing.T) {
	partitionId := uint64(12345)
	parentId := uint64(67890)
	name := "test_dentry"

	// Construct RocksTree
	tree := &RocksTree{partitionId: partitionId}

	// Method 1: GetRocksdbLongKey + dentryEncodingKey
	keyBuf := tree.GetRocksdbLongKey(byte(DentryTable))
	defer PutRocksdbLongKey(keyBuf)
	key1 := dentryEncodingKey(keyBuf, parentId, name)

	// Method 2: dentryEncodingKeyV0 + warpKeyV0
	keyV0 := dentryEncodingKeyV0(parentId, name)
	key2 := tree.warpKeyV0(keyV0)

	if !bytes.Equal(key1, key2) {
		t.Errorf("DentryTable key1 != key2\nkey1: %v\nkey2: %v", key1, key2)
	}
}

func TestExtendKeyEncodingCompatibility(t *testing.T) {
	partitionId := uint64(12345)
	ino := uint64(67890)
	tree := &RocksTree{partitionId: partitionId}

	keyBuf := tree.GetRocksdbNormalKey(byte(ExtendTable))
	defer PutRocksdbNormalKey(keyBuf)
	key1 := extendEncodingKey(keyBuf, ino)

	keyV0 := extendEncodingKeyV0(ino)
	key2 := tree.warpKeyV0(keyV0)

	if !bytes.Equal(key1, key2) {
		t.Errorf("ExtendTable key1 != key2\nkey1: %v\nkey2: %v", key1, key2)
	}
}

func TestMultipartKeyEncodingCompatibility(t *testing.T) {
	partitionId := uint64(12345)
	keyStr := "multipart_key"
	id := "part_id"
	tree := &RocksTree{partitionId: partitionId}

	keyBuf := tree.GetRocksdbLongKey(byte(MultipartTable))
	defer PutRocksdbLongKey(keyBuf)
	key1 := multipartEncodingKey(keyBuf, keyStr, id)

	keyV0 := multipartEncodingKeyV0(keyStr, id)
	key2 := tree.warpKeyV0(keyV0)

	if !bytes.Equal(key1, key2) {
		t.Errorf("MultipartTable key1 != key2\nkey1: %v\nkey2: %v", key1, key2)
	}
}

func TestTransactionKeyEncodingCompatibility(t *testing.T) {
	partitionId := uint64(12345)
	txId := "tx_abc"
	tree := &RocksTree{partitionId: partitionId}

	keyBuf := tree.GetRocksdbLongKey(byte(TransactionTable))
	defer PutRocksdbLongKey(keyBuf)
	key1 := transactionEncodingKey(keyBuf, txId)

	keyV0 := transactionEncodingKeyV0(txId)
	key2 := tree.warpKeyV0(keyV0)

	if !bytes.Equal(key1, key2) {
		t.Errorf("TransactionTable key1 != key2\nkey1: %v\nkey2: %v", key1, key2)
	}
}

func TestTransactionRollbackInodeKeyEncodingCompatibility(t *testing.T) {
	partitionId := uint64(12345)
	ino := uint64(67890)
	tree := &RocksTree{partitionId: partitionId}

	keyBuf := tree.GetRocksdbNormalKey(byte(TransactionRollbackInodeTable))
	defer PutRocksdbNormalKey(keyBuf)
	key1 := transactionRollbackInodeEncodingKey(keyBuf, ino)

	keyV0 := transactionRollbackInodeEncodingKeyV0(ino)
	key2 := tree.warpKeyV0(keyV0)

	if !bytes.Equal(key1, key2) {
		t.Errorf("TransactionRollbackInodeTable key1 != key2\nkey1: %v\nkey2: %v", key1, key2)
	}
}

func TestTransactionRollbackDentryKeyEncodingCompatibility(t *testing.T) {
	partitionId := uint64(12345)
	parentId := uint64(67890)
	name := "tx_dentry"
	tree := &RocksTree{partitionId: partitionId}

	keyBuf := tree.GetRocksdbLongKey(byte(TransactionRollbackDentryTable))
	defer PutRocksdbLongKey(keyBuf)
	key1 := transactionRollbackDentryEncodingKey(keyBuf, parentId, name)

	keyV0 := transactionRollbackDentryEncodingKeyV0(parentId, name)
	key2 := tree.warpKeyV0(keyV0)

	if !bytes.Equal(key1, key2) {
		t.Errorf("TransactionRollbackDentryTable key1 != key2\nkey1: %v\nkey2: %v", key1, key2)
	}
}

func TestBaseInfoKeyEncodingCompatibility(t *testing.T) {
	partitionId := uint64(12345)
	tree := &RocksTree{partitionId: partitionId}

	keyBuf := tree.GetRocksdbNormalKey(byte(BaseInfoType))
	defer PutRocksdbNormalKey(keyBuf)
	key1 := keyBuf.Bytes()

	key2 := tree.warpKeyV0([]byte{byte(BaseInfoType)})

	if !bytes.Equal(key1, key2) {
		t.Errorf("BaseInfoTable key1 != key2\nkey1: %v\nkey2: %v", key1, key2)
	}
}

func TestRocksBaseInfoMarshalCompatibility(t *testing.T) {
	info := &RocksBaseInfo{
		version:           1,
		length:            128,
		applyId:           1001,
		inodeCnt:          2002,
		dentryCnt:         3003,
		extendCnt:         4004,
		multiCnt:          5005,
		persistentApplyId: 1001,
		cursor:            6006,
		txCnt:             7007,
		txRbInodeCnt:      8008,
		txRbDentryCnt:     9009,
		txId:              1010,
		uniqID:            2020,
	}

	dataV0, err := info.MarshalV0()
	if err != nil {
		t.Fatalf("MarshalV0 failed: %v", err)
	}
	data, err := info.Marshal()
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	if !bytes.Equal(dataV0, data) {
		t.Errorf("MarshalV0 and Marshal result not equal\nMarshalV0: %v\nMarshal: %v", dataV0, data)
	}
}

func TestRocksBaseInfoMarshalWithoutApplyIDCompatibility(t *testing.T) {
	info := &RocksBaseInfo{
		version:           2,
		length:            256,
		persistentApplyId: 1002,
		inodeCnt:          2003,
		dentryCnt:         3004,
		extendCnt:         4005,
		multiCnt:          5006,
		cursor:            6007,
		txCnt:             7008,
		txRbInodeCnt:      8009,
		txRbDentryCnt:     9010,
		txId:              1011,
		uniqID:            2021,
	}

	dataV0, err := info.MarshalWithoutApplyIDV0()
	if err != nil {
		t.Fatalf("MarshalWithoutApplyIDV0 failed: %v", err)
	}
	data, err := info.MarshalWithoutApplyID()
	if err != nil {
		t.Fatalf("MarshalWithoutApplyID failed: %v", err)
	}

	if !bytes.Equal(dataV0, data) {
		t.Errorf("MarshalWithoutApplyIDV0 and MarshalWithoutApplyID result not equal\nMarshalWithoutApplyIDV0: %v\nMarshalWithoutApplyID: %v", dataV0, data)
	}
}

func TestInodeRocksKVFromMemorySnapshot_EncodingCompatibility(t *testing.T) {
	partitionId := uint64(12345)
	inodeId := uint64(67890)
	snapK := make([]byte, 8)
	binary.BigEndian.PutUint64(snapK, inodeId)
	snapV := []byte("inode_value_payload")

	// Expected (legacy make+offset layout)
	expKey := make([]byte, 8+1+8)
	binary.BigEndian.PutUint64(expKey[0:8], partitionId)
	expKey[8] = byte(InodeTable)
	copy(expKey[9:], snapK)

	expVal := make([]byte, 4+8+4+len(snapV))
	binary.BigEndian.PutUint32(expVal[0:4], uint32(8))
	copy(expVal[4:12], snapK)
	binary.BigEndian.PutUint32(expVal[12:16], uint32(len(snapV)))
	copy(expVal[16:], snapV)

	keyBuf := GetRocksdbNormalKey()
	defer PutRocksdbNormalKey(keyBuf)
	valBuf := GetRocksdbValueBuf()
	defer PutRocksdbValueBuf(valBuf)

	gotKey, gotVal := inodeRocksKVFromMemorySnapshot(partitionId, keyBuf, valBuf, snapK, snapV)

	if !bytes.Equal(expKey, gotKey) {
		t.Fatalf("inode rocksKey mismatch\nexp: %v\ngot: %v", expKey, gotKey)
	}
	if !bytes.Equal(expVal, gotVal) {
		t.Fatalf("inode rocksValue mismatch\nexp: %v\ngot: %v", expVal, gotVal)
	}
}

func TestDentryRocksKVFromMemorySnapshot_EncodingCompatibility(t *testing.T) {
	partitionId := uint64(12345)
	parentId := uint64(67890)
	name := []byte("test_dentry")

	snapK := make([]byte, 8+len(name))
	binary.BigEndian.PutUint64(snapK[0:8], parentId)
	copy(snapK[8:], name)
	snapV := []byte("dentry_value_payload")

	// Expected (legacy make+offset layout)
	expKey := make([]byte, 8+1+8+1+len(name))
	binary.BigEndian.PutUint64(expKey[0:8], partitionId)
	expKey[8] = byte(DentryTable)
	binary.BigEndian.PutUint64(expKey[9:17], parentId)
	expKey[17] = 0
	copy(expKey[18:], name)

	keyDataWithSep := expKey[9:]
	expVal := make([]byte, 4+len(keyDataWithSep)+4+len(snapV))
	binary.BigEndian.PutUint32(expVal[0:4], uint32(len(keyDataWithSep)))
	copy(expVal[4:4+len(keyDataWithSep)], keyDataWithSep)
	binary.BigEndian.PutUint32(expVal[4+len(keyDataWithSep):4+len(keyDataWithSep)+4], uint32(len(snapV)))
	copy(expVal[4+len(keyDataWithSep)+4:], snapV)

	keyBuf := GetRocksdbLongKey()
	defer PutRocksdbLongKey(keyBuf)
	valBuf := GetRocksdbValueBuf()
	defer PutRocksdbValueBuf(valBuf)

	gotKey, gotVal := dentryRocksKVFromMemorySnapshot(partitionId, keyBuf, valBuf, snapK, snapV)

	if !bytes.Equal(expKey, gotKey) {
		t.Fatalf("dentry rocksKey mismatch\nexp: %v\ngot: %v", expKey, gotKey)
	}
	if !bytes.Equal(expVal, gotVal) {
		t.Fatalf("dentry rocksValue mismatch\nexp: %v\ngot: %v", expVal, gotVal)
	}
}
