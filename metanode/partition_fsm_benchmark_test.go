package metanode

import (
	"testing"

	"github.com/cubefs/cubefs/proto"
)

func newMpForFsmBench(storeMode proto.StoreMode) *metaPartition {
	config := getMpConfigForFsmTest(storeMode)
	mp := newPartition(config, newManager())
	mp.manager.metaNode = &MetaNode{
		raftSyncSnapFormatVersion: SnapFormatVersion_1,
	}
	mp.uniqChecker = newUniqChecker()
	mp.multiVersionList = &proto.VolVersionInfoList{}
	return mp
}

func prepareDataForMpFsmBench(b *testing.B, mp *metaPartition) {
	handle, err := mp.inodeTree.CreateBatchWriteHandle()
	if err != nil {
		b.Fatal(err)
	}

	ino := NewInode(0, DirModeType)
	if _, _, err = mp.inodeTree.ReplaceOrInsert(handle, ino, true); err != nil {
		b.Fatal(err)
	}

	den := &Dentry{
		ParentId: 0,
		Name:     "test",
		Inode:    1,
	}
	if _, _, err = mp.dentryTree.ReplaceOrInsert(handle, den, true); err != nil {
		b.Fatal(err)
	}

	if _, _, err = mp.extendTree.ReplaceOrInsert(handle, &Extend{}, true); err != nil {
		b.Fatal(err)
	}

	if _, _, err = mp.multipartTree.ReplaceOrInsert(handle, &Multipart{}, true); err != nil {
		b.Fatal(err)
	}

	if _, _, err = mp.txProcessor.txManager.txTree.ReplaceOrInsert(handle, proto.NewTransactionInfo(0, 0), true); err != nil {
		b.Fatal(err)
	}

	if _, _, err = mp.txProcessor.txResource.txRbInodeTree.ReplaceOrInsert(
		handle,
		NewTxRollbackInode(ino, []uint32{}, proto.NewTxInodeInfo("", 0, 0), 0),
		true,
	); err != nil {
		b.Fatal(err)
	}

	if _, _, err = mp.txProcessor.txResource.txRbDentryTree.ReplaceOrInsert(
		handle,
		NewTxRollbackDentry(den, proto.NewTxDentryInfo("", 0, "", 0), 0),
		true,
	); err != nil {
		b.Fatal(err)
	}

	if err = mp.inodeTree.CommitAndReleaseBatchWriteHandle(handle, true); err != nil {
		b.Fatal(err)
	}

	handle, err = mp.inodeTree.CreateBatchWriteHandle()
	if err != nil {
		b.Fatal(err)
	}
	mp.inodeTree.SetApplyID(10)
	mp.applyID = 10
	if err = mp.inodeTree.CommitAndReleaseBatchWriteHandle(handle, true); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkApplySnapshot_Rocksdb(b *testing.B) {
	benchmarkApplySnapshot(b, proto.StoreModeRocksDb)
}

func BenchmarkApplySnapshot_Mem(b *testing.B) {
	benchmarkApplySnapshot(b, proto.StoreModeMem)
}

func benchmarkApplySnapshot(b *testing.B, storeMode proto.StoreMode) {
	leaderMp := newMpForFsmBench(storeMode)
	followerMp := newMpForFsmBench(storeMode)
	prepareDataForMpFsmBench(b, leaderMp)

	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		iter, err := leaderMp.Snapshot()
		if err != nil {
			b.Fatal(err)
		}

		done := make(chan error, 1)
		go func() {
			sm := <-followerMp.storeChan
			done <- followerMp.store(sm)
		}()

		b.StartTimer()
		err = followerMp.ApplySnapshot(nil, iter)
		b.StopTimer()

		iter.Close()

		if err != nil {
			b.Fatal(err)
		}
		if err := <-done; err != nil {
			b.Fatal(err)
		}
	}
}
