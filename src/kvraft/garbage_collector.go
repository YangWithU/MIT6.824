package kvraft

import (
	"6.5840/labgob"
	"bytes"
	"log"
)

const GCRatio = 0.8

func (kv *KVServer) makeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	encodes := []interface{}{&kv.snapShotIndex, &kv.db, &kv.maxClerkAppliedOpId}
	for _, val := range encodes {
		if err := e.Encode(val); err != nil {
			panic("failed to encode some field")
		}
	}
	return w.Bytes()
}

// 手动构造新snapshot
func (kv *KVServer) checkpoint(index int) {
	snapshot := kv.makeSnapshot()
	kv.rf.Snapshot(index, snapshot)
	kv.snapShotIndex = index
	log.Printf("S%v checkpoints (SI=%v)", kv.me, kv.snapShotIndex)

	/////////////////
	var snapshotIndex int
	db := make(map[string]string)
	maxAppliedOpIdOfClerk := make(map[int64]int)
	nw := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(nw)
	if d.Decode(&snapshotIndex) != nil || d.Decode(&db) != nil || d.Decode(&maxAppliedOpIdOfClerk) != nil {
		panic("failed to decode some fields")
	}
	log.Printf("S%v checkpoints with (SI=%v db=%v maxAppliedOpId=%v)", kv.me, snapshotIndex, db, maxAppliedOpIdOfClerk)
}

// 解析输入snapshot,存储结果到kv的snapShotIndex,db,maxClerkAppliedOpId
func (kv *KVServer) ingestSnapshot(snapshot []byte) {
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	decodes := []interface{}{&kv.snapShotIndex, &kv.db, &kv.maxClerkAppliedOpId}
	for _, val := range decodes {
		if d.Decode(val) != nil {
			panic("failed to decode some field")
		}
	}
	kv.lastAppliedIndex = kv.snapShotIndex

	log.Printf("S%v ingests snapshot (SI=%v)", kv.me, kv.snapShotIndex)
	log.Printf("S%v ingests with (SI=%v db=%v maxAppliedOpId=%v)",
		kv.me, kv.snapShotIndex, kv.db, kv.maxClerkAppliedOpId)
}

// RaftStateSize当前存所有snapshot大小大于maxRaftStateSize*0.8
func (kv *KVServer) approachGCLimit() bool {
	return float32(kv.persister.RaftStateSize()) > GCRatio*float32(kv.maxRaftStateSize)
}
