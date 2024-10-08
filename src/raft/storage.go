package raft

import (
	"6.5840/labgob"
	"bytes"
)

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	encodes := []interface{}{rf.currentTerm, rf.votedTo, rf.log.entries,
		rf.log.snapShot.Index, rf.log.snapShot.Term}
	for _, val := range encodes {
		if e.Encode(val) != nil {
			panic("failed to encode some rf field")
		}
	}
	raftState := w.Bytes()
	rf.persister.Save(raftState, rf.log.snapShot.Data)

	rf.logger.persistLog() // debug log
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	decodes := []interface{}{&rf.currentTerm, &rf.votedTo, &rf.log.entries,
		&rf.log.snapShot.Index, &rf.log.snapShot.Term}
	for _, val := range decodes {
		if d.Decode(val) != nil {
			panic("failed to decode some rf field")
		}
	}

	rf.log.toCompactSnapShot(SnapShot{
		Data:  rf.persister.ReadSnapshot(),
		Index: rf.log.snapShot.Index, // decode出来的index和term
		Term:  rf.log.snapShot.Term,
	})

	rf.logger.restoreLog() // debug log
}
