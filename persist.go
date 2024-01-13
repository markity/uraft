package uraft

import (
	"bytes"

	"github.com/markity/uraft/internal/structs"

	"github.com/markity/uraft/internal/labgob"
)

// 考虑到本地做压缩意义不大, 这里raft state直接用labgob压缩
func (rf *raft) persist(snapshot []byte) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	err := e.Encode(&rf.state.PersistInfo)
	if err != nil {
		panic(err)
	}
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, snapshot)
}

func (rf *raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		rf.state.PersistInfo.Logs = make([]structs.LogEntry, 1)
		rf.state.PersistInfo.Term = 0
		rf.state.PersistInfo.VotedForThisTerm = -1
		return
	}

	r := bytes.NewReader(data)
	d := labgob.NewDecoder(r)
	if err := d.Decode(&rf.state.PersistInfo); err != nil {
		panic(err)
	}
}
