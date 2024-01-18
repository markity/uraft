package persister

import (
	"errors"

	"github.com/tidwall/wal"
)

type Persister struct {
	RaftStateLog          *wal.Log
	RaftStateLastLogIndex int64
	RaftStateSize         int64
	SnapshotLog           *wal.Log
	SnapshotLastLogIndex  int64
	SnapshotSize          int64
}

func (p *Persister) Save(raftState []byte, snapshot []byte) {
	if raftState != nil {
		p.RaftStateLastLogIndex++
		p.RaftStateLog.Write(uint64(p.RaftStateLastLogIndex), raftState)
		p.RaftStateLog.TruncateFront(uint64(p.RaftStateLastLogIndex))
		p.RaftStateSize = int64(len(raftState))
	}

	if snapshot != nil {
		p.SnapshotLastLogIndex++
		p.SnapshotLog.Write(uint64(p.SnapshotLastLogIndex), snapshot)
		p.SnapshotLog.TruncateFront(uint64(p.SnapshotLastLogIndex))
		p.SnapshotSize = int64(len(snapshot))
	}
}

func (p *Persister) GetRaftState() []byte {
	if p.RaftStateLastLogIndex == 0 {
		return nil
	}
	bs, err := p.RaftStateLog.Read(uint64(p.RaftStateLastLogIndex))
	if err != nil {
		panic(err)
	}
	return bs
}

func (p *Persister) GetRaftStateSize() int64 {
	return p.RaftStateSize
}

func (p *Persister) GetSnapshot() []byte {
	if p.SnapshotLastLogIndex == 0 {
		return nil
	}
	bs, err := p.SnapshotLog.Read(uint64(p.SnapshotLastLogIndex))
	if err != nil {
		panic(err)
	}
	return bs
}

func (p *Persister) GetSnapshotSize() int64 {
	return p.SnapshotSize
}

// TODO: Cleanup会清除旧的raft state和日志, 但是可能比较耗时, 先不急着实现
func (p *Persister) Cleanup() {
	panic("not implemented yet")
}

func NewPersister(name string) (*Persister, error) {
	sl, err := wal.Open(name+"/snapshot", nil)
	if err != nil {
		return nil, err
	}

	rl, err := wal.Open(name+"/raftstate", nil)
	if err != nil {
		return nil, err
	}

	slidx, err := sl.LastIndex()
	if err != nil {
		return nil, err
	}

	rlidx, err := rl.LastIndex()
	if err != nil {
		return nil, err
	}

	data, err := sl.Read(slidx)
	if err != nil {
		if !errors.Is(err, wal.ErrNotFound) {
			panic(err)
		}
	}

	return &Persister{
		RaftStateLog:          rl,
		RaftStateLastLogIndex: int64(rlidx),
		SnapshotLog:           sl,
		SnapshotLastLogIndex:  int64(slidx),
		SnapshotSize:          int64(len(data)),
	}, nil
}
