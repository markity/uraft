package persister

import "github.com/tidwall/wal"

type Persister struct {
	RaftStateLog          *wal.Log
	RaftStateLastLogIndex int64
	SnapshotLog           *wal.Log
	SnapshotLastLogIndex  int64
	SnapshotSize          int64
}

func (p *Persister) Save(raftState []byte, snapshot []byte) {
	if raftState != nil {
		p.RaftStateLastLogIndex++
		p.RaftStateLog.Write(uint64(p.RaftStateLastLogIndex), raftState)
	}

	if snapshot != nil {
		p.SnapshotLastLogIndex++
		p.SnapshotLog.Write(uint64(p.SnapshotLastLogIndex), snapshot)
		p.SnapshotSize = int64(len(snapshot))
	}
}

func (p *Persister) GetRaftState() []byte {
	bs, err := p.RaftStateLog.Read(uint64(p.RaftStateLastLogIndex))
	if err != nil {
		panic(err)
	}
	return bs
}

func (p *Persister) GetSnapshot() []byte {
	bs, err := p.SnapshotLog.Read(uint64(p.RaftStateLastLogIndex))
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
		panic(err)
	}

	return &Persister{
		RaftStateLog:          rl,
		RaftStateLastLogIndex: int64(rlidx),
		SnapshotLog:           sl,
		SnapshotLastLogIndex:  int64(slidx),
		SnapshotSize:          int64(len(data)),
	}, nil
}
