package uraft

import (
	"net"
	"sync/atomic"
	"time"

	unboundedqueue "github.com/markity/uraft/internal/unbounded_queue"

	pers "github.com/markity/uraft/internal/persister"
	"github.com/markity/uraft/internal/structs"
)

// RaftIface不是并发安全的
type RaftIface interface {
	Start() chan ApplyMsg
	Stop()
	GetRaftState() (isLeader bool, term int64, raftStateSize int64)
	StartCommand(commandType string, commandBytes []byte) (idx int64, term int64, isLeader bool)
	Snapshot(snapshotBytes []byte, idx int64)
}

type raft struct {
	cfg RaftConfig

	reqDead         chan chan struct{}
	reqGetState     chan chan structs.GetStateInfo
	sendCmdChan     chan structs.SendCmdChanInfo
	messagePipeLine chan structs.Message
	snapshotChan    chan structs.DoSnapshotInfo

	applyCh    chan ApplyMsg
	applyQueue *unboundedqueue.UnboundedQueue
	timer      <-chan time.Time
	state      structs.RaftState

	serverCloseChan chan struct{}

	persister *pers.Persister

	listener *net.TCPListener

	// 1 is started, 0 is stopped
	started int64
}

func (rf *raft) Start() chan ApplyMsg {
	ok := atomic.CompareAndSwapInt64(&rf.started, 0, 1)
	if ok {
		queue := unboundedqueue.NewUnboundedQueue()

		rf.applyQueue = queue
		rf.messagePipeLine = make(chan structs.Message)
		rf.reqDead = make(chan chan struct{})
		rf.reqGetState = make(chan chan structs.GetStateInfo)
		rf.sendCmdChan = make(chan structs.SendCmdChanInfo)
		rf.snapshotChan = make(chan structs.DoSnapshotInfo)
		applyCh := make(chan ApplyMsg)
		rf.applyCh = applyCh
		rf.serverCloseChan = make(chan struct{}, 1)
		var err error
		rf.persister, err = pers.NewPersister(rf.cfg.LogPath)
		if err != nil {
			panic(err)
		}
		ip := rf.cfg.Peers[rf.cfg.Me].Addr().As4()
		port := rf.cfg.Peers[rf.cfg.Me].Port()
		listener, err := net.ListenTCP("tcp", &net.TCPAddr{
			IP:   net.IPv4(ip[0], ip[1], ip[2], ip[3]),
			Port: int(port),
		})
		if err != nil {
			panic(err)
		}
		rf.listener = listener
		if err != nil {
			panic(err)
		}

		go rf.stateMachine()
		return applyCh
	} else {
		panic("started already")
	}
}

func (rf *raft) Stop() {
	ok := atomic.CompareAndSwapInt64(&rf.started, 1, 0)
	if ok {
		rf.applyQueue.Close()
		rf.listener.Close()
		c := make(chan struct{}, 1)
		rf.reqDead <- c
		<-c
	} else {
		panic("stopped already")
	}
}

func (rf *raft) GetRaftState() (isLeader bool, term int64, raftStateSize int64) {
	ok := atomic.LoadInt64(&rf.started)
	if ok == 0 {
		panic("raft stopped")
	}

	c := make(chan structs.GetStateInfo, 1)
	rf.reqGetState <- c
	s := <-c
	return s.Isleader, s.Term, s.RaftStateSize
}

func (rf *raft) StartCommand(typ string, bs []byte) (idx int64, term int64, isLeader bool) {
	c := make(chan structs.SendCmdRespInfo, 1)
	rf.sendCmdChan <- structs.SendCmdChanInfo{
		CommandType:  typ,
		CommandBytes: bs,
		Resp:         c,
	}
	s := <-c
	return s.Index, s.Term, s.IsLeader
}

func (rf *raft) Snapshot(snapshotBytes []byte, idx int64) {
	c := make(chan struct{}, 1)
	rf.snapshotChan <- structs.DoSnapshotInfo{
		SnapShot:       snapshotBytes,
		SnapshotOKChan: c,
		Index:          idx,
	}
	<-c
}

func NewRaft(config RaftConfig) RaftIface {
	_ = config.Peers[config.Me] // 试探是否越界
	if config.ElectionIntervalLowMS <= 0 {
		config.ElectionIntervalLowMS = 300
	}
	if config.ElectiontIntervalHighMS <= 0 {
		config.ElectiontIntervalHighMS = config.ElectionIntervalLowMS + 150
	}
	if config.HeartbeatIntervalMS <= 0 {
		config.HeartbeatIntervalMS = 100
	}
	if config.MaxAppendEntries <= 0 {
		config.MaxAppendEntries = 0
	}
	if len(config.Peers) == 0 {
		panic("len(peers) cannot be 0")
	}
	return &raft{
		cfg: config,
	}
}
