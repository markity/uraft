package structs

type GetStateInfo struct {
	Term          int64
	Isleader      bool
	RaftStateSize int64
}

type SendCmdChanInfo struct {
	CommandType  string
	CommandBytes []byte
	Resp         chan SendCmdRespInfo
}

type SendCmdRespInfo struct {
	Term     int64
	Index    int64
	IsLeader bool
}

type Message struct {
	Term int64
	Msg  interface{}
}

type RaftState struct {
	// State = follower candidate leader之一
	State string

	CommitIndex int64

	FollowerState
	CandidateState
	LeaderState

	// 带有需要持久化的状态, 里面的内容一旦改变就persist
	PersistInfo
}

// 初始为follower, follower在某个term只能投票一次
type FollowerState struct {
}

// 候选者, 当follower超时后, 递增term并进入这个状态, 在开始选举的时候发一次
// request vote请求, 实际实现是在进入candidate状态后会发送多次request vote, 但是
// 我简化了步骤, 只发一次
// TODO: 实现超时前多次发送
type CandidateState struct {
	ReceivedNAgrees int
}

type LeaderState struct {
	// 存储每个节点上个日志的日志索引
	NextLogIndex []int64
	MatchIndex   []int64
}

type PersistInfo struct {
	Term              int64
	Logs              Logs
	VotedForThisTerm  int64
	LastIncludedIndex int64
	LastIncludedTerm  int64
}

type DoSnapshotInfo struct {
	Index    int64
	SnapShot []byte

	SnapshotOKChan chan struct{}
}
