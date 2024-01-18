package uraft

import "net/netip"

// 外部通过chan ApplyMsg收apply消息, 这个结构体需要导出
type ApplyMsg struct {
	// noop优化: 用于强制提交日志, leader需要写入noop日志用于强制提交, ApplyMsg会发给上层应用告知目前最新
	IsNoop    bool
	NoopIndex int64
	NoopTerm  int64

	// command
	CommandValid bool
	CommandType  string
	CommandBytes []byte
	CommandIndex int64
	CommandTerm  int64 // 加上这个玩意是为了方便lab3及时返回给客户端no leader信息

	// snapshot
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int64
	SnapshotIndex int64
}

type RaftConfig struct {
	// IF <= 0, default 300
	ElectionIntervalLowMS int

	// IF <= 0, default ElectionIntervalLowMS + 150
	ElectiontIntervalHighMS int

	// IF <= 0, default 100
	HeartbeatIntervalMS int

	// IF <= 0, default Unlimited
	MaxAppendEntries int64

	// IF <= 0, default 0
	ApplyChSize int

	// Peers[Me] is current raft server's ip port
	Peers []netip.AddrPort
	Me    int64

	// where to store data, both raft state info and snapshot info
	LogPath string
}
