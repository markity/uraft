package uraft

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
	HeartbeatIntervalLowMS  int
	HeartbeatIntervalHighMS int
	ElectionIntervalMS      int
}
