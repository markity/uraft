package uraft

import (
	"net"
	"uraft/internal/pb/protobuf"
)

// 外部通过chan ApplyMsg收apply消息, 这个结构体需要导出
type ApplyMsg struct {
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

type aeMessage struct {
	ip      net.IP
	message protobuf.AppendEntriesRequest
}
