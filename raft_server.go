package uraft

import (
	"context"
	"errors"
	"io"
	"uraft/internal/pb/protobuf"
	"uraft/internal/structs"

	quic "github.com/quic-go/quic-go"
	"google.golang.org/protobuf/proto"
)

// 启动quic server, closeChan用来防止阻塞在<-messageChan上
// TODO: 现在接收连接是串行的, 是否需要并发处理?
func runQuicServer(listener *quic.Listener, messageChan chan structs.Message, closeChan chan struct{}) {
	for {
		conn, err := listener.Accept(context.Background())
		if err != nil {
			if errors.Is(err, quic.ErrServerClosed) {
				return
			}
			continue
		}
		stream, err := conn.OpenStream()
		if err != nil {
			continue
		}
		bs, err := io.ReadAll(stream)
		if err != nil {
			stream.Close()
			continue
		}
		stream.Close()

		// 分包格式, 第一个字节表示包的类型, 接下来的所有字节构成包的本体
		typ := bs[0]
		packet := bs[1:]
		var term int64
		var msg interface{}
		switch typ {
		// vote req
		case 1:
			s := protobuf.VoteRequest{}
			err := proto.Unmarshal(packet, &s)
			if err != nil {
				panic(err)
			}
			msg = &s
			term = s.Term
		// vote reply
		case 2:
			s := protobuf.VoteReply{}
			err := proto.Unmarshal(packet, &s)
			if err != nil {
				panic(err)
			}
			msg = &s
		// ae req
		case 3:
			s := protobuf.AppendEntriesRequest{}
			err := proto.Unmarshal(packet, &s)
			if err != nil {
				panic(err)
			}
			msg = &s
			term = s.Term
		// ae reply
		case 4:
			s := protobuf.AppendEntriesReply{}
			err := proto.Unmarshal(packet, &s)
			if err != nil {
				panic(err)
			}
			msg = &s
		// install req
		case 5:
			s := protobuf.InstallSnapshotRequest{}
			err := proto.Unmarshal(packet, &s)
			if err != nil {
				panic(err)
			}
			msg = &s
			term = s.Term
		// install reply
		case 6:
			s := protobuf.InstallSnapshotReply{}
			err := proto.Unmarshal(packet, &s)
			if err != nil {
				panic(err)
			}
			msg = &s
			term = s.Term
		default:
			panic("unexpected")
		}

		for {
			select {
			case messageChan <- structs.Message{
				Term: term,
				Msg:  msg,
			}:
			case <-closeChan:
				return
			}
		}
	}
}
