package uraft

import (
	"bytes"
	"context"
	"uraft/internal/pb/protobuf"

	"github.com/quic-go/quic-go"
	"google.golang.org/protobuf/proto"
)

// packet 1
func (rf *raft) sendVoteRequest(to int64, req *protobuf.VoteRequest) {
	peer := rf.peersIP[to]
	go func() {
		conn, err := quic.DialAddr(context.Background(), peer.String(), nil, nil)
		if err != nil {
			return
		}
		defer conn.CloseWithError(0, "close")

		stream, err := conn.OpenStream()
		if err != nil {
			return
		}
		defer stream.Close()

		buf := bytes.Buffer{}
		bs, err := proto.Marshal(req)
		if err != nil {
			panic(err)
		}
		buf.WriteByte(1)
		buf.Write(bs)
		stream.Write(buf.Bytes())
	}()
}

// packet 2
func (rf *raft) sendVoteReply(to int64, req *protobuf.VoteReply) {
	peer := rf.peersIP[to]
	go func() {
		conn, err := quic.DialAddr(context.Background(), peer.String(), nil, nil)
		if err != nil {
			return
		}
		defer conn.CloseWithError(0, "close")

		stream, err := conn.OpenStream()
		if err != nil {
			return
		}
		defer stream.Close()

		buf := bytes.Buffer{}
		bs, err := proto.Marshal(req)
		if err != nil {
			panic(err)
		}
		buf.WriteByte(2)
		buf.Write(bs)
		stream.Write(buf.Bytes())
	}()
}

// packet 3
func (rf *raft) sendAppendEntriesRequest(to int64, req *protobuf.AppendEntriesRequest) {
	peer := rf.peersIP[to]
	go func() {
		conn, err := quic.DialAddr(context.Background(), peer.String(), nil, nil)
		if err != nil {
			return
		}
		defer conn.CloseWithError(0, "close")

		stream, err := conn.OpenStream()
		if err != nil {
			return
		}
		defer stream.Close()

		buf := bytes.Buffer{}
		bs, err := proto.Marshal(req)
		if err != nil {
			panic(err)
		}
		buf.WriteByte(3)
		buf.Write(bs)
		stream.Write(buf.Bytes())
	}()
}

// packet 4
func (rf *raft) sendAppendEntriesReply(to int64, req *protobuf.AppendEntriesReply) {
	peer := rf.peersIP[to]
	go func() {
		conn, err := quic.DialAddr(context.Background(), peer.String(), nil, nil)
		if err != nil {
			return
		}
		defer conn.CloseWithError(0, "close")

		stream, err := conn.OpenStream()
		if err != nil {
			return
		}
		defer stream.Close()

		buf := bytes.Buffer{}
		bs, err := proto.Marshal(req)
		if err != nil {
			panic(err)
		}
		buf.WriteByte(4)
		buf.Write(bs)
		stream.Write(buf.Bytes())
	}()
}

// packet 5
func (rf *raft) sendInstallSnapshotRequest(to int64, req *protobuf.InstallSnapshotRequest) {
	peer := rf.peersIP[to]
	go func() {
		conn, err := quic.DialAddr(context.Background(), peer.String(), nil, nil)
		if err != nil {
			return
		}
		defer conn.CloseWithError(0, "close")

		stream, err := conn.OpenStream()
		if err != nil {
			return
		}
		defer stream.Close()

		buf := bytes.Buffer{}
		bs, err := proto.Marshal(req)
		if err != nil {
			panic(err)
		}
		buf.WriteByte(5)
		buf.Write(bs)
		stream.Write(buf.Bytes())
	}()
}

// packet 6
func (rf *raft) sendInstallSnapshotReply(to int64, req *protobuf.InstallSnapshotReply) {
	peer := rf.peersIP[to]
	go func() {
		conn, err := quic.DialAddr(context.Background(), peer.String(), nil, nil)
		if err != nil {
			return
		}
		defer conn.CloseWithError(0, "close")

		stream, err := conn.OpenStream()
		if err != nil {
			return
		}
		defer stream.Close()

		buf := bytes.Buffer{}
		bs, err := proto.Marshal(req)
		if err != nil {
			panic(err)
		}
		buf.WriteByte(6)
		buf.Write(bs)
		stream.Write(buf.Bytes())
	}()
}
