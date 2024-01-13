package uraft

import (
	"bytes"
	"net"

	"github.com/markity/uraft/internal/pb/protobuf"

	"google.golang.org/protobuf/proto"
)

// packet 1
func (rf *raft) sendVoteRequest(to int64, req *protobuf.VoteRequest) {
	peer := rf.peersIP[to]
	go func() {
		conn, err := net.Dial("tcp", peer.String())
		if err != nil {
			return
		}
		defer conn.Close()

		buf := bytes.Buffer{}
		bs, err := proto.Marshal(req)
		if err != nil {
			panic(err)
		}
		buf.WriteByte(1)
		buf.Write(bs)
		conn.Write(buf.Bytes())
	}()
}

// packet 2
func (rf *raft) sendVoteReply(to int64, req *protobuf.VoteReply) {
	peer := rf.peersIP[to]
	go func() {
		conn, err := net.Dial("tcp", peer.String())
		if err != nil {
			return
		}
		defer conn.Close()

		buf := bytes.Buffer{}
		bs, err := proto.Marshal(req)
		if err != nil {
			panic(err)
		}
		buf.WriteByte(2)
		buf.Write(bs)
		conn.Write(buf.Bytes())
	}()
}

// packet 3
func (rf *raft) sendAppendEntriesRequest(to int64, req *protobuf.AppendEntriesRequest) {
	peer := rf.peersIP[to]
	go func() {
		conn, err := net.Dial("tcp", peer.String())
		if err != nil {
			return
		}
		defer conn.Close()

		buf := bytes.Buffer{}
		bs, err := proto.Marshal(req)
		if err != nil {
			panic(err)
		}
		buf.WriteByte(3)
		buf.Write(bs)
		conn.Write(buf.Bytes())
	}()
}

// packet 4
func (rf *raft) sendAppendEntriesReply(to int64, req *protobuf.AppendEntriesReply) {
	peer := rf.peersIP[to]
	go func() {
		conn, err := net.Dial("tcp", peer.String())
		if err != nil {
			return
		}
		defer conn.Close()

		buf := bytes.Buffer{}
		bs, err := proto.Marshal(req)
		if err != nil {
			panic(err)
		}
		buf.WriteByte(4)
		buf.Write(bs)
		conn.Write(buf.Bytes())
	}()
}

// packet 5
func (rf *raft) sendInstallSnapshotRequest(to int64, req *protobuf.InstallSnapshotRequest) {
	peer := rf.peersIP[to]
	go func() {
		conn, err := net.Dial("tcp", peer.String())
		if err != nil {
			return
		}
		defer conn.Close()

		buf := bytes.Buffer{}
		bs, err := proto.Marshal(req)
		if err != nil {
			panic(err)
		}
		buf.WriteByte(5)
		buf.Write(bs)
		conn.Write(buf.Bytes())
	}()
}

// packet 6
func (rf *raft) sendInstallSnapshotReply(to int64, req *protobuf.InstallSnapshotReply) {
	peer := rf.peersIP[to]
	go func() {
		conn, err := net.Dial("tcp", peer.String())
		if err != nil {
			return
		}
		defer conn.Close()

		buf := bytes.Buffer{}
		bs, err := proto.Marshal(req)
		if err != nil {
			panic(err)
		}
		buf.WriteByte(6)
		buf.Write(bs)
		conn.Write(buf.Bytes())
	}()
}
