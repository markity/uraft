package client

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/markity/uraft/example/kv/common"
	"github.com/markity/uraft/example/kv/common/rpc"
)

type Clerk struct {
	servers []rpc.KVRaftClient
	// You will have to modify this struct.

	// 我们需要使客户端请求是串行的, 用锁保证
	// It's OK to assume that a client will make
	// only one call into a Clerk at a time.
	mu sync.Mutex

	// 当前leader, 用于快速找leader
	currentLeaderID int
	// 每个客户端需要生成一个唯一的clientID
	clientID int64
	// 单调递增的请求id
	reqID int64

	timeoutSeconds int
}

func (ck *Clerk) changeLeader() {
	ck.currentLeaderID = (ck.currentLeaderID + 1) % len(ck.servers)
}

func MakeClerk(servers []rpc.KVRaftClient, clientUniqueID int64, timeoutSecond int) *Clerk {
	ck := new(Clerk)
	ck.servers = servers

	ck.clientID = clientUniqueID
	ck.reqID = 0
	ck.timeoutSeconds = timeoutSecond

	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	getArgs := rpc.GetArgs{
		Key:      key,
		ClientId: ck.clientID,
		ReqId:    atomic.AddInt64(&ck.reqID, 1),
	}
	for {
		c := make(chan *rpc.GetReply)
		go func() {
			reply, err := ck.servers[ck.currentLeaderID].Get(context.Background(), &getArgs)
			if err != nil {
				reply = &rpc.GetReply{}
				reply.Err = common.ErrNetworkNotOK
			}
			c <- reply
		}()
		select {
		case <-time.After(time.Second * time.Duration(ck.timeoutSeconds)):
			ck.changeLeader()
			time.Sleep(time.Millisecond)
			continue
		case result := <-c:
			switch result.Err {
			case common.ErrOK:
				return result.Value
			case common.ErrNoKey:
				return ""
			case common.ErrWrongLeader, common.ErrNetworkNotOK:
				ck.changeLeader()
				time.Sleep(time.Millisecond)
				continue
			default:
				panic("checkme")
			}
		}
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	putArgs := rpc.PutAppendArgs{
		Key:      key,
		ClientId: ck.clientID,
		ReqId:    atomic.AddInt64(&ck.reqID, 1),
		Value:    value,
		Op:       op,
	}
	for {
		c := make(chan *rpc.PutAppendReply)
		go func() {
			reply, err := ck.servers[ck.currentLeaderID].PutAppend(context.Background(), &putArgs)
			if err != nil {
				reply = new(rpc.PutAppendReply)
				reply.Err = common.ErrNetworkNotOK
			}
			c <- reply
		}()
		select {
		case <-time.After(time.Second * time.Duration(ck.timeoutSeconds)):
			ck.changeLeader()
			time.Sleep(time.Millisecond)
			continue
		case result := <-c:
			switch result.Err {
			case common.ErrOK:
				return
			case common.ErrWrongLeader, common.ErrNetworkNotOK:
				ck.changeLeader()
				time.Sleep(time.Millisecond)
				continue
			default:
				panic("checkme")
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
