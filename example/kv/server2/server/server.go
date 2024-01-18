package server

import (
	"bytes"
	"context"
	"fmt"
	"net/netip"
	"sync"

	"github.com/markity/uraft"
	"github.com/markity/uraft/example/kv/common"
	"github.com/markity/uraft/example/kv/common/labgob"
	"github.com/markity/uraft/example/kv/common/rpc"
	"google.golang.org/protobuf/proto"
)

type KVServer struct {
	me      int64
	rf      uraft.RaftIface
	applyCh chan uraft.ApplyMsg

	deadReq chan struct{}
	deadOk  chan struct{}

	maxraftstate int64 // snapshot if log grows this big

	// Your definitions here.
	mu sync.Mutex
	// clientID -> reqID的映射, 防止client重复发送
	// 当op == nil时代表已经start进去了, 但是还没被apply, 需要等在channel上
	duplicatedReqTable map[int64]reqCache
	notification       map[int64][]notifyStruct
	lastAppliedIndex   int64
	data               map[string]string
}

type notifyStruct struct {
	term   int64
	reqid  int64
	notify chan notify
}

type notify struct {
	NoLeader bool

	// useful for get notify
	Exists bool
	Value  string
}

type reqCache struct {
	Reqid     int64
	CacheData interface{}
}

type getCache struct {
	Exists bool
	Value  string
}

func (kv *KVServer) Get(ctx context.Context, args *rpc.GetArgs) (*rpc.GetReply, error) {
	reply := new(rpc.GetReply)
	kv.mu.Lock()
	bs, err := proto.Marshal(args)
	if err != nil {
		panic(err)
	}
	_, term, leader := kv.rf.StartCommand("get", bs)
	if !leader {
		kv.mu.Unlock()
		reply.Err = common.ErrWrongLeader
		return reply, nil
	}

	notifyC := make(chan notify, 1)
	kv.notification[args.ClientId] = append(kv.notification[args.ClientId], notifyStruct{
		term:   term,
		notify: notifyC,
		reqid:  args.ReqId,
	})
	kv.mu.Unlock()

	n := <-notifyC
	if n.NoLeader {
		reply.Err = common.ErrWrongLeader
		return reply, nil
	}
	if !n.Exists {
		reply.Err = common.ErrNoKey
		return reply, nil
	}
	reply.Err = common.ErrOK
	reply.Value = n.Value
	return reply, nil
}

func (kv *KVServer) PutAppend(ctx context.Context, args *rpc.PutAppendArgs) (*rpc.PutAppendReply, error) {
	reply := new(rpc.PutAppendReply)
	kv.mu.Lock()
	fmt.Println("received")

	// 4 1
	bs, err := proto.Marshal(args)
	if err != nil {
		panic(err)
	}
	_, term, leader := kv.rf.StartCommand("putappend", bs)
	if !leader {
		kv.mu.Unlock()
		reply.Err = common.ErrWrongLeader
		return reply, nil
	}

	notifyC := make(chan notify, 1)
	kv.notification[args.ClientId] = append(kv.notification[args.ClientId], notifyStruct{
		term:   term,
		notify: notifyC,
		reqid:  args.ReqId,
	})
	kv.mu.Unlock()

	n := <-notifyC
	if n.NoLeader {
		reply.Err = common.ErrWrongLeader
		return reply, nil
	}
	reply.Err = common.ErrOK
	return reply, nil
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	kv.deadReq <- struct{}{}
	<-kv.deadOk
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []netip.AddrPort, me int64, maxraftstate int64) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(&getCache{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.rf = uraft.NewRaft(uraft.RaftConfig{
		Peers:   servers,
		Me:      me,
		LogPath: "./log",
	})
	kv.applyCh = kv.rf.Start()
	kv.notification = make(map[int64][]notifyStruct)
	kv.duplicatedReqTable = make(map[int64]reqCache)
	kv.data = make(map[string]string)
	kv.deadReq = make(chan struct{})
	kv.deadOk = make(chan struct{})

	go poller(kv)

	return kv
}

func poller(kv *KVServer) {
	for {
		select {
		case <-kv.deadReq:
			kv.mu.Lock()
			kv.rf.Stop()
			kv.mu.Unlock()
			kv.deadOk <- struct{}{}
			return
		case apply := <-kv.applyCh:

			// 为了效率, 一次性读出所有apply进行批量处理
			applies := make([]uraft.ApplyMsg, 0, 1)
			applies = append(applies, apply)
			flag := false
			for {
				select {
				case app := <-kv.applyCh:
					applies = append(applies, app)
				default:
					flag = true
				}

				if flag {
					break
				}
			}

			kv.mu.Lock()

			for _, apply := range applies {
				if apply.CommandValid {
					var getCommand rpc.GetArgs
					var putappendCommand rpc.PutAppendArgs
					var cliID, reqID int64
					if apply.CommandType == "get" {
						proto.Unmarshal(apply.CommandBytes, &getCommand)
						cliID = getCommand.ClientId
						reqID = getCommand.ReqId
					} else if apply.CommandType == "putappend" {
						proto.Unmarshal(apply.CommandBytes, &putappendCommand)
						cliID = putappendCommand.ClientId
						reqID = putappendCommand.ReqId
					}

					// 保证幂等, 不重复执行命令, 并缓存此次执行的返回值以及lastReqid, 用duplicatedReqTable存
					if reqID > kv.duplicatedReqTable[cliID].Reqid {
						switch apply.CommandType {
						case "putappend":
							if putappendCommand.Op == "Append" {
								kv.data[putappendCommand.Key] = kv.data[putappendCommand.Key] + putappendCommand.Value
								kv.duplicatedReqTable[cliID] = reqCache{
									Reqid:     reqID,
									CacheData: nil,
								}
							} else if putappendCommand.Op == "Put" {
								kv.data[putappendCommand.Key] = putappendCommand.Value
								kv.duplicatedReqTable[cliID] = reqCache{
									Reqid:     reqID,
									CacheData: nil,
								}
							} else {
								panic("unexpected")
							}
						case "get":
							// DO NOTHING
							value, ok := kv.data[getCommand.Key]
							kv.duplicatedReqTable[cliID] = reqCache{
								Reqid: reqID,
								CacheData: getCache{
									Exists: ok,
									Value:  value,
								},
							}
						}
					}

					// 做通知
					newNotify := make([]notifyStruct, 0)
					switch apply.CommandType {
					case "putappend":
						for _, v := range kv.notification[cliID] {
							// if v.typ != "Put" && v.typ != "Append" {
							// 	continue
							// }
							if apply.CommandTerm != v.term {
								v.notify <- notify{
									NoLeader: true,
								}
							} else if v.reqid == reqID {
								v.notify <- notify{
									NoLeader: false,
								}
							} else {
								newNotify = append(newNotify, v)
							}
						}
						kv.notification[cliID] = newNotify
					case "get":
						for _, v := range kv.notification[cliID] {
							// if v.typ != "Get" {
							// 	continue
							// }
							if apply.CommandTerm != v.term {
								v.notify <- notify{
									NoLeader: true,
								}
							} else if v.reqid == reqID {
								// 改进这里, 需要缓存之前的response, 这样比较优雅, 否则会发生这种情况
								//		Client Get(Reqid = 32) 2
								//		Client Get(Reqid = 32) 8
								// 		duplicated Get is not eq, 很不优雅
								cache := kv.duplicatedReqTable[cliID].CacheData.(getCache)
								v.notify <- notify{
									NoLeader: false,
									Exists:   cache.Exists,
									Value:    cache.Value,
								}
							} else {
								newNotify = append(newNotify, v)
							}
						}
						kv.notification[cliID] = newNotify
					}

					kv.lastAppliedIndex++
				} else if apply.SnapshotValid {
					// 此时应当应用这个snapshot, 然后通知所有等待方no leader
					dec := labgob.NewDecoder(bytes.NewReader(apply.Snapshot))
					err := dec.Decode(&kv.data)
					if err != nil {
						panic("unexpected")
					}
					err = dec.Decode(&kv.duplicatedReqTable)
					if err != nil {
						panic("unexpected")
					}

					// 3
					// 1 2 3 0
					// 0 2 1
					// 一旦接收到snapshot就通知所有的等待方no leader
					for _, v := range kv.notification {
						for _, c := range v {
							c.notify <- notify{
								NoLeader: true,
							}
						}
					}
					kv.notification = make(map[int64][]notifyStruct)
					kv.lastAppliedIndex = int64(apply.SnapshotIndex)
				} else {
					// 收到noop, 通知所有等待方no leader
					for _, v := range kv.notification {
						for _, c := range v {
							c.notify <- notify{
								NoLeader: true,
							}
						}
					}
					kv.notification = make(map[int64][]notifyStruct)
					kv.lastAppliedIndex++
				}
			}

			// 此处检查raft state的size
			if kv.maxraftstate > 0 {
				_, _, sz := kv.rf.GetRaftState()
				fmt.Println(kv.maxraftstate, sz)
				if sz > kv.maxraftstate {
					buf := bytes.Buffer{}
					err := labgob.NewEncoder(&buf).Encode(kv.data)
					if err != nil {
						panic(err)
					}
					err = labgob.NewEncoder(&buf).Encode(kv.duplicatedReqTable)
					if err != nil {
						panic(err)
					}

					kv.rf.Snapshot(buf.Bytes(), kv.lastAppliedIndex)
				}
			}

			kv.mu.Unlock()
		}
	}
}
