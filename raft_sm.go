package uraft

import (
	"log"
	"math/rand"
	"time"

	"github.com/markity/uraft/internal/structs"

	"github.com/markity/uraft/internal/pb/protobuf"
)

func (rf *raft) randElectionTimer() {
	t := time.Millisecond*_LEADER_ELECTION_TIMEOUT_LOW +
		time.Millisecond*time.Duration(
			(rand.Int()%(_LEADER_ELECTION_TIMEOUT_HIGH-_LEADER_ELECTION_TIMEOUT_LOW)))
	rf.timer = time.After(t)
}

func (rf *raft) resetLeaderTimer() {
	rf.timer = time.After(time.Millisecond * _HEARTBEAT_INTERVAL)
}

func (rf *raft) timerTimeout() {
	rf.timer = time.After(0)
}

func (rf *raft) commit(leaderCommitIndex int64, newEntry structs.LogEntry) {
	if leaderCommitIndex > rf.state.CommitIndex {
		oldCommitIndex := rf.state.CommitIndex
		rf.state.CommitIndex = min(leaderCommitIndex, newEntry.LogIndex)

		for i := oldCommitIndex + 1; i <= rf.state.CommitIndex; i++ {
			_, ok := rf.state.Logs.FindLogByIndex(i)
			if !ok {
				log.Panicf("oldCommitIndex=%v newCommitIndex=%v i = %v logs=%v", oldCommitIndex, rf.state.CommitIndex, i, rf.state.Logs)
			}
			msg := ApplyMsg{
				CommandValid: true,
				CommandType:  newEntry.CommandType,
				CommandBytes: newEntry.CommandBytes,
				CommandIndex: i,
				CommandTerm:  newEntry.LogTerm,
			}
			rf.applyQueue.Push(msg)
		}
	}
}

func (rf *raft) leaderSendLogs(to int64) {
	if rf.state.State != "leader" {
		panic("checkme")
	}

	// 如果nextIndex的<=lastIncIndex, 需要发整个snapshot
	if rf.state.NextLogIndex[to] <= rf.state.LastIncludedIndex {
		s := rf.persister.GetSnapshot()
		rf.sendInstallSnapshotRequest(to, &protobuf.InstallSnapshotRequest{
			Term:              rf.state.Term,
			LeaderId:          rf.me,
			LastIncludedIndex: rf.state.LastIncludedIndex,
			LastIncludedTerm:  rf.state.LastIncludedTerm,
			Snapshot:          s,
		})
	} else {
		// 此时logs拥有此日志
		lastLog := rf.state.Logs.LastLog()
		preLog, ok := rf.state.Logs.FindLogByIndex(rf.state.NextLogIndex[to] - 1)
		if !ok {
			panic("checkme")
		}
		// 如果最后一个日志的编号>=nextIndex, 此时有日志可发, 需要拼装日志用AE发送
		if lastLog.LogIndex >= rf.state.NextLogIndex[to] {
			tobeSendLogs := make([]*protobuf.CommandEntry, 0)
			for i := rf.state.NextLogIndex[to]; i <= lastLog.LogIndex; i++ {
				l, ok := rf.state.Logs.FindLogByIndex(i)
				if !ok {
					panic("checkme")
				}
				tobeSendLogs = append(tobeSendLogs, &protobuf.CommandEntry{
					CommandType:  l.CommandType,
					CommandBytes: l.CommandBytes,
				})

			}
			rf.sendAppendEntriesRequest(to, &protobuf.AppendEntriesRequest{
				Term:         rf.state.Term,
				LeaderId:     rf.me,
				PreLogIndex:  preLog.LogIndex,
				PreLogTerm:   preLog.LogTerm,
				Entries:      tobeSendLogs,
				LeaderCommit: rf.state.CommitIndex,
			})
		} else {
			// 发心跳
			rf.sendAppendEntriesRequest(to, &protobuf.AppendEntriesRequest{
				Term:         rf.state.Term,
				LeaderId:     rf.me,
				PreLogIndex:  lastLog.LogIndex,
				PreLogTerm:   lastLog.LogTerm,
				Entries:      nil,
				LeaderCommit: rf.state.CommitIndex,
			})
		}
	}
}

func (rf *raft) stateMachine() {
	go func(c chan ApplyMsg) {
		all, err := rf.applyQueue.PopAll()
		if err != nil {
			return
		}
		for _, v := range all {
			msg := v.(ApplyMsg)
			c <- msg
		}
	}(rf.applyCh)

	go runQuicServer(rf.quicListener, rf.messagePipeLine, rf.quicServerCloseChan)

	rf.readPersist(rf.persister.GetRaftState())
	rf.state.State = "follower"
	rf.state.CandidateState = structs.CandidateState{ReceivedNAgrees: 0}
	rf.state.LeaderState = structs.LeaderState{
		NextLogIndex: make([]int64, len(rf.peersIP)+1),
		MatchIndex:   make([]int64, len(rf.peersIP)+1),
	}
	rf.randElectionTimer()
	snapShot := rf.persister.GetSnapshot()
	if len(snapShot) != 0 {
		rf.state.CommitIndex = rf.state.LastIncludedIndex
		rf.applyQueue.Push(ApplyMsg{
			SnapshotValid: true,
			Snapshot:      snapShot,
			SnapshotTerm:  rf.state.LastIncludedTerm,
			SnapshotIndex: rf.state.LastIncludedIndex,
		})
	}

	for {
		select {
		case <-rf.timer:
			switch rf.state.State {
			// 如果是follower超时, 那么进入candidate状态, 并且为自己加一票
			case "follower":
				// 理论上说应该是发送rpc之前刷入state就行了, 但是为了简洁, 目前暂时修改了就立即刷入
				// TODO: 这样效率低下, 可以优化
				rf.state.State = "candidate"
				rf.state.ReceivedNAgrees = 1
				rf.state.PersistInfo.VotedForThisTerm = rf.me
				rf.state.PersistInfo.Term++
				rf.persist(nil)
				rf.randElectionTimer()

				// 并发地发送选票请求
				// 外部会共享这个变量, 为了并发安全我们需要拷贝一份t给协程用
				lastLog := rf.state.Logs.LastLog()
				for i := int64(0); i < int64(len(rf.peersIP)); i++ {
					if i != rf.me {
						rf.sendVoteRequest(i, &protobuf.VoteRequest{
							Term:        rf.state.Term,
							CandidateId: rf.me,
							LastLogIdx:  lastLog.LogIndex,
							LastLogTerm: lastLog.LogTerm,
						})
					}
				}
			// 说明candidate超时了, 加term继续
			case "candidate":
				rf.state.State = "candidate"
				rf.state.ReceivedNAgrees = 1
				rf.state.PersistInfo.Term++
				rf.persist(nil)
				rf.randElectionTimer()

				lastLog := rf.state.Logs.LastLog()
				for i := int64(0); i < int64(len(rf.peersIP)); i++ {
					if i != rf.me {
						rf.sendVoteRequest(i, &protobuf.VoteRequest{
							Term:        rf.state.Term,
							CandidateId: rf.me,
							LastLogIdx:  lastLog.LogIndex,
							LastLogTerm: lastLog.LogTerm,
						})
					}
				}
			// leader超时是定时器超时, 只需要发送心跳维统治即可
			case "leader":
				for i := int64(0); i < int64(len(rf.peersIP)); i++ {
					if i != rf.me {
						rf.leaderSendLogs(i)
					}
				}
				rf.resetLeaderTimer()
			}
		default:
			select {
			// 选举超时timer, 这部分拷贝外面的, 这么写是为了让timer优先得到机会被执行
			case <-rf.timer:
				switch rf.state.State {
				// 如果是follower超时, 那么进入candidate状态, 并且为自己加一票
				case "follower":
					// 理论上说应该是发送rpc之前刷入state就行了, 但是为了简洁, 目前暂时修改了就立即刷入
					// TODO: 这样效率低下, 可以优化
					rf.state.State = "candidate"
					rf.state.ReceivedNAgrees = 1
					rf.state.PersistInfo.VotedForThisTerm = rf.me
					rf.state.PersistInfo.Term++
					rf.persist(nil)
					rf.randElectionTimer()

					// 并发地发送选票请求
					// 外部会共享这个变量, 为了并发安全我们需要拷贝一份t给协程用
					lastLog := rf.state.Logs.LastLog()
					for i := int64(0); i < int64(len(rf.peersIP)); i++ {
						if i != rf.me {
							rf.sendVoteRequest(i, &protobuf.VoteRequest{
								Term:        rf.state.Term,
								CandidateId: rf.me,
								LastLogIdx:  lastLog.LogIndex,
								LastLogTerm: lastLog.LogTerm,
							})
						}
					}
				// 说明candidate超时了, 加term继续
				case "candidate":
					rf.state.State = "candidate"
					rf.state.ReceivedNAgrees = 1
					rf.state.PersistInfo.Term++
					rf.persist(nil)
					rf.randElectionTimer()

					lastLog := rf.state.Logs.LastLog()
					for i := int64(0); i < int64(len(rf.peersIP)); i++ {
						if i != rf.me {
							rf.sendVoteRequest(i, &protobuf.VoteRequest{
								Term:        rf.state.Term,
								CandidateId: rf.me,
								LastLogIdx:  lastLog.LogIndex,
								LastLogTerm: lastLog.LogTerm,
							})
						}
					}
				// leader超时是定时器超时, 只需要发送心跳维统治即可
				case "leader":
					for i := int64(0); i < int64(len(rf.peersIP)); i++ {
						if i != rf.me {
							rf.leaderSendLogs(i)
						}
					}
					rf.resetLeaderTimer()
				}
			case c := <-rf.reqGetState:
				c <- structs.GetStateInfo{
					Term:         rf.me,
					Isleader:     rf.state.State == "leader",
					SnapshotSize: rf.persister.GetSnapshotSize(),
				}
			case info := <-rf.snapshotChan:
				// 收到裁减log的命令, 需要进行日志裁减, 然后把新的snapshot持久化
				// 此时有可能发生自身logs已经被切割了, 但是上层应用不知道的情况, 对应下面的!ok
				l, ok := rf.state.Logs.FindLogByIndex(info.Index)
				if !ok {
					info.SnapshotOKChan <- struct{}{}
					break
				}
				rf.state.Logs.TrimLogs(info.Index)
				rf.state.LastIncludedIndex = l.LogIndex
				rf.state.LastIncludedTerm = l.LogTerm
				rf.persist(info.SnapShot)

				info.SnapshotOKChan <- struct{}{}
			case c := <-rf.reqDead:
				rf.quicListener.Close()
				rf.conn.Close()
				rf.applyQueue.Close()
				c <- struct{}{}
				return
			// start command的收口
			case command := <-rf.sendCmdChan:
				switch rf.state.State {
				case "follower", "candidate":
					command.Resp <- structs.SendCmdRespInfo{
						Term:     rf.state.Term,
						Index:    -1,
						IsLeader: false,
					}
				case "leader":
					// 首先追加日志
					rf.state.Logs.Append(structs.LogEntry{
						LogTerm:      rf.state.PersistInfo.Term,
						LogIndex:     int64(len(rf.state.PersistInfo.Logs)),
						CommandType:  command.CommandType,
						CommandBytes: command.CommandBytes,
					})
					rf.persist(nil)
					l := rf.state.Logs.LastLog().LogIndex
					command.Resp <- structs.SendCmdRespInfo{
						Term:     rf.state.Term,
						Index:    l,
						IsLeader: true,
					}
					// 为了尽快同步日志并返回客户端, 需要让定时器尽快过期
					rf.timerTimeout()
				}
			// messagePipeline, 外部rpc信息的统一收口
			case input := <-rf.messagePipeLine:
				/*
					if one server’s current
					term is smaller than the other’s, then it updates its current
					term to the larger value. If a candidate or leader discovers
					that its term is out of date, it immediately reverts to follower state.
					 If a server receives a request with a stale term
					number, it rejects the request.
				*/
				if rf.state.PersistInfo.Term < input.Term {
					rf.state.State = "follower"
					rf.state.PersistInfo.Term = input.Term
					rf.state.PersistInfo.VotedForThisTerm = -1
					rf.persist(nil)

					/*
						If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)这个是不需要reset election timeout的
						注意修改currentTerm、votedFor、log[]其中一个后都要调用persist()方法，我之前就是因为第一个reply term那个点修改了currentTerm但是忘记调用了
						注意处理过期的RPC回复，student guide里面有写

						Make sure you reset your election timer exactly when Figure 2
						says you should. Specifically, you should only restart your election
						timer if a) you get an AppendEntries RPC from the current leader
						(i.e., if the term in the AppendEntries arguments is outdated, you
						should not reset your timer); b) you are starting an election; or c)
						you grant a vote to another peer.
					*/
					// timer = time.After(RandElectionTime())
				}

				switch val := input.Msg.(type) {
				case *protobuf.InstallSnapshotRequest:
					// 自己的term >= 对方了, 如果对面的term比自己小, 那么直接发信息让对方回到follower
					if val.Term < rf.state.Term {
						rf.sendInstallSnapshotReply(val.LeaderId, &protobuf.InstallSnapshotReply{
							ReqTerm: val.Term,
							Term:    rf.state.Term,
						})
						break
					}

					// 当前term相等, 是不可能产生两个leader的
					if rf.state.State == "leader" {
						panic("checkme")
					}

					if rf.state.State == "candidate" {
						rf.state.State = "follower"
						rf.state.PersistInfo.Term = input.Term
						rf.state.PersistInfo.VotedForThisTerm = rf.me
						rf.persist(nil)
					}

					rf.randElectionTimer()

					/*
						If existing log entry has same index and term as snapshot’s
						last included entry, retain log entries following it and reply
					*/
					// 如果已经有这些日志了, 比如installSnapshotRPC滞留delay的情况, 直接告知ok即可
					hasEntry, ok := rf.state.Logs.FindLogByIndex(val.LastIncludedIndex)
					if (ok && hasEntry.LogTerm == val.Term) ||
						rf.state.CommitIndex >= val.LastIncludedIndex {
						rf.sendInstallSnapshotReply(val.LeaderId, &protobuf.InstallSnapshotReply{
							ReqTerm:              val.Term,
							ReqLastIncludedIndex: val.LastIncludedIndex,
							ReqLastIncludedTerm:  val.LastIncludedTerm,
							Id:                   rf.me,
							Term:                 rf.state.Term,
						})
						break
					}

					// discard all logs
					var newLogs structs.Logs
					newLogs = append(newLogs, structs.LogEntry{
						LogTerm:  val.LastIncludedTerm,
						LogIndex: val.LastIncludedIndex,
						// it is for preLog, Command is useless
						CommandType:  "",
						CommandBytes: nil,
					})
					rf.state.Logs = newLogs
					rf.state.LastIncludedIndex = val.LastIncludedIndex
					rf.state.LastIncludedTerm = val.LastIncludedTerm
					rf.state.CommitIndex = val.LastIncludedIndex
					rf.persist(val.Snapshot)
					rf.applyQueue.Push(ApplyMsg{
						CommandValid:  false,
						SnapshotValid: true,
						Snapshot:      val.Snapshot,
						SnapshotTerm:  val.LastIncludedTerm,
						SnapshotIndex: val.LastIncludedIndex,
					})
					rf.sendInstallSnapshotReply(val.LeaderId, &protobuf.InstallSnapshotReply{
						ReqTerm:              val.Term,
						ReqLastIncludedIndex: val.LastIncludedIndex,
						ReqLastIncludedTerm:  val.LastIncludedTerm,
						Id:                   rf.me,
						Term:                 rf.state.Term,
					})
				case *protobuf.InstallSnapshotReply:
					if rf.state.State != "leader" {
						break
					}

					// 滞后的消息, 忽略
					if val.ReqTerm != rf.state.Term {
						break
					}

					// 来的消息是其它朝代的, 也许是自己之前当过leader, 然后reply滞后了
					//		这种情况不用管, 之后的心跳会同步它的
					if val.Term < rf.state.Term {
						break
					}

					// 收到了合法的reply, 检查它是否需要发送其它的日志
					nextLogMaybeSet := val.ReqLastIncludedIndex + 1
					rf.state.NextLogIndex[val.Id] = max(rf.state.NextLogIndex[val.Id], nextLogMaybeSet)
					rf.state.MatchIndex[val.Id] = rf.state.NextLogIndex[val.Id] - 1

					if rf.state.NextLogIndex[val.Id] != rf.state.Logs.LastLog().LogIndex+1 {
						rf.leaderSendLogs(val.Id)
					}

					sortedMatchIndex := make([]int64, len(rf.state.MatchIndex))
					copy(sortedMatchIndex, rf.state.MatchIndex)
					sortedMatchIndex[rf.me] = int64(len(rf.state.PersistInfo.Logs)) - 1
					int64Sorts(sortedMatchIndex)
					N := sortedMatchIndex[len(rf.peersIP)/2]
					if N > rf.state.CommitIndex && rf.state.PersistInfo.Logs.
						GetByIndex(N).LogTerm == rf.state.PersistInfo.Term {

						oldCommitIndex := rf.state.CommitIndex
						rf.state.CommitIndex = N
						flag := false
						for i := oldCommitIndex + 1; i <= rf.state.CommitIndex; i++ {
							flag = true
							msg := ApplyMsg{
								CommandValid: true,
								CommandType:  rf.state.Logs[i].CommandType,
								CommandBytes: rf.state.Logs[i].CommandBytes,
								CommandIndex: i,
								CommandTerm:  rf.state.PersistInfo.Logs[i].LogTerm,
							}
							rf.applyQueue.Push(msg)
						}
						if !flag {
							panic("checkme")
						}
					}
				case *protobuf.VoteReply:
					// 如果是>当前term, 那么马上转变成follower, 更新term
					/*原文, 就算是VoteReply, 如果发现自己的term落后了, 也需要立马回到follower, 更新term
					All Servers
					If RPC request or response contains term T > currentTerm:
					set currentTerm = T, convert to follower (§5.1)
					*/

					if val.ReqTerm != rf.state.Term {
						break
					}

					// 如果是过去的消息, 直接无视
					if val.Term != rf.state.PersistInfo.Term {
						break
					}

					/*
						有两种情况:
						1. candidate竞争失败, 会退到当前term的follower
						2. 成为leader
					*/
					if rf.state.State != "candidate" {
						break
					}

					if val.VoteGranted {
						rf.state.ReceivedNAgrees++
						if rf.state.ReceivedNAgrees > len(rf.peersIP)/2 {
							rf.state.State = "leader"
							/*
								上任后, 认为每个节点都同步到了最新的日志
								for each server, index of the next log entry
								to send to that server (initialized to leader
								last log index + 1)
							*/
							nextIndex := rf.state.Logs.LastLogIndex() + 1
							for i := 0; i < len(rf.peersIP); i++ {
								rf.state.NextLogIndex[i] = nextIndex
								rf.state.MatchIndex[i] = 0
							}

							// 简便方法, 直接超时
							rf.timerTimeout()
						}
					}
				case *protobuf.VoteRequest:
					// 自己的term >= 对方的term, 作为candidate, 拒绝
					switch rf.state.State {
					case "candidate":
						// 自己的term >= val.Term, 那么直接拒绝这个, 给自己的term
						rf.sendVoteReply(val.CandidateId, &protobuf.VoteReply{
							ReqTerm:     rf.state.Term,
							VoteGranted: false,
							Term:        rf.state.Term,
						})
					// 但是作为follower, 自己的term>=val.Term, 如果自己的term > 对方的term, 那么拒绝
					case "follower":
						/*
							If a server receives a request with a stale term
							number, it rejects the request.
						*/
						if rf.state.Term > val.Term {
							rf.sendVoteReply(val.CandidateId, &protobuf.VoteReply{
								ReqTerm:     val.Term,
								Term:        rf.state.Term,
								VoteGranted: false,
							})
						} else {
							// 这种情况下对面的term==自己的term, 如果自己没投票过, 那么就agree
							if rf.state.Term != val.Term {
								log.Panic("checkme")
							}

							if rf.state.PersistInfo.VotedForThisTerm == -1 {
								// 此外, 还需要管的是对方的日志比自己新
								lastLog := rf.state.Logs.LastLog()
								if val.LastLogTerm < lastLog.LogTerm || (val.LastLogTerm == lastLog.LogTerm &&
									val.LastLogIdx < lastLog.LogIndex) {
									rf.sendVoteReply(val.CandidateId, &protobuf.VoteReply{
										Term:        rf.state.Term,
										VoteGranted: false,
										ReqTerm:     val.Term,
									})
								} else {
									rf.state.PersistInfo.VotedForThisTerm = val.CandidateId
									rf.persist(nil)
									rf.sendVoteReply(val.CandidateId, &protobuf.VoteReply{
										Term:        rf.state.Term,
										VoteGranted: true,
										ReqTerm:     val.Term,
									})

									// you should only restart your election timer if a)
									// you get an AppendEntries RPC from the current leader
									//  (i.e., if the term in the AppendEntries arguments is outdated,
									// you should not reset your timer); b) you are starting an election; or c)
									//  you grant a vote to another peer.
									rf.randElectionTimer()
								}
							} else {
								rf.sendVoteReply(val.CandidateId, &protobuf.VoteReply{
									Term:        rf.state.Term,
									VoteGranted: false,
									ReqTerm:     val.Term,
								})
							}
						}
					case "leader":
						// 否则, 拒绝, 自己的term大于登于对方的term, 不能接受提议
						rf.sendVoteReply(val.CandidateId, &protobuf.VoteReply{
							Term:        rf.state.Term,
							VoteGranted: false,
							ReqTerm:     val.Term,
						})
					}

				case *protobuf.AppendEntriesRequest:
					originPreLogIdx := val.PreLogIndex
					// 进入这个case的时候self term >= remote term
					entries := make([]structs.LogEntry, 0)
					for _, v := range val.Entries {
						entries = append(entries, structs.LogEntry{
							LogTerm:      v.CommandTerm,
							LogIndex:     v.CommandIndex,
							CommandType:  v.CommandType,
							CommandBytes: v.CommandBytes,
						})
					}

					// 如果对方的term小于自己, 那么久直接拒绝日志, 对方收到term后会立刻回退到follower, 此时不用更新timer
					if rf.state.Term > val.Term {
						// 经测试, 必须包含ReqTerm才能保证正确性
						rf.sendAppendEntriesReply(val.LeaderId, &protobuf.AppendEntriesReply{
							Term:    rf.state.Term,
							Success: false,
							ReqTerm: val.Term,
						})
						break
					}

					// 下面的逻辑是对面的term==自己的term了
					if rf.state.State == "leader" {
						panic("checkme")
					}

					if rf.state.State == "candidate" {
						rf.state.State = "follower"
						rf.state.PersistInfo.Term = input.Term
						rf.state.PersistInfo.VotedForThisTerm = rf.me
						rf.persist(nil)
					}

					// 重置timer
					rf.randElectionTimer()

					// 比CommitIndex都还小, 已经确定拥有全数日志, Success给True
					if val.PreLogIndex+int64(len(entries)) <= rf.state.CommitIndex {
						rf.sendAppendEntriesReply(val.LeaderId, &protobuf.AppendEntriesReply{
							Id:             rf.me,
							ReqTerm:        val.Term,
							PreIndex:       val.PreLogIndex,
							Success:        true,
							NLogsInRequest: int64(len(entries)),
							Term:           rf.state.Term,
						})
						break
					}

					// 压根没有这条日志, ConflictIndex给-1, 要求leader回退
					if rf.state.Logs.LastLog().LogIndex < val.PreLogIndex {
						rf.sendAppendEntriesReply(val.LeaderId, &protobuf.AppendEntriesReply{
							Id:             rf.me,
							Term:           rf.state.Term,
							PreIndex:       val.PreLogIndex,
							Success:        false,
							NLogsInRequest: int64(len(val.Entries)),
							ConflictIndex:  -1,
							ReqTerm:        val.Term,
						})
						break
					}

					preLog, ok := rf.state.Logs.FindLogByIndex(val.PreLogIndex)
					if !ok {
						preLogNew := entries[rf.state.LastIncludedIndex-val.PreLogIndex-1]
						entries = entries[rf.state.LastIncludedIndex-val.PreLogIndex:]
						val.PreLogIndex = preLogNew.LogIndex
						val.PreLogTerm = preLogNew.LogTerm
						preLog, ok = rf.state.Logs.FindLogByIndex(val.PreLogIndex)
						if !ok {
							panic("checkme")
						}
					}

					// 如果已经拥有preLog对应得index, 那么需要检查是否应该丢弃log
					// preLog匹配不上, 则删除preLog及其之后的所有, 要求leader回溯
					if preLog.LogTerm != val.PreLogTerm {
						conflictTerm := rf.state.Logs.GetByIndex(val.PreLogIndex).LogTerm
						i := val.PreLogIndex - rf.state.Logs[0].LogIndex
						for ; i > 0; i-- {
							if rf.state.Logs[i].LogTerm != conflictTerm {
								break
							}
						}
						rf.state.Logs.TruncateBy(val.PreLogIndex)
						rf.persist(nil)
						rf.sendAppendEntriesReply(val.LeaderId, &protobuf.AppendEntriesReply{
							Id:             rf.me,
							Term:           rf.state.Term,
							PreIndex:       val.PreLogIndex,
							Success:        false,
							NLogsInRequest: int64(len(val.Entries)),
							ConflictIndex:  i + 1,
							ReqTerm:        val.Term,
						})
						break
					}

					// preLog能匹配了, 如果Entries没有, 那么必然成功, 此时同步preLog那里
					if len(entries) == 0 {
						rf.commit(val.LeaderCommit, preLog)
					} else {
						// 此时entries是有很多日志的, 需要进行追加
						for _, entry := range entries {
							if checkSelfLog, ok := rf.state.Logs.FindLogByIndex(entry.LogIndex); ok {
								if checkSelfLog.LogTerm != entry.LogTerm {
									rf.state.Logs.TruncateBy(entry.LogIndex)
									rf.state.Logs.Append(entry)
								}
							} else {
								rf.state.Logs.Append(entry)
							}
						}
						rf.persist(nil)
						rf.commit(val.LeaderCommit, entries[len(entries)-1])
					}

					rf.sendAppendEntriesReply(val.LeaderId, &protobuf.AppendEntriesReply{
						Id:             rf.me,
						Term:           rf.state.Term,
						PreIndex:       originPreLogIdx,
						Success:        true,
						NLogsInRequest: int64(len(val.Entries)),
						ReqTerm:        val.Term,
					})
				case *protobuf.AppendEntriesReply:
					// 此时自己的term>=对方的term
					// 只有leader理这个信息
					if rf.state.State != "leader" {
						break
					}

					if val.ReqTerm != rf.state.Term {
						break
					}

					// 来的消息是其它朝代的, 也许是自己之前当过leader, 然会reply滞后了
					//		这种情况不用管, 之后的心跳会同步它的
					if val.Term < rf.state.Term {
						break
					}

					// 如果已经在当前日志找不到前一个日志了, 就应当发送installSnapshot了, 之后心跳会自动同步的
					preEntry, ok := rf.state.Logs.FindLogByIndex(rf.state.NextLogIndex[val.Id] - 1)
					if !ok {
						// TODO: 这里可以加入leaderSendLogs加速日志同步
						break
					}

					// 如果找到了前一个日志, 但是index不相等, 说明过时了, 直接忽略就行了
					if val.PreIndex != preEntry.LogIndex {
						break
					}

					if !val.Success {
						if val.ConflictIndex == -1 {
							l, ok := rf.state.Logs.FindLogByIndex(val.PreIndex)
							if !ok {
								panic("checkme")
							}
							// 5 6 7 8
							j := l.LogIndex - rf.state.Logs[0].LogIndex
							for j >= 0 && rf.state.Logs.At(j).LogTerm == l.LogTerm {
								j--
							}
							rf.state.NextLogIndex[val.Id] = min(rf.state.NextLogIndex[val.Id], rf.state.Logs.At(j+1).LogIndex)
						} else {
							rf.state.NextLogIndex[val.Id] = min(val.ConflictIndex, rf.state.NextLogIndex[val.Id])
							if rf.state.NextLogIndex[val.Id] == 0 {
								panic("checkme")
							}
						}
						rf.leaderSendLogs(val.Id)
						// success, 那么加nextIndex, 加matchIndex
					} else {
						nextLogMaybeSet := val.PreIndex + val.NLogsInRequest + 1
						rf.state.NextLogIndex[val.Id] = max(rf.state.NextLogIndex[val.Id], nextLogMaybeSet)
						rf.state.MatchIndex[val.Id] = rf.state.NextLogIndex[val.Id] - 1

						if rf.state.NextLogIndex[val.Id] != rf.state.Logs.LastLog().LogIndex+1 {
							rf.leaderSendLogs(val.Id)
						}

						// If there exists an N such that N > commitIndex,
						//  a majority of matchIndex[i] ≥ N, and log[N].
						// term == currentTerm: set commitIndex = N
						sortedMatchIndex := make([]int64, len(rf.state.MatchIndex))
						copy(sortedMatchIndex, rf.state.MatchIndex)
						sortedMatchIndex[rf.me] = rf.state.Logs.LastLogIndex()
						int64Sorts(sortedMatchIndex)
						N := sortedMatchIndex[len(rf.peersIP)/2]
						if N > rf.state.CommitIndex && rf.state.PersistInfo.Logs.
							GetByIndex(N).LogTerm == rf.state.PersistInfo.Term {

							oldCommitIndex := rf.state.CommitIndex
							rf.state.CommitIndex = N
							flag := false
							for i := oldCommitIndex + 1; i <= rf.state.CommitIndex; i++ {
								flag = true
								l, ok := rf.state.Logs.FindLogByIndex(i)
								if !ok {
									panic("checkme")
								}
								msg := ApplyMsg{
									CommandValid: true,
									CommandType:  l.CommandType,
									CommandBytes: l.CommandBytes,
									CommandIndex: i,
									CommandTerm:  l.LogTerm,
								}
								rf.applyQueue.Push(msg)
							}
							if !flag {
								panic("checkme")
							}
						}
					}
				}
			}
		}
	}
}
