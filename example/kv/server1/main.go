package main

import (
	"net"
	"net/netip"

	"github.com/markity/uraft/example/kv/common/rpc"
	"github.com/markity/uraft/example/kv/server1/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

const me = 1
const rpcListenAt = "127.0.0.1:8001"

func main() {
	r1, _ := netip.ParseAddrPort("127.0.0.1:5000")
	r2, _ := netip.ParseAddrPort("127.0.0.1:5001")
	r3, _ := netip.ParseAddrPort("127.0.0.1:5002")
	servers := []netip.AddrPort{r1, r2, r3}

	kvserver := server.StartKVServer(servers, me, 6000)
	rpcserver := grpc.NewServer()
	rpc.RegisterKVRaftServer(rpcserver, kvserver)
	reflection.Register(rpcserver)
	lis, err := net.Listen("tcp", rpcListenAt)
	if err != nil {
		panic(err)
	}
	err = rpcserver.Serve(lis)
	if err != nil {
		panic(err)
	}
}
