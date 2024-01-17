package main

import (
	"fmt"
	"time"

	rd "math/rand"

	"github.com/markity/uraft/example/kv/client"
	"github.com/markity/uraft/example/kv/common/rpc"
	"google.golang.org/grpc"
)

func main() {
	grpcConn0, err := grpc.Dial(":8000", grpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	grpcConn1, err := grpc.Dial(":8001", grpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	grpcConn2, err := grpc.Dial(":8002", grpc.WithInsecure())
	if err != nil {
		panic(err)
	}

	server0 := rpc.NewKVRaftClient(grpcConn0)
	server1 := rpc.NewKVRaftClient(grpcConn1)
	server2 := rpc.NewKVRaftClient(grpcConn2)

	randClientID := rd.Int()
	fmt.Println(randClientID)
	clerk := client.MakeClerk([]rpc.KVRaftClient{server0, server1, server2}, int64(randClientID), 1)
	for {
		clerk.Append("hello", "1")
		fmt.Println(clerk.Get("hello"))
		time.Sleep(time.Millisecond * 100)
	}
}
