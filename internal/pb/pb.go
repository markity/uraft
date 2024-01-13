package pb

import (
	"github.com/markity/uraft/internal/pb/protobuf"

	"google.golang.org/protobuf/proto"
)

func PackAEReqMessage(req *protobuf.AppendEntriesRequest) []byte {
	bs, err := proto.Marshal(req)
	if err != nil {
		panic(err)
	}
	return bs
}

func PackAEReplyMessage(reply *protobuf.AppendEntriesReply) []byte {
	bs, err := proto.Marshal(reply)
	if err != nil {
		panic(err)
	}
	return bs
}

func PackInsatllSnapshotMessage() {

}

func PackInstallSnapshotReplyMessage() {

}
