package service

import (
	"context"
	"faydfs/proto"
	"google.golang.org/grpc"
	"log"
	"time"
)

type NameNodeClientEnd struct {
	id    uint64
	addr  string
	conns []*grpc.ClientConn
}

func datanodeReloadReplica(blockName, newIP, processIP string) error {
	conn, client, _, _ := getGrpcN2DConn(processIP)
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	//status, err := (*client).GetDirMeta(ctx, &proto.PathName{PathName: remoteDirPath})
	log.Println("replicate "+blockName+" to ", newIP)
	_, err := (*client).ReloadReplica(ctx, &proto.CopyReplica2DN{BlockName: blockName, NewIP: newIP})
	if err != nil {
		log.Print("datanode ReloadReplica fail: processIP :", err)
		return err
	}
	return nil
}

func (nn *NameNode) getBlockReport2DN() {
	dnList := nn.DB.GetDn()
	for ip, dn := range dnList {
		if dn.Status != datanodeDown {
			blockReplicaList, err := nn.getBlockReportRPC(ip)
			if err != nil {
				log.Println(err)
				return
			}
			for _, bm := range blockReplicaList.BlockReplicaList {
				nn.GetBlockReport(bm)
			}
		}
	}

}

func (nn *NameNode) getBlockReportRPC(addr string) (*proto.BlockReplicaList, error) {
	conn, client, _, _ := getGrpcN2DConn(addr)
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	//status, err := (*client).GetDirMeta(ctx, &proto.PathName{PathName: remoteDirPath})
	blockReplicaList, err := (*client).GetBlockReport(ctx, &proto.Ping{Ping: addr})
	if err != nil {
		log.Print("datanode get BlockReport fail: addr :", addr)
		return nil, err
	}
	return blockReplicaList, nil
}

func getGrpcN2DConn(address string) (*grpc.ClientConn, *proto.N2DClient, *context.CancelFunc, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	//conn, err := grpc.DialContext(ctx, address, grpc.WithBlock())
	conn2, err := grpc.DialContext(ctx, address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("did not connect to %v error %v", address, err)
	}
	client := proto.NewN2DClient(conn2)
	return conn2, &client, &cancel, err
}
