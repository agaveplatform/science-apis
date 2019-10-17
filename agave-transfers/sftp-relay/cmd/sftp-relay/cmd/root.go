package cmd

import (
	"github.com/agaveplatform/science-apis/agave-transfers/sftp-relay/pkg/sftpproto"
	"github.com/agaveplatform/science-apis/agave-transfers/sftp-relay/pkg/sftprelay"
	"google.golang.org/grpc"
	"log"
	"net"
)

func Execute() {
	log.Println("Server was initialized")

	// set up the protobuf server
	lis, err := net.Listen("tcp", "0.0.0.0:50051")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	s := grpc.NewServer()
	sftpproto.RegisterSFTPServer(s, sftprelay.Server{})

	err = s.Serve(lis)
	if err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
