package cmd

import (
	"github.com/agaveplatform/science-apis/agave-transfers/sftp-relay/pkg/sftpproto"
	api "github.com/agaveplatform/science-apis/agave-transfers/sftp-relay/pkg/sftprelay"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"io"
	"log"
	"net"
	"os"
	"time"
)

func Execute() {

	// log to console and file
	f, err := os.OpenFile("SFTPServer.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	wrt := io.MultiWriter(os.Stdout, f)
	log.SetOutput(wrt)

	// set up the protobuf server
	lis, err := net.Listen("tcp", "localhost:50051")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	log.Println("Server was initialized")

	log.Println("Initializing grpc server")
	s := grpc.NewServer(
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle: 5 * time.Minute, // <--- This fixes it!
			Time:              (time.Duration(20) * time.Second),
			Timeout:           (time.Duration(10) * time.Second),
		}))
	sftpproto.RegisterSftpRelayServer(s, &api.Server{})
	log.Println("Server has been initialized.  Now listening on port 50051")
	err = s.Serve(lis)
	if err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
