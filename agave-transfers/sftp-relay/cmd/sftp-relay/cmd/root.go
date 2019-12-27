package cmd

import (
	"github.com/agaveplatform/science-apis/agave-transfers/sftp-relay/pkg/sftpproto"
	api "github.com/agaveplatform/science-apis/agave-transfers/sftp-relay/pkg/sftprelay"
	"google.golang.org/grpc"
	"google.golang.org/grpc/channelz/service"
	"google.golang.org/grpc/keepalive"
	"io"
	"log"
	"net"
	"os"
	"time"
)

func Execute() {

	// log to console and file
	f, err := os.OpenFile("sftprelay.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	wrt := io.MultiWriter(os.Stdout, f)
	log.SetOutput(wrt)

	port := "0.0.0.0:50051"
	// set up the protobuf server
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
		os.Exit(1)
	}
	log.Println("Server was initialized")

	log.Println("Initializing grpc server")
	s := grpc.NewServer(
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle: 20 * time.Minute,
			Time:              (time.Duration(10) * time.Second),
			Timeout:           (time.Duration(10) * time.Second),
		}))
	sftpproto.RegisterSftpRelayServer(s, &api.Server{})
	log.Printf("Server has been initialized.  Now listening on port ", port)
	err = s.Serve(lis)
	if err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}

	// s is a *grpc.Server
	service.RegisterChannelzServiceToServer(s)
}
