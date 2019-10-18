package main

import (
	"context"
	sftppb "github.com/agaveplatform/science-apis/agave-transfers/sftp-relay/pkg/sftpproto"
	"log"

	"fmt"
	"google.golang.org/grpc"
	"strconv"
	"time"
)

func main() {
	fmt.Println("Starting SFTP client")

	conn, err := grpc.Dial("localhost:50051", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("could not connect: %v", err)
	}
	defer conn.Close()

	c := sftppb.NewSFTPRelayClient(conn)
	// push file test
	doUnaryPut(c)
	//pull the test file
	doUnaryPull(c)

	//p

}

func doUnaryPut(c sftppb.SFTPRelayClient) {
	fmt.Println("Starting to do uniary rpc client: ")
	startPulltime := time.Now()
	fmt.Println("Start Pull Time = " + startPulltime.String())

	req := &sftppb.CopyLocalToRemoteRequest{
		Sftp: &sftppb.Sftp{
			Username: "testuser",
			SystemId: "sftp",
			FileName: "/tmp/hosts",
			HostKey:  "",
			PassWord: "testuser",
			HostPort: ":22",
		},
	}
	fmt.Println("Username = " + req.Sftp.Username)

	res, err := c.CopyLocalToRemoteService(context.Background(), req)
	if err != nil {
		log.Fatalf("error while calling RPC: %v", err)
	}
	secs := time.Since(startPulltime).Seconds()
	log.Println("Response from SFTP: " + res.Result)
	fmt.Println("Function Time: " + strconv.FormatFloat(secs, 'f', -1, 64))

}

func doUnaryPull(c sftppb.SFTPRelayClient) {
	fmt.Println("Starting 2nd do uniary rpc client: ")
	startPulltime := time.Now()
	req := &sftppb.CopyFromRemoteRequest{
		Sftp: &sftppb.Sftp{
			Username: "testuser",
			SystemId: "sftp",
			FileName: "/tmp/hosts",
			HostKey:  "",
			PassWord: "testuser",
			HostPort: ":22",
		},
	}
	res, err := c.CopyFromRemoteService(context.Background(), req)
	if err != nil {
		log.Fatalf("error while calling RPC: %v", err)
	}
	secs := time.Since(startPulltime).Seconds()
	log.Println("Response from SFTP: " + res.Result)
	fmt.Println("Function Time: " + strconv.FormatFloat(secs, 'f', -1, 64))

}
