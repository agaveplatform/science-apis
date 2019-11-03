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

	fmt.Println("getting realy client")
	c := sftppb.NewSftpRelayClient(conn)
	// push file test
	fmt.Println("doing the put request")
	doUnaryPut(c)
	fmt.Println("done with put now doing the get request")
	//pull the test file
	doUnaryPull(c)
	fmt.Println("done with everything.")
	//p

}

func doUnaryPut(c sftppb.SftpRelayClient) {
	fmt.Println("Starting to do uniary rpc client: ")
	startPulltime := time.Now()
	fmt.Println("Start Pull Time = " + startPulltime.String())

	req := &sftppb.SrvPutRequest{
		SrceSftp: &sftppb.Sftp{
			Username:   "ertanner",
			PassWord:   "Bambie69",
			SystemId:   "192.168.1.16",
			HostKey:    "",
			HostPort:   ":2225",
			ClientKey:  "",
			FileName:   "/tmp/hosts",
			FileSize:   0,
			BufferSize: 0,
			Type:       "SFTP",
		},
		DestSftp: &sftppb.Sftp{
			Username:   "ertanner",
			PassWord:   "Bambie69",
			SystemId:   "192.168.1.16",
			HostKey:    "",
			FileName:   "/tmp/hosts_et",
			FileSize:   0,
			HostPort:   ":2225",
			ClientKey:  "",
			BufferSize: 0,
			Type:       "LOCAL",
		},
	}
	fmt.Println("Username = " + req.SrceSftp.Username)

	res, err := c.Put(context.Background(), req)
	if err != nil {
		log.Fatalf("error while calling RPC: %v", err)
	}
	secs := time.Since(startPulltime).Seconds()
	log.Println("Response from SFTP: " + res.Result)
	fmt.Println("Function Time: " + strconv.FormatFloat(secs, 'f', -1, 64))

}

func doUnaryPull(c sftppb.SftpRelayClient) {
	fmt.Println("Starting 2nd do uniary rpc client: ")
	startPulltime := time.Now()
	req := &sftppb.SrvGetRequest{
		SrceSftp: &sftppb.Sftp{
			Username:   "testuser",
			PassWord:   "testuser",
			SystemId:   "192.168.1.16",
			HostKey:    "",
			HostPort:   ":2225",
			ClientKey:  "",
			FileName:   "/tmp/hosts",
			FileSize:   0,
			BufferSize: 0,
			Type:       "LOCAL",
		},
		DestSftp: &sftppb.Sftp{
			Username:   "testuser",
			PassWord:   "testuser",
			SystemId:   "192.168.1.16",
			HostKey:    "",
			FileName:   "/tmp/hosts_et",
			FileSize:   0,
			HostPort:   ":2225",
			ClientKey:  "",
			BufferSize: 0,
			Type:       "SFTP",
		},
	}
	res, err := c.Get(context.Background(), req)
	if err != nil {
		log.Fatalf("error while calling RPC: %v", err)
	}
	secs := time.Since(startPulltime).Seconds()
	log.Println("Response from SFTP: " + res.Result)
	fmt.Println("Function Time: " + strconv.FormatFloat(secs, 'f', -1, 64))

}
