package main

import (
	"context"
	"fmt"
	sftppb "github.com/agaveplatform/science-apis/agave-transfers/sftp-relay/pkg/sftpproto"
	"google.golang.org/grpc"
	"strconv"
	"time"
)

const (
	testUsername = "testuser"
	testHost     = "sftp"
	testKey      = ""
	testPassword = "testuser"
	testPort     = "10022"
)

func main() {
	fmt.Println("Starting SFTP client")

	conn, err := grpc.Dial("localhost:50052", grpc.WithInsecure())
	if err != nil {
		fmt.Errorf("could not connect: %v", err)
	}
	fmt.Println("Connected to server.")
	defer conn.Close()

	fmt.Printf("conn = %s", conn)

	c := sftppb.NewSftpRelayClient(conn)
	fmt.Printf("c = %s", c)

	// check auth
	//fmt.Println("=====================================================")
	//doCheckAuth(c)
	//fmt.Println("=====================================================")

	fmt.Println("\n")

	// push file test
	fmt.Println("=====================================================")
	doUnaryPut(c)
	fmt.Println("=====================================================")

	fmt.Println("\n")

	//pull the test file
	fmt.Println("=====================================================")
	doUnaryGet(c)
	fmt.Println("=====================================================")
}

func getTestSftpConfig() *sftppb.Sftp {
	return &sftppb.Sftp{
		SystemId: testHost,
		HostPort: testPort,
		Username: testUsername,
		PassWord: testPassword,
		HostKey:  testKey,
	}
}

type Client struct{}

//func (c *Client) doCheckAuth(sftpRelay sftppb.SftpRelayClient) {
//	fmt.Println("Starting Auth rpc client: ")
//	startTime := time.Now()
//
//	req := &sftppb.AuthenticateToRemoteRequest{
//		SftpConfig: getTestSftpConfig(),
//	}
//
//	fmt.Println("Username = " + req.SftpConfig.Username)
//
//	res, err := sftpRelay.AuthenticateToRemoteService(context.Background(), req)
//	if err != nil {
//		fmt.Fatalf("error while calling RPC auth: %v", err)
//	}
//	secs := time.Since(startTime).Seconds()
//	fmt.Println("Response from Auth: " + strconv.FormatBool(res.GetResult()))
//	fmt.Println("RPC Auth Time: " + strconv.FormatFloat(secs, 'f', -1, 64))
//}

func doUnaryPut(sftpRelay sftppb.SftpRelayClient) {
	fmt.Println("Starting Push rpc client: ")
	startPushtime := time.Now()
	fmt.Printf("Start Time = %v", startPushtime)

	req := &sftppb.SrvPutRequest{
		SrceSftp: &sftppb.Sftp{
			Username:   "testuser",
			PassWord:   "testuser",
			SystemId:   "192.168.1.16",
			HostKey:    "",
			FileName:   "/tmp/test2.txt",
			FileSize:   16,
			HostPort:   ":10022",
			ClientKey:  "",
			BufferSize: 16,
			Type:       "SFTP",
		},
	}
	fmt.Println("got past req :=")
	res, err := sftpRelay.Put(context.Background(), req)
	if err != nil {
		fmt.Errorf("error while calling RPC push: %v", err)
	}
	fmt.Println("End Time %s", time.Since(startPushtime).Seconds())
	secs := time.Since(startPushtime).Seconds()
	//fmt.Printf( "%v", res )
	fmt.Printf("Response from FileName: %v", res.FileName)
	fmt.Printf("Response from BytesReturned: %d", res.BytesReturned)
	fmt.Printf("Response from Error Code: %v", res.Error)
	fmt.Println("RPC Push Time: " + strconv.FormatFloat(secs, 'f', -1, 64))
}

func doUnaryGet(sftpRelay sftppb.SftpRelayClient) {
	fmt.Println("Starting Pull rpc client: ")
	startPulltime := time.Now()

	req := &sftppb.SrvGetRequest{
		SrceSftp: &sftppb.Sftp{
			Username:   "testuser",
			PassWord:   "testuser",
			SystemId:   "192.168.1.16",
			HostKey:    "",
			HostPort:   ":10022",
			ClientKey:  "",
			FileName:   "/tmp/test2.txt_1",
			FileSize:   0,
			BufferSize: 0,
			Type:       "SFTP",
		},
	}
	fmt.Println("Sftp requested:  %s", req.SrceSftp.Username)

	res, err := sftpRelay.Get(context.Background(), req)
	if err != nil {
		fmt.Errorf("error while calling RPC pull: %v", err)
	}

	secs := time.Since(startPulltime).Seconds()
	fmt.Printf("Response from FileName: %v", res.FileName)
	fmt.Printf("Response from BytesReturned: %d", res.BytesReturned)
	fmt.Printf("Response from Error Code: %v", res.Error)
	fmt.Println("RPC Pull Time: " + strconv.FormatFloat(secs, 'f', -1, 64))
}
