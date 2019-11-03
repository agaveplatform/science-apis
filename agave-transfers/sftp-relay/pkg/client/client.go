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

const (
	testUsername = "testuser"
	testHost     = "sftp"
	testKey      = ""
	testPassword = "testuser"
	testPort     = "10022"
)

func main() {
	fmt.Println("Starting SFTP client")

	conn, err := grpc.Dial("localhost:50051", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("could not connect: %v", err)
	}
	defer conn.Close()

	c := sftppb.NewSFTPRelayClient(conn)
	// check auth
	fmt.Println("=====================================================")
	doCheckAuth(c)
	fmt.Println("=====================================================")

	fmt.Println("\n")

	// push file test
	fmt.Println("=====================================================")
	doUnaryPut(c)
	fmt.Println("=====================================================")

	fmt.Println("\n")

	//pull the test file
	fmt.Println("=====================================================")
	doUnaryPull(c)
	fmt.Println("=====================================================")
}

func getTestSftpConfig() *sftppb.SftpConfig {
	return  &sftppb.SftpConfig{
		Host:                 testHost,
		Port:                 testPort,
		Username:             testUsername,
		Password:             []byte(testPassword),
		Key:                  []byte(testKey),
	}
}

type Client struct {}

func (c *Client) doCheckAuth(sftpRelay sftppb.SFTPRelayClient) {
	log.Println("Starting Auth rpc client: ")
	startTime := time.Now()

	req := &sftppb.AuthenticateToRemoteRequest{
		SftpConfig: getTestSftpConfig(),
	}

	fmt.Println("Username = " + req.SftpConfig.Username)

	res, err := sftpRelay.AuthenticateToRemoteService(context.Background(), req)
	if err != nil {
		log.Fatalf("error while calling RPC auth: %v", err)
	}
	secs := time.Since(startTime).Seconds()
	log.Println("Response from Auth: " + strconv.FormatBool(res.GetResult()))
	log.Println("RPC Auth Time: " + strconv.FormatFloat(secs, 'f', -1, 64))
}

func (c *Client) doUnaryPut(sftpRelay sftppb.SFTPRelayClient) {
	log.Println("Starting Push rpc client: ")
	startPushtime := time.Now()

	req := &sftppb.SrvPutRequest{
		SftpConfig: getTestSftpConfig(),
		Src: "/etc/hosts",
		Dest: "/tmp/hosts",
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

	res, err := sftpRelay.Put(context.Background(), req)
	if err != nil {
		log.Fatalf("error while calling RPC push: %v", err)
	}
	secs := time.Since(startPushtime).Seconds()
	log.Println("Response from Push: " + res.Result)
	log.Println("RPC Push Time: " + strconv.FormatFloat(secs, 'f', -1, 64))

}

func (c *Client) doUnaryPull(sftpRelay sftppb.SFTPRelayClient) {
	log.Println("Starting Pull rpc client: ")
	startPulltime := time.Now()

	req := &sftppb.SrvGetRequest{
		SftpConfig: getTestSftpConfig(),
		Src: "/etc/hosts",
		Dest: "/tmp/hosts",
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

	res, err := sftpRelay.Get(context.Background(), req)
	if err != nil {
		log.Fatalf("error while calling RPC pull: %v", err)
	}

	secs := time.Since(startPulltime).Seconds()
	log.Println("Response from Pull: " + res.Result)
	log.Println("RPC Pull Time: " + strconv.FormatFloat(secs, 'f', -1, 64))
}
