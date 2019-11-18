package main

import (
	"context"
	sftppb "github.com/agaveplatform/science-apis/agave-transfers/sftp-relay/pkg/sftpproto"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"io"
	"os"
	"strconv"
	"time"
)

var log = logrus.New()

const (
	testUsername = "testuser"
	testHost     = "sftp"
	testKey      = ""
	testPassword = "testuser"
	testPort     = "10022"
)

func init() {
	// log to console and file
	f, err := os.OpenFile("SFTPC_Client.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	wrt := io.MultiWriter(os.Stdout, f)

	log.SetOutput(wrt)
}

func main() {
	log.Println("Starting SFTP client")

	conn, err := grpc.Dial("localhost:50051", grpc.WithInsecure())
	if err != nil {
		log.Errorf("could not connect: %v", err)
	}
	log.Println("Connected to server.")
	defer conn.Close()

	log.Printf("conn = %s", conn)
	sftpRelay := sftppb.NewSftpRelayClient(conn)
	log.Printf("c = %s", sftpRelay)
	log.Println("Starting Put grpc client: ")
	startPushtime := time.Now()
	log.Printf("Start Time = %v \n", startPushtime)

	req := &sftppb.SrvPutRequest{
		SrceSftp: &sftppb.Sftp{
			Username:     "testuser",
			PassWord:     "testuser",
			SystemId:     "localhost",
			HostKey:      "",
			FileName:     "/tmp/test.txt",
			FileSize:     128,
			HostPort:     ":10022",
			ClientKey:    "",
			BufferSize:   16138,
			Type:         "SFTP",
			DestFileName: "/tmp/test3.txt",
		},
	}

	log.Println("got past req :=")
	res, err := sftpRelay.Put(context.Background(), req)
	if err != nil {
		log.Errorf("error while calling RPC push: %v", err)
	}
	log.Println("End Time %s", time.Since(startPushtime).Seconds())
	secs := time.Since(startPushtime).Seconds()
	//log.Printf( "%v", res )
	if res.Error != "" {
		log.Printf("Response from Error Code: %v \n", res.Error)

	} else {
		log.Printf("Response from FileName: %v \n", res.FileName)
		log.Printf("Response from BytesReturned: %d \n", res.BytesReturned)
	}
	log.Println("gRPC Push Time: " + strconv.FormatFloat(secs, 'f', -1, 64))
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
