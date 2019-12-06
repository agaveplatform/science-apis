package main

import (
	"context"
	sftppb "github.com/agaveplatform/science-apis/agave-transfers/sftp-relay/pkg/sftpproto"
	//"github.com/pkg/sftp"
	"github.com/sirupsen/logrus"
	//"golang.org/x/crypto/ssh"
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

	AgentSocket := "/var/folders/14/jjtrwj5x4zl2tp72ncljn6n40000gn/T//ssh-1t1VnKoFb1xv/agent.28756" //, ok := os.LookupEnv("SSH_AUTH_SOCK")
	log.Infof("AgentSocket = ", AgentSocket)
	//if !ok {
	//	log.Fatalln("Could not connect to SSH_AUTH_SOCK. Is ssh-agent running?")
	//}

	conn, err := grpc.Dial("0.0.0.0:50051", grpc.WithInsecure())
	if err != nil {
		log.Errorf("Could not connect: %v", err)
		log.Exit(1)
	}
	log.Println("Connected to server.")
	defer conn.Close()

	//log.Printf("conn = %s", conn)
	sftpRelay := sftppb.NewSftpRelayClient(conn)
	//log.Printf("Conn = %s", sftpRelay)
	log.Println("Starting Get grpc client: ")

	startPushtime := time.Now()
	log.Printf("Start Time = %v \n", startPushtime)

	for i := 0; i < 1; i++ {
		time.Sleep(1000)
		req := &sftppb.SrvPutRequest{
			SrceSftp: &sftppb.Sftp{
				Username:   "testuser",
				PassWord:   "testuser",
				SystemId:   "0.0.0.0",
				HostKey:    "",
				FileName:   "/tmp/1MB.txt",
				FileSize:   0,
				HostPort:   ":10022",
				ClientKey:  "",
				BufferSize: 8192,
				//BufferSize:   16384,
				//BufferSize:	  32768,
				//BufferSize:   65536,
				Type:         "SFTP",
				DestFileName: "/tmp/1MB_" + strconv.Itoa(i) + ".txt",
			},
		}
		log.Infof("got past req := ", req)

		res, err := sftpRelay.Put(context.Background(), req)
		if err != nil {
			log.Errorf("Error while calling RPC push: %s", err)
		}
		if res != nil {
			log.Errorf("Response from Error Code: %v \n", res.Error)
			log.Printf("Response from FileName: %v \n", res.FileName)
			log.Printf("Response from BytesReturned: %d \n", res.BytesReturned)
		} else {
			log.Printf("Response from FileName: %v \n", res.FileName)
			log.Printf("Response from BytesReturned: %d \n", res.BytesReturned)
		}
		log.Println("End Time %s", time.Since(startPushtime).Seconds())
	}

	secs := time.Since(startPushtime).Seconds()
	log.Println("gRPC Push Time: " + strconv.FormatFloat(secs, 'f', -1, 64))
	time.Sleep(1000)
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
