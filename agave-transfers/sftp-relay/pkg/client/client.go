package main

import (
	"context"
	sftppb "github.com/agaveplatform/science-apis/agave-transfers/sftp-relay/pkg/sftpproto"
	"github.com/pkg/sftp"
	"github.com/sirupsen/logrus"
	"golang.org/x/crypto/ssh"
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

	conn, err := grpc.Dial("0.0.0.0:50052", grpc.WithInsecure())
	if err != nil {
		log.Errorf("could not connect: %v", err)
	}
	log.Println("Connected to server.")
	defer conn.Close()

	//log.Printf("conn = %s", conn)
	sftpRelay := sftppb.NewSftpRelayClient(conn)
	log.Printf("c = %s", sftpRelay)
	log.Println("Starting Get grpc client: ")

	startPushtime := time.Now()
	log.Printf("Start Time = %v \n", startPushtime)

	//+++++++++++++++++++++++++++++++++++
	// sftp file used to delete a file once it has been up loaded
	var config ssh.ClientConfig
	config = ssh.ClientConfig{
		User: "testuser",
		Auth: []ssh.AuthMethod{
			ssh.Password("testuser"),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}

	sftpConn, err := ssh.Dial("tcp", "0.0.0.0:10022", &config)
	if err != nil {
		log.Printf("Error Dialing the server: %v", err)
		log.Error(err)
	}
	defer conn.Close()

	// create new SFTP client
	client, err := sftp.NewClient(sftpConn, sftp.MaxPacket(8192))
	if err != nil {
		log.Errorf("Error creating new client, %v \n", err)
	}
	defer client.Close()

	//+++++++++++++++++++++++++++++++++++

	for i := 0; i < 2; i++ {

		req := &sftppb.SrvPutRequest{
			SrceSftp: &sftppb.Sftp{
				Username:   "testuser",
				PassWord:   "testuser",
				SystemId:   "0.0.0.0",
				HostKey:    "",
				FileName:   "/tmp/test/1K.txt",
				FileSize:   0,
				HostPort:   ":10022",
				ClientKey:  "",
				BufferSize: 8192,
				//BufferSize:   16384,
				//BufferSize:	  32768,
				//BufferSize:   65536,
				Type:         "SFTP",
				DestFileName: "/tmp/1K_" + strconv.Itoa(i) + ".txt",
			},
		}
		log.Println("got past req :=")

		res, err := sftpRelay.Put(context.Background(), req)
		if err != nil {
			log.Errorf("error while calling RPC push: %s", err)
			log.Fatal()
		}
		log.Println("End Time %s", time.Since(startPushtime).Seconds())

		//log.Printf( "%v", res )
		if res != nil {
			log.Errorf("Response from Error Code: %v \n", res.Error)

		} else {
			log.Printf("Response from FileName: %v \n", res.FileName)
			log.Printf("Response from BytesReturned: %d \n", res.BytesReturned)
		}
		// rm file

		// connect to the sftp server and delete the file
		//client.Remove("/tmp/100MB_" + strconv.Itoa(i) + ".txt")
		os.Remove("/tmp/receive/1K_" + strconv.Itoa(i) + ".txt")
	}

	secs := time.Since(startPushtime).Seconds()
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
