package main

import (
	"context"
	"fmt"
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
		fmt.Printf("error opening file %s: %s", f.Name(), err)
	}
	wrt := io.MultiWriter(os.Stdout, f)
	log.SetOutput(wrt)
}

var timeItr int64

func main() {
	fmt.Println("Starting SFTP client")
	timeItr = 2000

	fmt.Println("Get connections")
	conn, sshConn, err := getConn()
	if err != nil {
		fmt.Printf("Error = %s", err.Error())
		panic(err)
	}
	fmt.Println("Got back from the connection")

	startPushtime := time.Now()

	c := make(chan string, 1001)

	for i := 0; i < 2; i++ {
		fmt.Println("Int =", i)
		fmt.Println("put the next file")
		go gRpcFunc(conn, sshConn, i, c)
		fmt.Println("Got back from putting the next file.")
		//fmt.Println( <- c)
		//time.Sleep( time.Duration(timeItr) * (time.Millisecond))
	}
	for k := 0; k < 2; k++ {
		fmt.Println(<-c)
	}

	secs := time.Since(startPushtime).Seconds()
	fmt.Println("gRPC Push Time: " + strconv.FormatFloat(secs, 'f', -1, 64))
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

func getConn() (sftppb.SftpRelayClient, *sftp.Client, error) {
	fmt.Println("Got into the getConn")
	conn, err := grpc.Dial("0.0.0.0:50052", grpc.WithInsecure())
	if err != nil {
		fmt.Errorf("could not connect: %v", err)
	}
	fmt.Println("Connected to server.")
	defer conn.Close()

	//log.Printf("conn = %s", conn)
	sftpRelay := sftppb.NewSftpRelayClient(conn)
	fmt.Printf("c = %s", sftpRelay)
	fmt.Println("Starting Put grpc client: ")

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
		fmt.Printf("Error Dialing the server: %v", err)
	}
	defer conn.Close()

	// create new SFTP client
	client, err := sftp.NewClient(sftpConn, sftp.MaxPacket(8192))
	if err != nil {
		fmt.Errorf("Error creating new client, %v \n", err)
	}
	defer client.Close()

	return sftpRelay, client, err
}

type Client struct{}

func gRpcFunc(sftpRelay sftppb.SftpRelayClient, sftpConn *sftp.Client, i int, ch chan<- string) {
	fmt.Println("Got into the gRpcfunc")
	startPushtime := time.Now()

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
	fmt.Println("got past req := ", req.SrceSftp.DestFileName)

	res, err := sftpRelay.Put(context.Background(), req)
	if err != nil {
		fmt.Errorf("error while calling RPC push: %s", err)
	} else if res.Error != "" {
		fmt.Errorf("Response from Error Code: %v \n", res.Error)
	} else {
		fmt.Printf("Response from FileName: %v \n", res.FileName)
		fmt.Printf("Response from BytesReturned: %d \n", res.BytesReturned)
	}

	fmt.Errorf("Response from Error Code: %v \n", err)
	//sftpConn.Remove("/tmp/1GB_" + strconv.Itoa(i) + ".txt")
	fmt.Printf("Start Time = %v \n", startPushtime)

	ch <- fmt.Sprintf("End Time %s", time.Since(startPushtime).Seconds())
}
