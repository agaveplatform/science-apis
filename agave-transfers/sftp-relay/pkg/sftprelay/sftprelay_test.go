package sftprelay

import (
	"context"
	"fmt"
	agaveproto "github.com/agaveplatform/science-apis/agave-transfers/sftp-relay/pkg/sftpproto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
	"net"
	"os"
	"os/exec"
	"testing"
	"time"
)

type params ConnParams

const bufSize = 1024 * 1024

var lis *bufconn.Listener

func init() {
	if _, err := exec.LookPath("git"); err == nil {
		testHasGit = true
	}

	//set up for the servers
	lis = bufconn.Listen(bufSize)
	s := grpc.NewServer()
	agaveproto.RegisterSftpRelayServer(s, &Server{})
	go func() {
		if err := s.Serve(lis); err != nil {
			log.Fatalf("Server exited with error: %v", err)
		}
	}()

}
func bufDialer(string, time.Duration) (net.Conn, error) {
	return lis.Dial()
}

//type ReadTransferFactory interface {
//	ReadFrom(params sftp.ConnParams, name string) sftp.FileTransfer
//}

// Test table
func TestAddr(t *testing.T) {
	cases := map[string]struct{ A, B, expected int }{
		"foo": {1, 1, 2},
		"bar": {1, -1, 0},
	}
	for k, tc := range cases {
		actual := tc.A + tc.B
		if actual != tc.expected {
			t.Errorf(
				"%s: %d + %d = %d, expectd %d", k, tc.A, tc.B, actual, tc.expected)
		}
	}
}

// Helper configuration
type ServerOpts struct {
	CachePath string
	Port      int
}

// helper Chdir
func testChdir(t *testing.T, dir string) func() {
	old, err := os.Getwd()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("err: %s", err)
	}
	return func() {
		os.Chdir(old)
	}
}

//helper  net.Conn
func TestAuthenticate(t *testing.T) {
	ctx := context.Background()
	conn, err := grpc.DialContext(ctx, "bufnet", grpc.WithDialer(bufDialer), grpc.WithInsecure())
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}
	defer conn.Close()
	client := agaveproto.NewSftpRelayClient(conn)

	const (
		username = "testuser"
		host     = "sftp"
		key      = ""
		password = "testuser"
		port     = 10022
	)
	req := &agaveproto.AuthenticateToRemoteRequest{
		Auth: &agaveproto.Sftp{
			Username:   "testuser",
			PassWord:   "testuser",
			SystemId:   "192.168.1.9",
			HostKey:    "",
			FileName:   "",
			FileSize:   0,
			HostPort:   ":22",
			ClientKey:  "",
			BufferSize: 16384,
			Type:       "",
		},
	}

	resp, err := client.Authenticate(ctx, req)
	if err != nil {
		t.Errorf("Error while calling RPC auth: %v", err)
	}
	log.Printf("Response: %+v", resp)
}

//func TestAutheticate(t *testing.T)  {
//	s := Server{}
//
//	req := &agaveproto.AuthenticateToRemoteRequest{
//		Auth: &agaveproto.Sftp{
//			Username:   	"testuser",
//			PassWord:   	"testuser",
//			SystemId:		"192.168.1.9",
//			HostKey:        "",
//			FileName:       "",
//			FileSize:       0,
//			HostPort:       ":10022",
//			ClientKey:      "",
//			BufferSize:     16384,
//			Type:           "",
//		},
//	}
//	res, err := s.Authenticate(context.Background(), req)
//	if err != nil {
//		t.Errorf("Error with authenticate %v", err)
//	}
//	if res.Response == "true" || res.Response == "false" {
//		t.Errorf("Error with response: %v", res.Response)
//	}
//}

func TestThing(t *testing.T) {
	userHome, err := os.UserHomeDir()
	if err != nil {
		t.Errorf("Unable to find user home directory. %s", err)
	}
	defer testChdir(t, userHome)
}

func TestEtcHosts(t *testing.T) {
	defer testChdir(t, "/etc")
}

//Subprocessing
//Real
var testHasGit bool

func TestGitGetter(t *testing.T) {
	if !testHasGit {
		t.Log("git not found, skipping")
		t.Skip()
	}
}

func TestGetFileSize(t *testing.T) {
	fileSize, err := GetFileSize("/bin/ls")
	if fileSize <= 1 {
		t.Error("Error with file size")
	}
	if err != nil {
		t.Errorf("Error with GetFileSize: %v", err)
	}
}
func TestSetGetParams(t *testing.T) {
	const (
		username = "testuser"
		host     = "sftp"
		key      = ""
		password = "testuser"
		port     = 10022
	)
	req := &agaveproto.SrvGetRequest{
		SrceSftp: &agaveproto.Sftp{
			Username:   username,
			PassWord:   password,
			HostKey:    "",
			FileName:   "",
			FileSize:   0,
			HostPort:   fmt.Sprintf("%d", port),
			ClientKey:  key,
			BufferSize: 16384,
			Type:       "",
		},
	}
	connParams := setGetParams(req)
	if connParams.Srce.Username != "testuser" {
		t.Error("Error with setGetParams.  Username != testuser")
	}
}
func TestSetPutParams(t *testing.T) {
	const (
		username = "testuser"
		host     = "sftp"
		key      = ""
		password = "testuser"
		port     = 10022
	)
	req := &agaveproto.SrvPutRequest{
		SrceSftp: &agaveproto.Sftp{
			Username:   username,
			PassWord:   password,
			HostKey:    "",
			FileName:   "",
			FileSize:   0,
			HostPort:   fmt.Sprintf("%d", port),
			ClientKey:  key,
			BufferSize: 16384,
			Type:       "",
		},
	}
	connParams := setPutParams(req)
	if connParams.Srce.Username != "testuser" {
		t.Error("Error with setPutParams.  Username != testuser")
	}
}
func TestSetGetAuthParams(t *testing.T) {
	const (
		username = "testuser"
		host     = "sftp"
		key      = ""
		password = "testuser"
		port     = 10022
	)
	req := &agaveproto.AuthenticateToRemoteRequest{
		Auth: &agaveproto.Sftp{
			Username:   username,
			PassWord:   password,
			HostKey:    "",
			FileName:   "",
			FileSize:   0,
			HostPort:   fmt.Sprintf("%d", port),
			ClientKey:  key,
			BufferSize: 16384,
			Type:       "",
		},
	}
	connParams := setGetAuthParams(req)
	if connParams.Srce.Username != "testuser" {
		t.Error("Error with setGetAuthParams.  Username != testuser")
	}
}
