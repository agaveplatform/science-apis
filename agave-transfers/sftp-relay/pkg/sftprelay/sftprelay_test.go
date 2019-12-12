package sftprelay

import (
	"context"
	"fmt"
	agaveproto "github.com/agaveplatform/science-apis/agave-transfers/sftp-relay/pkg/sftpproto"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
	"io/ioutil"
	"net"
	"os"
	"os/exec"
	"strconv"
	"testing"
	"time"
)

type params ConnParams

const bufSize = 1024 * 1024

// net.Listener that creates local, buffered net.Conns over its Accept and Dial method
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

func createTestFile() {
	// put the test.txt file in /tmp/test.txt  Make the file length = 3 "abc"
	bigBuff := make([]byte, 32768)
	err := ioutil.WriteFile("/tmp/test.txt", bigBuff, 0777)
	if err != nil {
		panic(err)
	}

	// Remote without error as this should not be run against localhost
	os.Remove("/tmp/testDest.txt")
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
	createTestFile()
	ctx := context.Background()
	conn, err := grpc.DialContext(ctx, "bufnet", grpc.WithDialer(bufDialer), grpc.WithInsecure())
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}
	defer conn.Close()
	client := agaveproto.NewSftpRelayClient(conn)

	req := &agaveproto.AuthenticateToRemoteRequest{
		Auth: &agaveproto.Sftp{
			Username:     "testuser",
			PassWord:     "testuser",
			SystemId:     "sftp",
			HostKey:      "",
			FileName:     "",
			FileSize:     0,
			HostPort:     ":10022",
			ClientKey:    "",
			BufferSize:   8192,
			Type:         "",
			DestFileName: "",
		},
	}

	resp, err := client.Authenticate(ctx, req)
	assert.Nilf(t, err, "Error while calling RPC Authenticate: %v", err)
	log.Printf("Response: %+v", resp)
}

func TestMkdirs(t *testing.T) {

	ctx := context.Background()
	conn, err := grpc.DialContext(ctx, "bufnet", grpc.WithDialer(bufDialer), grpc.WithInsecure())
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}
	defer conn.Close()
	client := agaveproto.NewSftpRelayClient(conn)

	req := &agaveproto.SrvMkdirsRequest{
		SrceSftp: &agaveproto.Sftp{
			Username:     "testuser",
			PassWord:     "testuser",
			SystemId:     "sftp",
			HostKey:      "",
			HostPort:     ":10022",
			ClientKey:    "",
			BufferSize:   8192,
			Type:         "SFTP",
			DestFileName: "/tmp/deleteme",
		},
	}

	resp, err := client.Mkdirs(ctx, req)
	assert.Nilf(t, err, "Error while calling RPC Mkdirs: %v", err)
	log.Printf("Response: %+v", resp)
}

func TestRemove(t *testing.T) {

	ctx := context.Background()
	conn, err := grpc.DialContext(ctx, "bufnet", grpc.WithDialer(bufDialer), grpc.WithInsecure())
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}
	defer conn.Close()
	client := agaveproto.NewSftpRelayClient(conn)

	req := &agaveproto.SrvRemoveRequest{
		SrceSftp: &agaveproto.Sftp{
			Username:     "testuser",
			PassWord:     "testuser",
			SystemId:     "sftp",
			HostKey:      "",
			HostPort:     ":10022",
			ClientKey:    "",
			BufferSize:   8192,
			Type:         "SFTP",
			DestFileName: "/tmp/deleteme",
		},
	}

	resp, err := client.Remove(ctx, req)
	assert.Nilf(t, err, "Error while calling RPC Remove: %v", err)
	log.Printf("Response: %+v", resp)
}

func TestPut(t *testing.T) {
	createTestFile()
	ctx := context.Background()
	conn, err := grpc.DialContext(ctx, "bufnet", grpc.WithDialer(bufDialer), grpc.WithInsecure())
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}
	defer conn.Close()
	client := agaveproto.NewSftpRelayClient(conn)

	req := &agaveproto.SrvPutRequest{
		SrceSftp: &agaveproto.Sftp{
			Username:     "testuser",
			PassWord:     "testuser",
			SystemId:     "sftp",
			HostKey:      "",
			FileName:     "/tmp/test.txt",
			FileSize:     3,
			HostPort:     ":10022",
			ClientKey:    "",
			BufferSize:   8192,
			Type:         "SFTP",
			DestFileName: "/tmp/testDest.txt",
		},
	}

	resp, err := client.Put(ctx, req)
	assert.Nilf(t, err, "Error while calling RPC Put: %v", err)
	log.Printf("Response: %+v", resp)
}

func TestGet(t *testing.T) {
	createTestFile()
	ctx := context.Background()
	conn, err := grpc.DialContext(ctx, "bufnet", grpc.WithDialer(bufDialer), grpc.WithInsecure())
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}
	defer conn.Close()
	client := agaveproto.NewSftpRelayClient(conn)

	req := &agaveproto.SrvGetRequest{
		SrceSftp: &agaveproto.Sftp{
			Username:     "testuser",
			PassWord:     "testuser",
			SystemId:     "sftp",
			HostKey:      "",
			FileName:     "/tmp/testDest.txt",
			FileSize:     3,
			HostPort:     ":10022",
			ClientKey:    "",
			BufferSize:   8192,
			Type:         "SFTP",
			DestFileName: "/tmp/testDest.txt",
		},
	}

	resp, err := client.Get(ctx, req)
	assert.Nilf(t, err, "Error while calling RPC Get: %v", err)

	destFile, err := os.Stat(req.SrceSftp.DestFileName)
	assert.Nilf(t, err, "Downloaded file should be present at %s. No file found.", req.SrceSftp.DestFileName)

	bytesTransferred, err := strconv.ParseInt(resp.BytesReturned, 0, 64)
	assert.Equalf(t, bytesTransferred, destFile.Size(),
		"Downloaded file does not have same size as bytes returned value %s != %d", resp.BytesReturned, destFile.Size())

	log.Printf("Response: %+v", resp)
}

func TestThing(t *testing.T) {
	userHome, err := os.UserHomeDir()
	assert.Nilf(t, err, "Unable to find user home directory. %s", err)
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
	assert.Nilf(t, err, "Error with GetFileSize: %v", err)
	assert.True(t, fileSize > 1, "File size should be greater than 1")
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
			BufferSize: 8192,
			Type:       "",
		},
	}
	connParams, err := setGetParams(req)

	assert.Nilf(t, err, "Error with setGetParams: %v", err)
	assert.Equal(t, connParams.Srce.Username, "testuser",
		"Error with setGetParams.  Username != testuser")
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
			BufferSize: 8192,
			Type:       "",
		},
	}
	connParams, err := setPutParams(req)
	assert.Nilf(t, err, "Error with setPutParams: %v", err)
	assert.Equal(t, connParams.Srce.Username, "testuser",
		"Error with setPutParams.  Username != testuser")
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
			BufferSize: 8192,
			Type:       "",
		},
	}
	connParams, err := setGetAuthParams(req)
	assert.Nilf(t, err, "Error with setGetParams: %v", err)
	assert.Equal(t, connParams.Srce.Username, "testuser",
		"Error with setGetParams.  Username != testuser")
}
