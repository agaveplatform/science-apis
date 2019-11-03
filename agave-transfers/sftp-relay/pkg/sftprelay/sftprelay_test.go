package sftprelay_test

import (
	sftp "github.com/agaveplatform/science-apis/agave-transfers/sftp-relay/pkg/sftprelay"
	"net"
	"os"
	"os/exec"
	"testing"
)

type params sftp.ConnParams

type ReadTransferFactory interface {
	ReadFrom(params sftp.ConnParams, name string) sftp.FileTransfer
}

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
func GetTestConn(t *testing.T) (client, server net.Conn) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Errorf("Unable to listen to 127.0.0.1:0. %s", err)
	}

	//var server net.Conn
	go func() {
		defer ln.Close()
		server, err = ln.Accept()
	}()

	client, err = net.Dial("tcp", ln.Addr().String())
	return client, server
}

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

func init() {
	if _, err := exec.LookPath("git"); err == nil {
		testHasGit = true
	}
}
func TestGitGetter(t *testing.T) {
	if !testHasGit {
		t.Log("git not found, skipping")
		t.Skip()
	}
}
