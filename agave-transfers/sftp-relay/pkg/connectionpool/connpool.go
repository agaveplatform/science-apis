package connectionpool

import (
	"github.com/sirupsen/logrus"
	"golang.org/x/crypto/ssh"
	"io"
	"os"
)

var log = logrus.New()

type Server struct{}

type URI struct {
	Username  string
	PassWord  string
	SystemId  string
	HostKey   string
	HostPort  string
	ClientKey string
	FileName  string
	FileSize  int64
}

var UriParams = make([]*URI, 0)

func init() {
	// log to console and file
	f, err := os.OpenFile("SFTPServer.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	wrt := io.MultiWriter(os.Stdout, f)

	log.SetOutput(wrt)
	log.Info("Set up loggin for server")
}

func connectionpool(uri URI) (*ssh.Client, error) {

	// get host public key
	//HostKey := getHostKey(systemId)
	config := ssh.ClientConfig{
		User: uri.Username,
		Auth: []ssh.AuthMethod{
			ssh.Password(uri.PassWord),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		//HostKeyCallback: ssh.FixedHostKey(HostKey),
	}

	// connect
	conn, err := ssh.Dial("tcp", uri.SystemId+uri.HostPort, &config)
	if err != nil {
		log.Info("Error Dialing the server: %f", err)
		log.Error(err)

	}
	defer conn.Close()
	return conn, nil
}
