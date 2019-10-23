package connectionpool

import (
	"github.com/sirupsen/logrus"
	"golang.org/x/crypto/ssh"
	"io"
	"os"
)

var log = logrus.New()

type Server struct{}

type UriParams struct {
	username  string
	passWord  string
	systemId  string
	hostKey   string
	hostPort  string
	clientKey string
	fileName  string
	fileSize  int64
	conn      *ssh.Client
}

var UriSlice = make([]*UriParams, 10)

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

func ConnectionPool(Username string, PassWord string, SystemId string, HostKey string, HostPort string, ClientKey string, FileName string, FileSize int64) (*ssh.Client, error) {

	concat := Username + PassWord + SystemId + HostKey + HostPort + ClientKey
	var i int

	for i = 0; i < len(UriSlice); i++ {
		if concat == UriSlice[i].username+UriSlice[i].passWord+UriSlice[i].systemId+UriSlice[i].hostKey+UriSlice[i].hostPort+UriSlice[i].clientKey {
			return UriSlice[i].conn, nil
		}
	}
	// get host public key
	//HostKey := getHostKey(systemId)
	config := ssh.ClientConfig{
		User: Username,
		Auth: []ssh.AuthMethod{
			ssh.Password(PassWord),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		//HostKeyCallback: ssh.FixedHostKey(HostKey),
	}

	// connect
	conn, err := ssh.Dial("tcp", SystemId+HostPort, &config)
	if err != nil {
		log.Info("Error Dialing the server: %f", err)
		log.Error(err)
		return nil, err
	}
	defer conn.Close()

	UriSlice = append(UriSlice, &UriParams{
		Username,
		PassWord,
		SystemId,
		HostKey,
		HostPort,
		ClientKey,
		FileName,
		FileSize,
		conn})

	return conn, nil
}
