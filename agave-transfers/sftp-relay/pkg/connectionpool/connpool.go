package connectionpool

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"golang.org/x/crypto/ssh"
	"io"
	"os"
	"strconv"
	"sync"
	"time"
)

var log = logrus.New()

type Server struct{}

// PoolConfig defines configuration options of the pool.
type PoolConfig struct {
	// GCInterval specifies the frequency of Garbage Collector.
	GCInterval time.Duration

	// MaxConns is a maximum number of connections. GC will remove
	// the oldest connection from the pool if this limit is exceeded.
	MaxConns int
}

type UriParams struct {
	username  string
	passWord  string
	systemId  string // IP address or host name
	hostKey   string // private
	hostPort  string // typically port 22
	clientKey string // public
	fileName  string
	fileSize  int64
	conn      *ssh.Client
}

var UriSlice = make([]UriParams, 1)

func init() {
	// log to console and file
	f, err := os.OpenFile("connectionpool.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	wrt := io.MultiWriter(os.Stdout, f)

	log.SetOutput(wrt)
	log.Info("Set up logging for connection pool")
}

func ConnectionPool(Username string, PassWord string, SystemId string, HostKey string, HostPort string, ClientKey string, FileName string, FileSize int64) (*ssh.Client, error) {
	log.Info("got into the connection pool")
	concat := Username + PassWord + SystemId + HostKey + HostPort
	log.Info(concat)
	//var i int
	var config ssh.ClientConfig
	var hostKey ssh.PublicKey
	log.Info(len(UriSlice))
	log.Info(concat)
	log.Info("check to see if the connection already exists")
	//check to see if the connection already exists.  If it does then return the connection.
	//if len(UriSlice) > 0 {
	//	log.Info("length is greater than 0")
	//	for i = 0; i < len(UriSlice); i++ {
	//		log.Info("iteration %v", i)
	//		if concat == UriSlice[i].username + UriSlice[i].passWord + UriSlice[i].systemId + UriSlice[i].hostKey + UriSlice[i].hostPort {
	//			return UriSlice[i].conn, nil
	//		}
	//	}
	//}

	log.Infof("Check username %v and clientkey %v is not blank", Username, ClientKey)
	if Username != "" && ClientKey != "" {

		// A public key may be used to authenticate against the remote
		// server by using an unencrypted PEM-encoded private key file.
		//
		// If you have an encrypted private key, the crypto/x509 package
		// can be used to decrypt it.

		key := []byte(ClientKey)
		if key == nil {
			log.Fatalf("unable to read private key")
		}
		// Create the Signer for this private key.
		signer, err := ssh.ParsePrivateKey(key)
		if err != nil {
			log.Fatalf("unable to parse private key: %v", err)
		}
		config = ssh.ClientConfig{
			User: Username,
			Auth: []ssh.AuthMethod{
				ssh.PublicKeys(signer),
			},
			HostKeyCallback: ssh.FixedHostKey(hostKey),
		}
		// Checking if username and password are not null.  If so then try that method of connection
	} else if Username != "" && PassWord != "" {
		log.Infof("Check username %v and Password %v is not blank", Username, PassWord)
		config = ssh.ClientConfig{
			User: Username,
			Auth: []ssh.AuthMethod{
				ssh.Password(PassWord),
			},
			HostKeyCallback: ssh.InsecureIgnoreHostKey(),
			//HostKeyCallback: ssh.FixedHostKey(HostKey),
		}
	} else {
		//Return an error with no client connection
		log.Info("Return an error with no client connection")
		pwd := ""
		if PassWord == "" {
			pwd = "null"
		} else {
			pwd = "some password"
		}
		log.Error(Username)
		log.Error(pwd)
		log.Error(ClientKey)
		return nil, fmt.Errorf("Error with connection paramters.  Username is %v and client key is %v  and the pwd is %v", Username, ClientKey, pwd)
	}

	//validate port #
	log.Infof("validate port # %s", HostPort)
	port, err := validatePort(HostPort)
	if err != nil {
		return nil, fmt.Errorf("Wrong port number.  You entered %v", err)
	}
	log.Info("you entered port # %v", port)

	// *****************************************************
	// connect to the system and establish a "connection"
	// *****************************************************

	log.Info("Dial the conenction now")
	conn, err := ssh.Dial("tcp", SystemId+HostPort, &config)
	if err != nil {
		log.Info("Error Dialing the server: %f", err)
		log.Error(err)
		return nil, err
	}
	defer conn.Close()

	log.Info("Put the info into the slice for future use")
	mux := &sync.Mutex{}
	var wg sync.WaitGroup

	for _, myObject := range UriSlice {
		wg.Add(1)
		go func(closureMyObject UriParams) {
			defer wg.Done()
			var tmpObj UriParams
			tmpObj.username = closureMyObject.username
			tmpObj.passWord = closureMyObject.passWord
			tmpObj.systemId = closureMyObject.systemId
			tmpObj.hostKey = closureMyObject.hostKey
			tmpObj.hostPort = closureMyObject.hostPort
			tmpObj.clientKey = closureMyObject.clientKey
			tmpObj.fileName = closureMyObject.fileName
			tmpObj.fileSize = closureMyObject.fileSize
			tmpObj.conn = closureMyObject.conn
			mux.Lock()
			UriSlice = append(UriSlice, tmpObj)
			mux.Unlock()
		}(myObject)
	}
	wg.Wait()

	return conn, nil
}

func validatePort(s string) (string, error) {

	log.Infof("validatePort - Port number = %s", s)

	// strip all non number from any entry
	//processedString := s
	//processedString = strings.ReplaceAll( s, "[\\D]", "" )

	processedString := "10022"
	log.Infof("mod port # %s", processedString)

	// empty string defaults to port 22, ports 1-65535 valid, otherwise error
	if processedString == "" {
		// it is the caller's responsibility to pass in a valid, non-empty port
		return "22", nil
	}
	i, _ := strconv.Atoi(processedString)
	if i < 1 || i > 65535 {
		return "", fmt.Errorf("Invalid port: %v", processedString)
	}
	log.Infof("Port # %s", processedString)
	return processedString, nil
}
