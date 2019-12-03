package connectionpool

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/crypto/ssh"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
)

var log = logrus.New()

type Server struct{}

type UriParams struct {
	username  string
	passWord  string
	systemId  string // IP address or host name
	hostKey   string // private
	hostPort  string // typically port 22
	clientKey string // public
	fileName  string
	fileSize  int64
	createdAt int64
	expiresAt int64
	client    *ssh.Client
}

var sshConnectionMap = make(map[string]UriParams)

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

func getConnectionHashFromValues(Username string, PassWord string, SystemId string, HostKey string, HostPort string) string {
	hash := md5.Sum([]byte(Username + PassWord + SystemId + HostKey + HostPort))
	return hex.EncodeToString(hash[:])
}

func ConnectionPool(Username string, PassWord string, SystemId string, HostKey string, HostPort string, ClientKey string, FileName string, FileSize int64) (*ssh.Client, error) {
	log.Info("got into the connection pool")
	var connectionHash string
	connectionHash = getConnectionHashFromValues(Username, PassWord, SystemId, HostKey, HostPort)

	log.Infof("Connection pool key = %v", connectionHash)

	//var i int
	var config ssh.ClientConfig
	var hostKey ssh.PublicKey

	// check to see if the connection already exists.  If it does then return the connection.
	log.Infof("Connection pool length = %d", len(sshConnectionMap))
	existingConnection, ok := sshConnectionMap[connectionHash]

	if ok {
		// should we check and refresh connection here before returning?
		existingConnection.expiresAt = time.Now().Add(time.Hour).Unix() // remove after an hour of inactivity

		// update pool
		sshConnectionMap[connectionHash] = existingConnection

		// return connection
		return existingConnection.client, nil
	}

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
	log.Infof("validate port # %v", HostPort)
	validatedPort, err := validatePort(HostPort)
	if err != nil {
		return nil, fmt.Errorf("Wrong port number.  You entered %v", err)
	}
	log.Infof("you entered port # %v", validatedPort)

	// *****************************************************
	// connect to the system and establish a "connection"
	// *****************************************************

	log.Info("Dialing the connection now")
	client, err := ssh.Dial("tcp", SystemId+":"+validatedPort, &config)

	if err != nil {
		log.Infof("Error Dialing the server: %f", err)
		log.Error(err)
		return nil, err
	}
	defer client.Close()

	log.Info("Created a new entry for the connection pool and add it now")
	//rightNow := time.Now().Unix()
	//sshConnectionMap[connectionHash] = UriParams{
	//	Username,
	//	PassWord,
	//	SystemId,
	//	HostKey,
	//	HostPort,
	//	ClientKey,
	//	FileName,
	//	FileSize,
	//	rightNow,
	//	rightNow + 3600,
	//	client }
	//
	//// add the connection to the keepalive loop. we only add it once here. it will purge on its own
	//go keepAlive(connectionHash, make(chan struct{}) )
	//
	//return sshConnectionMap[connectionHash].client, nil
	return client, nil
}

func validatePort(s string) (string, error) {
	// empty string defaults to port 22, ports 1-65535 valid, otherwise error
	sPort := strings.TrimLeft(s, ":")
	if sPort == "" {
		// it is the caller's responsibility to pass in a valid, non-empty port
		return "22", nil
	}
	iPort, _ := strconv.Atoi(sPort)
	if iPort < 1 || iPort > 65535 {
		return "", fmt.Errorf("Invalid port: %v", iPort)
	}
	return sPort, nil
}

//func keepAlive(cl *ssh.Client, conn net.Conn, done <-chan struct{}) error {
// this starts a timer to keep the connection fresh by sending a request every minute to the host.
// This will continue for 1 hour, at which time the
func keepAlive(connectionHash string, done <-chan struct{}) error {
	const keepAliveInterval = time.Minute
	t := time.NewTicker(keepAliveInterval)
	defer t.Stop()
	for {
		var uriParams UriParams
		var ok bool
		uriParams, ok = sshConnectionMap[connectionHash]
		if !ok {
			log.Warnf("Connection %v is no longer present in the pool.", connectionHash)
			return nil
		}
		// we really need access to the underlying client to keep the channel
		// open as well as the client
		//deadline := time.Now().Add(keepAliveInterval).Add(15 * time.Second)
		//err := conn.SetDeadline(deadline)
		//if err != nil {
		//	return errors.Wrap(err, "failed to set deadline")
		//}
		select {
		case <-t.C:
			log.Warnf("Sending keepalive for pooled connection %v.", connectionHash)
			_, _, err := uriParams.client.SendRequest("keepalive@agaveplatform.org", true, nil)
			if err != nil {
				t.Stop()
				return errors.Wrap(err, "Failed to send keep alive to connection %v"+connectionHash)
			}
		case <-done:
			t.Stop()
			return nil
		}
	}
}
