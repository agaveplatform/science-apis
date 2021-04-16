package sftprelay

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	agaveproto "github.com/agaveplatform/science-apis/agave-transfers/sftp-relay/pkg/sftpproto"
	"github.com/golang/protobuf/jsonpb"
	"github.com/minio/highwayhash"
	"github.com/pkg/errors"
	"golang.org/x/crypto/ssh"
	"io"
	"net"
	"os"
	"strings"
	"time"
)

// serializes the remote system config to json for use as an id in the ssh client connection pool
func ProtobufToJSON(systemConfig *agaveproto.RemoteSystemConfig) (string, error) {
	marshaler := jsonpb.Marshaler{
		EnumsAsInts:  false,
		EmitDefaults: true,
		Indent:       "  ",
		OrigName:     true,
	}

	return marshaler.MarshalToString(systemConfig)
}

// deserializes remote system config proto from json for use creating connections in the ssh client connection pool
func ProtobufFromJSON(jsonSystemConfig string) (*agaveproto.RemoteSystemConfig, error) {
	unmarshaler := jsonpb.Unmarshaler{
		AllowUnknownFields: false,
	}
	sreader := strings.NewReader(jsonSystemConfig)
	systemConfig := agaveproto.RemoteSystemConfig{}

	err := unmarshaler.Unmarshal(sreader, &systemConfig)
	if err != nil {
		return nil, errors.Wrap(err, "Unable to parse remote system config from json")
	}
	return &systemConfig, nil
}

// generates a new SSHConfig from the *agaveproto.RemoteSystemConfig included in every server request
func NewSSHConfig(systemConfig *agaveproto.RemoteSystemConfig) (*SSHConfig, error) {

	AgentSocket, ok := os.LookupEnv("SSH_AUTH_SOCK")
	if !ok {
		log.Fatalln("Could not connect to SSH_AUTH_SOCK. Is ssh-agent running?")
	}
	//log.Debugf("SSH_AUTH_SOCK = %v", AgentSocket)

	sshConfig := SSHConfig{
		User:               systemConfig.Username,
		Host:               systemConfig.Host,
		Port:               int(systemConfig.Port),
		Timeout:            60 * time.Second,
		TCPKeepAlive:       true,
		TCPKeepAlivePeriod: 60 * time.Second,
		AgentSocket:        AgentSocket,
		ForwardAgent:       false,
		HostKeyCallback:    ssh.InsecureIgnoreHostKey(),
	}

	// We don't have access to the auth info from within the SSHConfig, so we need to assign a hash
	// to the config to guarantee uniqueness of different auth types to the same host within the pool.
	// Since this has to be recalculated per request, we use a particularly fast hash to encode the
	// credentials and auth type as the salt. This only exists in memory, so exposure is minimal here.
	hh, err := highwayhash.New(sshKeyHash)
	if err != nil {
		// fallback to md5 if highwayhash cannot run on the given host
		log.Errorf("Failed to create HighwayHash instance. Falling back to MD5: %v", err) // add error handling
		hh = md5.New()
	}

	if systemConfig.PrivateKey == "" {
		io.WriteString(hh, "password")
		io.WriteString(hh, systemConfig.Password)

		sshConfig.Auth = []ssh.AuthMethod{ssh.Password(systemConfig.Password)}
	} else {
		var keySigner ssh.Signer
		var err error

		// passwordless keys
		if systemConfig.Password == "" {
			io.WriteString(hh, "publickey")
			io.WriteString(hh, systemConfig.PrivateKey)

			keySigner, err = ssh.ParsePrivateKey([]byte(systemConfig.PrivateKey))
			if err != nil {
				log.Errorf("Unable to parse ssh keys. Authentication will fail: %v", err)
				return nil, err

			}
		} else {
			io.WriteString(hh, "publickeypass")
			io.WriteString(hh, systemConfig.PrivateKey)

			keySigner, err = ssh.ParsePrivateKeyWithPassphrase([]byte(systemConfig.PrivateKey), []byte(systemConfig.Password))
			if err != nil {
				log.Errorf("Unable to parse ssh keys. Authentication will fail: %v", err)
				return nil, err
			}
		}

		sshConfig.Auth = []ssh.AuthMethod{ssh.PublicKeys(keySigner)}
	}

	// create a salt to hash the config. Without this, we can't track authentication to the same host/account/port
	// with different auth mechanisms, which is critical for accounting.
	sshConfig.HashSalt = hex.EncodeToString(hh.Sum(nil))

	return &sshConfig, nil
}

// SSHConn defines the configuration options of the SSH connection.
type SSHConfig struct {
	User     string
	Host     string
	Port     int
	Auth     []ssh.AuthMethod
	HashSalt string

	// Timeout is the maximum amount of time for the TCP connection to establish.
	Timeout time.Duration

	// TCPKeepAlive specifies whether to send TCP keepalive messages
	// to the other side.
	TCPKeepAlive bool
	// TCPKeepAlivePeriod specifies the TCP keepalive frequency.
	TCPKeepAlivePeriod time.Duration

	// AgentSocket is the path to the socket of authentication agent.
	AgentSocket string
	// ForwardAgent specifies whether the connection to the authentication agent
	// (if any) will be forwarded to the remote machine.
	ForwardAgent bool

	HostKeyCallback func(hostname string, remote net.Addr, key ssh.PublicKey) error
}

// BaseURI returns the base portion of the remote host URI up to the path
func (c *SSHConfig) BaseURI() string {
	return fmt.Sprintf(
		"sftp://%s@%s:%d",
		c.User,
		c.Host,
		c.Port)
}

// String returns a hash string generated from the SSH config parameters.
func (c *SSHConfig) String() string {
	return fmt.Sprintf(
		"%s@%s:%d?salt=%s&ForwardAgent=%s",
		c.User,
		c.Host,
		c.Port,
		c.HashSalt,
		fmt.Sprint(c.ForwardAgent),
	)
}

