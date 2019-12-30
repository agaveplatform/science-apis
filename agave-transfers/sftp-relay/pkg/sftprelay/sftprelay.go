package sftprelay

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/agaveplatform/science-apis/agave-transfers/sftp-relay/pkg/connectionpool"
	"github.com/minio/highwayhash"
	"math/rand"
	"path/filepath"
	"sort"

	agaveproto "github.com/agaveplatform/science-apis/agave-transfers/sftp-relay/pkg/sftpproto"
	"github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/pkg/sftp"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"golang.org/x/crypto/ssh"
	"golang.org/x/net/context"
	"io"
	"os"
	"time"
)

var log = logrus.New()

const MAX_PACKET_SIZE = 32768

type Server struct{
	Registry prometheus.Registry
	GrpcMetrics grpc_prometheus.ServerMetrics
}

//The Pool and AgentSocket variables are created here and instantiated in the init() function.
var (
	Pool *connectionpool.SSHPool

	// The path to the unix socket on which the ssh-agent is listen
	AgentSocket string

	// key used to hash the ssh configs in the pool
	sshKeyHash []byte

	// Get request counter.
	getCounterMetric = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "sftprelay_get_method_handle_count",
		Help: "Total number of Get RPCs handled on the server.",
	}, []string{"name"})
	// get total bytes transferred
	getTotalBytesTransferredMetric = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "sftprelay_get_total_bytes_transferred",
		Help: "Total bytes transferred on get requests.",
	}, []string{"name"})
	// Put request counter.
	putCounterMetric = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "sftprelay_put_method_handle_count",
		Help: "Total number of Put RPCs handled on the server.",
	}, []string{"name"})
	// put total bytes transferred
	putTotalBytesTransferredMetric = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "sftprelay_put_total_bytes_transferred",
		Help: "Total bytes transferred on put requests",
	}, []string{"name"})
	// Remove request counter.
	removeCounterMetric = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "sftprelay_remove_method_handle_count",
		Help: "Total number of Remove RPCs handled on the server.",
	}, []string{"name"})
	// connectionpool request counter.
	connectionpoolCounterMetric = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "sftprelay_connectionpool_total_size",
		Help: "Total number of configs in the connectin pool.",
	}, []string{"name"})
	// Stat request counter.
	statCounterMetric = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "sftprelay_stat_method_handle_count",
		Help: "Total number of Stat RPCs handled on the server.",
	}, []string{"name"})
	// Mkdirs request counter.
	mkdirsCounterMetric = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "sftprelay_mkdirs_method_handle_count",
		Help: "Total number of Mkdirs RPCs handled on the server.",
	}, []string{"name"})
	// AuthCheck request counter.
	authCounterMetric = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "sftprelay_authcheck_method_handle_count",
		Help: "Total number of AuthCheck RPCs handled on the server.",
	}, []string{"name"})
	// ListCheck request counter.
	listCounterMetric = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "sftprelay_list_method_handle_count",
		Help: "Total number of List RPCs handled on the server.",
	}, []string{"name"})
)


// generates a new SSHConfig from the *agaveproto.RemoteSystemConfig included in every server request
func NewSSHConfig(systemConfig *agaveproto.RemoteSystemConfig) (*connectionpool.SSHConfig, error) {

	AgentSocket, ok := os.LookupEnv("SSH_AUTH_SOCK")
	if !ok {
		log.Fatalln("Could not connect to SSH_AUTH_SOCK. Is ssh-agent running?")
	}
	log.Debugf("SSH_AUTH_SOCK = %v", AgentSocket)

	var sshConfig connectionpool.SSHConfig

	sshConfig = connectionpool.SSHConfig{
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

func NewRemoteFileInfo(path string, fileInfo os.FileInfo) agaveproto.RemoteFileInfo {
	return agaveproto.RemoteFileInfo{
		Name:        fileInfo.Name(),
		Path:        path,
		Size:        fileInfo.Size(),
		Mode:        fileInfo.Mode().String(),
		LastUpdated: fileInfo.ModTime().Unix(),
		IsDirectory: fileInfo.IsDir(),
		IsLink:      !(fileInfo.Mode().IsRegular() || fileInfo.Mode().IsDir()),
	}
}

func init() {
	// Open a log file to log the server
	f, err := os.OpenFile("sftp-relay.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	wrt := io.MultiWriter(os.Stdout, f)
	log.SetOutput(wrt)

	// Now set up the ssh connection pool
	AgentSocket, ok := os.LookupEnv("SSH_AUTH_SOCK")
	if !ok {
		log.Fatalln("Could not connect to SSH_AUTH_SOCK. Is ssh-agent running?")
	}
	log.Debugf("SSH_AUTH_SOCK = %v", AgentSocket)

}

func newConnectionPool() *connectionpool.SSHPool {
	// set the pool configuration.  The MaxConns sets the maximum connections for the pool.
	poolCfg := &connectionpool.PoolConfig{
		GCInterval: 5 * time.Second,
		MaxConns:   500,
	}
	// Create the new pool
	return connectionpool.NewPool(poolCfg)
}

// generates a random n digit byte array
func RandString(n int) string {
	return string(RandBytes(n))
}

func RandBytes(length int) []byte {
	all := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	digits := "0123456789"
	rand.Seed(time.Now().UnixNano())

	buf := make([]byte, length)
	buf[0] = digits[rand.Intn(len(digits))]
	for i := 1; i < length; i++ {
		buf[i] = all[rand.Intn(len(all))]
	}
	rand.Shuffle(len(buf), func(i, j int) {
		buf[i], buf[j] = buf[j], buf[i]
	})
	return buf
}

func (s *Server) InitMetrics() {
	Pool = newConnectionPool()

	// Register standard server metrics and customized metrics to registry.

	s.Registry.MustRegister(&s.GrpcMetrics,
		getCounterMetric,
		getTotalBytesTransferredMetric,
		putCounterMetric, putTotalBytesTransferredMetric,
		removeCounterMetric,
		connectionpoolCounterMetric,
		statCounterMetric,
		mkdirsCounterMetric,
		authCounterMetric)

	// generate a unique hash key to encrypt the ssh configs in the pool
	sshKeyHash = RandBytes(64)
}

// Performs a basic authentication handshake to the remote system
func (*Server) AuthCheck(ctx context.Context, req *agaveproto.AuthenticationCheckRequest) (*agaveproto.EmptyResponse, error) {
	log.Tracef("Invoking Auth service")

	sshCfg, err := NewSSHConfig(req.SystemConfig)
	if err != nil {
		return &agaveproto.EmptyResponse{Error:  err.Error()}, nil
	}

	log.Debugf(sshCfg.String())

	sftpClient, sessionCount, err := Pool.ClaimClient(sshCfg, sftp.MaxPacket(MAX_PACKET_SIZE))
	// verify we have a valid connection to use
	if err != nil {
		log.Errorf("Error obtaining connection to %s:%d. Nil client returned from pool", req.SystemConfig.Host, req.SystemConfig.Port)
		return &agaveproto.EmptyResponse{Error: fmt.Sprintf("nil client returned from pool: %v", err.Error())}, nil
	}
	defer Pool.ReleaseClient(sshCfg)
	defer sftpClient.Close()

	log.Infof("Number of active connections: %d", sessionCount)

	log.Infof(fmt.Sprintf("sftp://%s@%s:%d", req.SystemConfig.Username, req.SystemConfig.Host, req.SystemConfig.Port))

	// increment the request metric
	authCounterMetric.WithLabelValues(req.SystemConfig.Host).Inc()

	// success returns an empty response
	return &agaveproto.EmptyResponse{}, nil
}

// Performs a mkdirs operation on a remote system.
func (*Server) Mkdir(ctx context.Context, req *agaveproto.SrvMkdirRequest) (*agaveproto.FileInfoResponse, error) {
	log.Printf("Invoking Mkdirs service")

	sshCfg, err := NewSSHConfig(req.SystemConfig)
	if err != nil {
		return &agaveproto.FileInfoResponse{Error:  err.Error()}, nil
	}

	log.Debugf(sshCfg.String())

	sftpClient, sessionCount, err := Pool.ClaimClient(sshCfg, sftp.MaxPacket(MAX_PACKET_SIZE))
	// verify we have a valid connection to use
	if err != nil {
		log.Errorf("Error obtaining connection to %s:%d. Nil client returned from pool", req.SystemConfig.Host, req.SystemConfig.Port)
		return &agaveproto.FileInfoResponse{Error: "nil client returned from pool"}, nil
	}
	defer Pool.ReleaseClient(sshCfg)
	defer sftpClient.Close()

	log.Debugf("Number of active connections: %d", sessionCount)

	log.Infof(fmt.Sprintf("sftp://%s@%s:%d/%s", req.SystemConfig.Username, req.SystemConfig.Host, req.SystemConfig.Port, req.RemotePath))

	// increment the request metric
	mkdirsCounterMetric.WithLabelValues(req.SystemConfig.Host).Inc()

	var response agaveproto.FileInfoResponse

	// Read From system
	response = mkdir(sftpClient, req.RemotePath, req.Recursive)

	return &response, nil
}

// Fetches file info for a remote path
func (*Server) Stat(ctx context.Context, req *agaveproto.SrvStatRequest) (*agaveproto.FileInfoResponse, error) {
	log.Printf("Invoking Stat service")

	sshCfg, err := NewSSHConfig(req.SystemConfig)
	if err != nil {
		return &agaveproto.FileInfoResponse{Error:  err.Error()}, nil
	}

	log.Debugf(sshCfg.String())

	sftpClient, sessionCount, err := Pool.ClaimClient(sshCfg, sftp.MaxPacket(MAX_PACKET_SIZE))
	// verify we have a valid connection to use
	if err != nil {
		log.Errorf("Error obtaining connection to %s:%d. Nil client returned from pool", req.SystemConfig.Host, req.SystemConfig.Port)
		return &agaveproto.FileInfoResponse{Error: "nil client returned from pool"}, nil
	}
	defer Pool.ReleaseClient(sshCfg)
	defer sftpClient.Close()

	log.Debugf("Number of active connections: %d", sessionCount)

	log.Infof(fmt.Sprintf("sftp://%s@%s:%d/%s", req.SystemConfig.Username, req.SystemConfig.Host, req.SystemConfig.Port, req.RemotePath))

	// increment the request metric
	statCounterMetric.WithLabelValues(req.SystemConfig.Host).Inc()

	var response agaveproto.FileInfoResponse

	// invoke the internal action
	response = stat(sftpClient, req.RemotePath)

	return &response, nil
}

// Removes the remote path. If the remote path is a directory, the entire tree is deleted.
func (*Server) Remove(ctx context.Context, req *agaveproto.SrvRemoveRequest) (*agaveproto.EmptyResponse, error) {
	log.Printf("Invoking Remove service")

	sshCfg, err := NewSSHConfig(req.SystemConfig)
	if err != nil {
		return &agaveproto.EmptyResponse{Error:  err.Error()}, nil
	}

	log.Debugf(sshCfg.String())

	sftpClient, sessionCount, err := Pool.ClaimClient(sshCfg, sftp.MaxPacket(MAX_PACKET_SIZE))
	// verify we have a valid connection to use
	if err != nil {
		log.Errorf("Error obtaining connection to %s:%d. Nil client returned from pool", req.SystemConfig.Host, req.SystemConfig.Port)
		return &agaveproto.EmptyResponse{Error: "nil client returned from pool"}, nil
	}
	defer Pool.ReleaseClient(sshCfg)
	defer sftpClient.Close()

	log.Debugf("Number of active connections: %d", sessionCount)

	log.Infof(fmt.Sprintf("sftp://%s@%s:%d/%s", req.SystemConfig.Username, req.SystemConfig.Host, req.SystemConfig.Port, req.RemotePath))

	// increment the request metric
	removeCounterMetric.WithLabelValues(req.SystemConfig.Host).Inc()

	var response agaveproto.EmptyResponse

	// invoke the internal action
	response = remove(sftpClient, req.RemotePath)

	return &response, nil
}

// Fetches a file from the remote system and stores it locally
func (*Server) Get(ctx context.Context, req *agaveproto.SrvGetRequest) (*agaveproto.TransferResponse, error) {
	log.Trace("Invoking Get service")

	sshCfg, err := NewSSHConfig(req.SystemConfig)
	if err != nil {
		return &agaveproto.TransferResponse{Error:  err.Error()}, nil
	}

	log.Debugf(sshCfg.String())

	sftpClient, sessionCount, err := Pool.ClaimClient(sshCfg, sftp.MaxPacket(MAX_PACKET_SIZE))
	// verify we have a valid connection to use
	if err != nil {
		log.Errorf("Error obtaining connection to %s:%d. Nil client returned from pool", req.SystemConfig.Host, req.SystemConfig.Port)
		return &agaveproto.TransferResponse{Error: "nil client returned from pool"}, nil
	}
	defer Pool.ReleaseClient(sshCfg)
	defer sftpClient.Close()

	log.Debugf("Number of active connections: %d", sessionCount)

	log.Infof(fmt.Sprintf("sftp://%s@%s:%d/%s => file://localhost/%s", req.SystemConfig.Username, req.SystemConfig.Host, req.SystemConfig.Port, req.RemotePath, req.LocalPath))

	// increment the request metric
	getCounterMetric.WithLabelValues(req.SystemConfig.Host).Inc()

	var response agaveproto.TransferResponse

	// invoke the internal action
	response = get(sftpClient, req.LocalPath, req.RemotePath, req.Force, req.Range)

	if response.BytesTransferred > 0 {
		// increment the request metric
		getTotalBytesTransferredMetric.WithLabelValues(req.SystemConfig.Host).Add(float64(response.BytesTransferred))
	}
	return &response, nil
}

// Transfers a file from the local system to the remote system
func (*Server) Put(ctx context.Context, req *agaveproto.SrvPutRequest) (*agaveproto.TransferResponse, error) {
	log.Trace("Invoking Put service")

	sshCfg, err := NewSSHConfig(req.SystemConfig)
	if err != nil {
		return &agaveproto.TransferResponse{Error:  err.Error()}, nil
	}

	log.Debugf(sshCfg.String())

	sftpClient, sessionCount, err := Pool.ClaimClient(sshCfg, sftp.MaxPacket(MAX_PACKET_SIZE))
	if err != nil {
		log.Errorf("Error with connection = %v", err)
		return &agaveproto.TransferResponse{Error: err.Error()}, nil
	}
	defer Pool.ReleaseClient(sshCfg)
	defer sftpClient.Close()

	log.Debugf("Number of active connections: %d", sessionCount)

	log.Infof(fmt.Sprintf("put file://localhost/%s => sftp://%s@%s:%d/%s", req.LocalPath, req.SystemConfig.Username, req.SystemConfig.Host, req.SystemConfig.Port, req.RemotePath))

	// increment the request metric
	putCounterMetric.WithLabelValues(req.SystemConfig.Host).Inc()

	var response agaveproto.TransferResponse

	// invoke the internal action
	response = put(sftpClient, req.LocalPath, req.RemotePath, req.Force, req.Append)

	if response.BytesTransferred > 0 {
		// increment the request metric
		putTotalBytesTransferredMetric.WithLabelValues(req.SystemConfig.Host).Add(float64(response.BytesTransferred))
	}

	return &response, nil
}

// Transfers a file from the local system to the remote system
func (*Server) List(ctx context.Context, req *agaveproto.SrvListRequest) (*agaveproto.FileInfoListResponse, error) {
	log.Trace("Invoking list service")

	sshCfg, err := NewSSHConfig(req.SystemConfig)
	if err != nil {
		return &agaveproto.FileInfoListResponse{Error:  err.Error()}, nil
	}

	log.Debugf(sshCfg.String())

	sftpClient, sessionCount, err := Pool.ClaimClient(sshCfg, sftp.MaxPacket(MAX_PACKET_SIZE))
	if err != nil {
		log.Errorf("Error with connection = %v", err)
		return &agaveproto.FileInfoListResponse{Error: err.Error()}, nil
	}
	defer Pool.ReleaseClient(sshCfg)
	defer sftpClient.Close()

	log.Debugf("Number of active connections: %d", sessionCount)

	log.Infof(fmt.Sprintf("ls sftp://%s@%s:%d/%s", req.SystemConfig.Username, req.SystemConfig.Host, req.SystemConfig.Port, req.RemotePath))

	// increment the request metric
	listCounterMetric.WithLabelValues(req.SystemConfig.Host).Inc()

	var response agaveproto.FileInfoListResponse

	// invoke the internal action
	response = list(sftpClient, req.RemotePath)

	return &response, nil
}

// Fetches file info for a remote path
func (*Server) Rename(ctx context.Context, req *agaveproto.SrvRenameRequest) (*agaveproto.FileInfoResponse, error) {
	log.Printf("Invoking Rename service")

	sshCfg, err := NewSSHConfig(req.SystemConfig)
	if err != nil {
		return &agaveproto.FileInfoResponse{Error:  err.Error()}, nil
	}

	log.Debugf(sshCfg.String())

	sftpClient, sessionCount, err := Pool.ClaimClient(sshCfg, sftp.MaxPacket(MAX_PACKET_SIZE))
	// verify we have a valid connection to use
	if err != nil {
		log.Errorf("Error obtaining connection to %s:%d. Nil client returned from pool", req.SystemConfig.Host, req.SystemConfig.Port)
		return &agaveproto.FileInfoResponse{Error: "nil client returned from pool"}, nil
	}
	defer Pool.ReleaseClient(sshCfg)
	defer sftpClient.Close()

	log.Debugf("Number of active connections: %d", sessionCount)

	log.Infof(fmt.Sprintf("sftp://%s@%s:%d/%s => sftp://%s@%s:%d/%s",
		req.SystemConfig.Username, req.SystemConfig.Host, req.SystemConfig.Port, req.RemotePath,
		req.SystemConfig.Username, req.SystemConfig.Host, req.SystemConfig.Port, req.NewName))

	// increment the request metric
	statCounterMetric.WithLabelValues(req.SystemConfig.Host).Inc()

	var response agaveproto.FileInfoResponse

	// invoke the internal action
	response = rename(sftpClient, req.RemotePath, req.NewName)

	return &response, nil
}
//*****************************************************************************************

/*
The last sections each implement an SFTP process.
*/

func get(sftpClient *sftp.Client, localFilePath string, remoteFilePath string, force bool, byteRanges string) agaveproto.TransferResponse {
	log.Trace("WriteTo (PUT) sFTP Service function was invoked ")

	// check if the file already exists. If is  does not and Force = false then error. Otherwise remove the
	//file first then do everything else
	var localFileExists bool
	localFileInfo, err := os.Stat(localFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			localFileExists = false
		} else {
			log.Errorf("Error attempting to stat local path %s: %v", remoteFilePath, err)
			return agaveproto.TransferResponse{Error: err.Error()}
		}
	} else {
		localFileExists = true
	}

	// Create the destination file
	if localFileExists && localFileInfo.IsDir() {
		log.Errorf("Error opening, %s. Destination is a directory.", remoteFilePath)
		return agaveproto.TransferResponse{Error: "destination path is a directory"}
	} else if (localFileExists && force) || !localFileExists {
		//if localFileExists {
		//	err := os.Remove(localFilePath)
		//	if err != nil {
		//		log.Errorf("Failed to remove existing local file, %s : %v", localFilePath, err)
		//	}
		//}

		remoteFile, err := sftpClient.Open(remoteFilePath)
		if err != nil {
			log.Errorf("Error opening handle to dest file. %s. %v", remoteFilePath, err)
			return agaveproto.TransferResponse{Error: err.Error()}
		}
		defer remoteFile.Close()

		// verify existence of local path
		remoteFileInfo, err := sftpClient.Stat(remoteFilePath)
		if err != nil {
			log.Errorf("Error verifying remote file %s: %v", remoteFilePath, err)
			return agaveproto.TransferResponse{Error: err.Error()}
		}

		// check that the remote path is not a directory
		if remoteFileInfo.IsDir() {
			err = errors.New(fmt.Sprintf("Error opening, %s. Remote path is a directory.", remoteFilePath))
			return agaveproto.TransferResponse{Error: "source path is a directory"}
		}

		// now that the dest file is open the next step is to open the source file
		log.Debugf("Opening local destination file %s", localFilePath)

		localFile, err := os.OpenFile(localFilePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0755)
		if err != nil {
			log.Errorf("Failed to open source file %s: %v", localFilePath, err)
			return agaveproto.TransferResponse{Error: err.Error()}
		}
		defer localFile.Close()

		var bytesWritten int64

		log.Debugf("Writing to remote file %s", remoteFilePath)
		// this will write the file BufferSize blocks until EOF is returned.
		bytesWritten, err = remoteFile.WriteTo(localFile)
		if err != nil {
			log.Errorf("Error after writing %d byes to file %s: %v", bytesWritten, localFilePath, err)
			return agaveproto.TransferResponse{Error: err.Error()}
		}
		log.Debugf("%d bytes copied to %s", bytesWritten, localFilePath)

		// flush the buffer to ensure everything was written
		err = localFile.Sync()
		if err != nil {
			log.Errorf("Error flushing local file, %s, when reading from %s: %v", localFilePath, remoteFilePath, err)
			return agaveproto.TransferResponse{Error: err.Error()}
		}
		// ensure permissions are retained
		log.Debugf("Setting permissions of dest file %s", localFilePath)
		err = os.Chmod(localFilePath, remoteFileInfo.Mode().Perm())
		if err != nil {
			log.Errorf("Error setting permissions of dest file %s after download: %v", localFilePath, err)
			return agaveproto.TransferResponse{Error: err.Error()}
		}

		// ensure timestamps are retained
		log.Debugf("Setting modification time of dest file %s", localFilePath)
		err = os.Chtimes(localFilePath, time.Now(), remoteFileInfo.ModTime())
		if err != nil {
			log.Errorf("Error setting modifcation time of dest file %s after download: %v", localFilePath, err)
			return agaveproto.TransferResponse{Error: err.Error()}
		}

		log.Debugf("Completed transfer to dest file %s", localFilePath)
		localFileInfo, _ = os.Stat(localFilePath)

		rfi := NewRemoteFileInfo(localFilePath, localFileInfo)
		return agaveproto.TransferResponse{
			RemoteFileInfo: &rfi,
			BytesTransferred: bytesWritten,
		}
	} else {
		return agaveproto.TransferResponse{Error: "file already exists"}
	}
}

func put(sftpClient *sftp.Client, localFilePath string, remoteFilePath string, force bool, append bool) agaveproto.TransferResponse {
	log.Trace("WriteTo (PUT) sFTP Service function was invoked ")

	// verify existence of local path
	localFileInfo, err := os.Stat(localFilePath)
	if err != nil {
		log.Errorf("Error verifying local file %s: %v", localFilePath, err)
		return agaveproto.TransferResponse{Error: err.Error()}
	}

	// check that the local path is not a directory
	if localFileInfo.IsDir() {
		err = errors.New(fmt.Sprintf("Error opening, %s. Local path is a directory.", localFilePath))
		return agaveproto.TransferResponse{Error: "source path is a directory"}
	}

	//###################################################################################################################
	// check if the file already exists. If is  does not and Force = false then error. Otherwise remove the
	//file first then do everything else
	var remoteFileExists bool
	remoteFileInfo, err := sftpClient.Stat(remoteFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			remoteFileExists = false
		} else {
			log.Errorf("Error attempting to stat remote path %s: %v", remoteFilePath, err)
			return agaveproto.TransferResponse{Error: err.Error()}
		}
	} else {
		remoteFileExists = true
	}

	// Create the destination file
	if remoteFileExists && remoteFileInfo.IsDir() {
		log.Errorf("Error opening, %s. Destination is a directory.", remoteFilePath)
		return agaveproto.TransferResponse{Error: "destination path is a directory"}
	} else if (remoteFileExists && force) || !remoteFileExists {
		//if remoteFileExists {
		//	err := sftpClient.Remove(remoteFilePath)
		//	if err != nil {
		//		log.Errorf("Failed to remove existing file, %s : %v", remoteFilePath, err)
		//	}
		//}

		// now that the dest file is created the next step is to open the source file
		log.Debugf("Opening local source file %s", localFilePath)

		localFile, err := os.Open(localFilePath)
		if err != nil {
			log.Errorf("Failed to open source file %s: %v", localFilePath, err)
			return agaveproto.TransferResponse{Error: err.Error()}
		}
		defer localFile.Close()

		remoteFile, err := sftpClient.OpenFile(remoteFilePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC)
		if err != nil {
			log.Errorf("Error opening handle to dest file. %s. %v", remoteFilePath, err)
			return agaveproto.TransferResponse{Error: err.Error()}
		}
		defer remoteFile.Close()

		var bytesWritten int64
		// if there were no error opening the two files then process the file.

		log.Debugf("Writing to remote file %s", remoteFilePath)
		// this will write the file BufferSize blocks until EOF is returned.
		bytesWritten, err = remoteFile.ReadFrom(localFile)
		if err != nil {
			log.Errorf("Error after writing %d byes to file %s: %v", bytesWritten, remoteFilePath, err)
			return agaveproto.TransferResponse{BytesTransferred: bytesWritten, Error: err.Error()}
		}
		log.Debugf("%d bytes copied to %s", bytesWritten, remoteFilePath)

		// flush the buffer to ensure everything was written
		err = localFile.Sync()
		if err != nil {
			log.Errorf("Error flushing source file, %s, when writing to %s: %v", localFilePath, remoteFilePath, err)
			return agaveproto.TransferResponse{Error: err.Error()}
		}
		// ensure permissions are retained
		log.Debugf("Setting permissions of dest file %s", remoteFilePath)
		err = sftpClient.Chmod(remoteFilePath, localFileInfo.Mode().Perm())
		if err != nil {
			log.Errorf("Error setting permissions of dest file %s after upload: %v", remoteFilePath, err)
			return agaveproto.TransferResponse{Error: err.Error()}
		}

		// ensure timestamps are retained
		log.Debugf("Setting modification time of dest file %s", remoteFilePath)
		err = sftpClient.Chtimes(remoteFilePath, time.Now(), localFileInfo.ModTime())
		if err != nil {
			log.Errorf("Error setting modifcation time of dest file %s after upload: %v", remoteFilePath, err)
			return agaveproto.TransferResponse{Error: err.Error()}
		}

		log.Debugf("Completed transfer to dest file %s", remoteFilePath)
		remoteFileInfo, _ = sftpClient.Stat(remoteFilePath)
		rfi := NewRemoteFileInfo(remoteFilePath, remoteFileInfo)
		return agaveproto.TransferResponse{
			RemoteFileInfo: &rfi,
			BytesTransferred: bytesWritten,
		}
	} else {
		return agaveproto.TransferResponse{Error: "file already exists"}
	}
}


func mkdir(sftpClient *sftp.Client, remoteDirectoryPath string, recursive bool) agaveproto.FileInfoResponse {
	log.Trace("Mkdir (Mkdirs) sFTP Service function was invoked ")

	// Create destination directory
	log.Debugf("Create destination directory %s", remoteDirectoryPath)
	if recursive {
		err := sftpClient.MkdirAll(remoteDirectoryPath)
		if err != nil {
			log.Errorf("Error creating directory. %s. %v", remoteDirectoryPath, err)
			return agaveproto.FileInfoResponse{Error: err.Error()}
		}
	} else {
		remoteFileInfo, err := sftpClient.Stat(remoteDirectoryPath)
		if err == nil {
			if remoteFileInfo.IsDir() {
				return agaveproto.FileInfoResponse{Error: "dir already exists"}
			} else {
				return agaveproto.FileInfoResponse{Error: "file already exists"}
			}
		}

		err = sftpClient.Mkdir(remoteDirectoryPath)
		if err != nil {
			log.Errorf("Error creating directory. %s. %v", remoteDirectoryPath, err)
			return agaveproto.FileInfoResponse{Error: err.Error()}
		}
	}

	log.Debugf("Created destination directory: " + remoteDirectoryPath)

	// stat the remoete file to return the fileinfo data
	remoteFileInfo, err := sftpClient.Stat(remoteDirectoryPath)
	if err != nil {
		return agaveproto.FileInfoResponse{Error: err.Error()}
	}

	rfi := NewRemoteFileInfo(remoteDirectoryPath, remoteFileInfo)
	return agaveproto.FileInfoResponse{
		RemoteFileInfo: &rfi,
	}
}

func stat(sftpClient *sftp.Client, remotePath string) agaveproto.FileInfoResponse {
	log.Trace("Stat (Stat) sFTP Service function was invoked ")

	// Stat the remote path
	remoteFileInfo, err := sftpClient.Stat(remotePath)
	if err != nil {
		log.Errorf("Unable to stat file %v", remotePath)
		return agaveproto.FileInfoResponse{Error: err.Error()}
	}

	rfi := NewRemoteFileInfo(remotePath, remoteFileInfo)
	return agaveproto.FileInfoResponse{
		RemoteFileInfo: &rfi,
	}
}

func remove(sftpClient *sftp.Client, remotePath string) agaveproto.EmptyResponse {
	log.Trace("Remove (Remove) sFTP Service function was invoked ")

	// Get the FileInfo for the remote path so we know how to proceed
	stat, err := sftpClient.Stat(remotePath)
	if err != nil {
		log.Errorf("Unable to stat %s: %v", remotePath, err)
		return agaveproto.EmptyResponse{Error: err.Error()}
	}

	// if the remote path is a directory, we need to walk the tree and delete the files before
	// deleting any of the directories.
	if stat.IsDir() {
		subdirMap := make(map[string]os.FileInfo)

		log.Printf("Deleting destination directory %s", remotePath)
		walker := sftpClient.Walk(remotePath)
		// the sftp.Walk object will traverse the tree breadth first, allowing us to
		// delete files as they come, and build a list of directories to delete
		// after we complete walking the full tree.
		for walker.Step() {
			if err := walker.Err(); err != nil && err.Error() != "EOF" {
				log.Warnf("Error traversing directory tree: %v", err)
				break
			}

			stat := walker.Stat()

			if stat.IsDir() {
				subdirMap[walker.Path()] = stat
			} else {
				log.Debugf("Deleting nested file at %s", walker.Path())
				err = sftpClient.Remove(walker.Path())
				if err != nil {
					log.Errorf("Error deleting nested file %s. %v", remotePath, err)
					break
				}
			}
		}

		// if we finished walking the tree cleanly, then all files are deleted.
		// Now that there are no more files, we can attempt to remove the directories.
		if walker.Err() == nil && err == nil {
			// get the list of directories we need to delete from our map above
			paths := make([]string, 0, len(subdirMap))
			for k := range subdirMap {
				paths = append(paths, k)
			}

			// sort them so we can honor the proper tree
			sort.Strings(paths)

			// reverse the list so we can safely delete depth first
			for i := len(paths)/2 - 1; i >= 0; i-- {
				opp := len(paths) - 1 - i
				paths[i], paths[opp] = paths[opp], paths[i]
			}

			// delete the directories in order
			for _, path := range paths {
				log.Debugf("Deleting nested directory at %s", path)
				err = sftpClient.RemoveDirectory(path)
				if err != nil {
					log.Errorf("Error deleting nested directory %s. %v", remotePath, err)
					return agaveproto.EmptyResponse{Error: err.Error()}
				}
			}
		} else if err != nil {
			log.Errorf("Error deleting one or more nested files below %s. %v", remotePath, err)
			return agaveproto.EmptyResponse{Error: err.Error()}
		} else {
			log.Errorf("Error deleting one or more nested files below %s. %v", remotePath, walker.Err())
			return agaveproto.EmptyResponse{Error: walker.Err().Error()}
		}

		log.Debugf("Deleted remote directory: %s", remotePath)
	} else {
		// the remote path was a file, so we just delete it directly.
		log.Printf("Deleting destination file %s", remotePath)
		err = sftpClient.Remove(remotePath)
		if err != nil {
			log.Errorf("Error deleting remote file %s: %v", remotePath, err)
			return agaveproto.EmptyResponse{Error: err.Error()}
		}
		log.Debugf("Deleted remote file: %s", remotePath)
	}

	// everything went well so we can return a successful response
	return agaveproto.EmptyResponse{}
}

func list(sftpClient *sftp.Client, remotePath string) agaveproto.FileInfoListResponse {
	log.Trace("List (List) sFTP Service function was invoked ")

	defer sftpClient.Close()

	// Stat the remote path
	remoteFileInfo, err := sftpClient.Stat(remotePath)
	if err != nil {
		log.Infof("Unable to stat file %s", remotePath)
		return agaveproto.FileInfoListResponse{Error: err.Error()}
	}
	var remoteFileInfoList []*agaveproto.RemoteFileInfo

	if ! remoteFileInfo.IsDir() {
		log.Debugf("Remote path is a file, returning stat for the file %s", remotePath)
		remoteFileInfoList = make([]*agaveproto.RemoteFileInfo, 1)
		rfi := NewRemoteFileInfo(filepath.Join(remotePath, remoteFileInfo.Name()), remoteFileInfo)
		remoteFileInfoList[0] = &rfi
	} else {
		var sftpFileInfoList []os.FileInfo

		log.Debugf("Listing directory %s", remotePath)

		// fetch the directory listing
		sftpFileInfoList, err = sftpClient.ReadDir(remotePath)
		if err != nil {
			log.Errorf("Unable to list directory %s", remotePath)
			return agaveproto.FileInfoListResponse{Error: err.Error()}
		}

		// convert the os.FileInfo into RemoteFileInfo for the response to the user
		remoteFileInfoList = make([]*agaveproto.RemoteFileInfo, len(sftpFileInfoList))
		for index,fileInfo := range sftpFileInfoList {
			rfi := NewRemoteFileInfo(filepath.Join(remotePath, fileInfo.Name()), fileInfo)
			remoteFileInfoList[index] = &rfi
		}
	}

	return agaveproto.FileInfoListResponse{
		Listing: remoteFileInfoList,
	}
}

func rename(sftpClient *sftp.Client, oldRemotePath string, newRemotePath string) agaveproto.FileInfoResponse {
	log.Trace("Rename (Rename) sFTP Service function was invoked ")

	defer sftpClient.Close()

	// stat the renamed path to catch permission and existence checks ahead of time
	remoteRenamedFileInfo, err := sftpClient.Stat(newRemotePath)
	if err != nil && ! os.IsNotExist(err) {
		log.Errorf("Unable to stat renamed file, %s: %v", newRemotePath, err)
		return agaveproto.FileInfoResponse{Error: err.Error()}
	} else if err == nil {
		log.Errorf("file or dir exists")
		return agaveproto.FileInfoResponse{Error: "file or dir exists"}
	}

	// Stat the remote path
	err = sftpClient.Rename(oldRemotePath, newRemotePath)
	if err != nil {
		log.Errorf("Unable to rename file %s to %s: %v", oldRemotePath, newRemotePath, err)
		return agaveproto.FileInfoResponse{Error: err.Error()}
	}

	// stat the remoete file to return the fileinfo data
	remoteRenamedFileInfo, err = sftpClient.Stat(newRemotePath)
	if err != nil {
		log.Errorf("Unable to stat renamed file, %s: %v", newRemotePath, err)
		return agaveproto.FileInfoResponse{Error: err.Error()}
	}

	rfi := NewRemoteFileInfo(newRemotePath, remoteRenamedFileInfo)
	return agaveproto.FileInfoResponse{
		RemoteFileInfo: &rfi,
	}
}


/*
This will return the size of the file in bytes.
It is not presently used.
*/
func GetFileSize(filepath string) (int64, error) {
	fi, err := os.Stat(filepath)
	if err != nil {
		return 0, err
	}
	// get the size
	return fi.Size(), nil
}
