package sftprelay

import (
	"entrogo.com/sshpool/pkg/clientpool"
	"entrogo.com/sshpool/pkg/sesspool"
	"fmt"
	"github.com/pkg/errors"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
	"io"
	"path/filepath"
	"sort"

	agaveproto "github.com/agaveplatform/science-apis/agave-transfers/sftp-relay/pkg/sftpproto"
	"github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"os"
	"time"
)

var log = logrus.New()

type Server struct {
	Registry    prometheus.Registry
	GrpcMetrics grpc_prometheus.ServerMetrics
	Pool        *clientpool.ClientPool
}

//The Pool and AgentSocket variables are created here and instantiated in the init function.
var (
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

//func init() {
//	// Open a log file to log the server
//	f, err := os.OpenFile("sftp-relay.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
//	if err != nil {
//		log.Fatalf("error opening file: %v", err)
//	}
//	wrt := io.MultiWriter(os.Stdout, f)
//	log.SetOutput(wrt)
//
//	// Now set up the ssh connection pool
//	AgentSocket, ok := os.LookupEnv("SSH_AUTH_SOCK")
//	if !ok {
//		log.Fatalln("Could not connect to SSH_AUTH_SOCK. Is ssh-agent running?")
//	}
//	log.Debugf("SSH_AUTH_SOCK = %v", AgentSocket)
//}

// sets the log level on the current logger of this relay instance
func (s *Server) SetLogLevel(level logrus.Level) {
	log.SetLevel(level)
}

func (s *Server) InitMetrics() {
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
	sshKeyHash = RandBytes(32)
}

// Uses the agaveproto.RemoteSystemConfig sent in each request to obtain a sftp client from the pool
func (s *Server) getSftpClientFromPool(ctx context.Context, systemConfig *agaveproto.RemoteSystemConfig) (*sftp.Client, func() error, error) {
	//id, err := ProtobufToJSON(systemConfig)
	//if err != nil {
	//	return nil, nil, err
	//}
	sshConfig, err := NewSSHConfig(systemConfig)
	if err != nil {
		return nil, nil, err
	}

	addr := fmt.Sprintf("%s:%d", sshConfig.Host, sshConfig.Port)
	cfg := &ssh.ClientConfig{
		User:            sshConfig.User,
		Auth:            sshConfig.Auth,
		Timeout:         sshConfig.Timeout,
		HostKeyCallback: sshConfig.HostKeyCallback,
	}

	sftpClient, closeSFTP, err := sesspool.AsSFTPClient(s.Pool.TryClaimSession(ctx, clientpool.WithDialArgs("tcp", addr, cfg), clientpool.WithID(sshConfig.String())))
	if err != nil {
		if errors.Cause(err) == sesspool.PoolExhausted {
			log.Errorf("Connection pool exhausted for %s@%s:%d", systemConfig.Username, systemConfig.Host, systemConfig.Port)
			return nil, nil, errors.Wrap(err, "No connections available in pool")
		} else {
			log.Errorf("Error creating sftp client from pool for %s@%s:%d", systemConfig.Username, systemConfig.Host, systemConfig.Port)
			return nil, nil, errors.Wrap(err, "Error creating sftp client")
		}
	}

	log.Debugf(fmt.Sprintf("Obtained sftp client to sftp://%s@%s:%d", systemConfig.Username, systemConfig.Host, systemConfig.Port))

	return sftpClient, closeSFTP, nil
}

// Performs a basic authentication handshake to the remote system
func (s *Server) AuthCheck(ctx context.Context, req *agaveproto.AuthenticationCheckRequest) (*agaveproto.EmptyResponse, error) {
	log.Trace("Invoking Auth service")

	log.Printf("AUTH sftp://%s@%s:%d", req.SystemConfig.Username, req.SystemConfig.Host, req.SystemConfig.Port)

	_, closeSFTP, err := s.getSftpClientFromPool(ctx, req.SystemConfig)
	if err != nil {
		return &agaveproto.EmptyResponse{Error: err.Error()}, nil
	}
	defer closeSFTP()

	// increment the request metric
	authCounterMetric.WithLabelValues(req.SystemConfig.Host).Inc()

	// success returns an empty response
	return &agaveproto.EmptyResponse{}, nil
}

// Performs a mkdirs operation on a remote system.
func (s *Server) Mkdir(ctx context.Context, req *agaveproto.SrvMkdirRequest) (*agaveproto.FileInfoResponse, error) {
	log.Trace("Invoking Mkdirs service")

	sftpClient, closeSFTP, err := s.getSftpClientFromPool(ctx, req.SystemConfig)
	if err != nil {
		return &agaveproto.FileInfoResponse{Error: err.Error()}, nil
	}
	defer closeSFTP()

	log.Printf("MKDIR sftp://%s@%s:%d/%s", req.SystemConfig.Username, req.SystemConfig.Host, req.SystemConfig.Port, req.RemotePath)

	// increment the request metric
	mkdirsCounterMetric.WithLabelValues(req.SystemConfig.Host).Inc()

	var response agaveproto.FileInfoResponse

	// Read From system
	response = mkdir(sftpClient, req.RemotePath, req.Recursive)

	return &response, nil
}

// Fetches file info for a remote path
func (s *Server) Stat(ctx context.Context, req *agaveproto.SrvStatRequest) (*agaveproto.FileInfoResponse, error) {
	log.Trace("Invoking Stat service")

	sftpClient, closeSFTP, err := s.getSftpClientFromPool(ctx, req.SystemConfig)
	if err != nil {
		return &agaveproto.FileInfoResponse{Error: err.Error()}, nil
	}
	defer closeSFTP()

	log.Printf("STAT sftp://%s@%s:%d/%s", req.SystemConfig.Username, req.SystemConfig.Host, req.SystemConfig.Port, req.RemotePath)

	// increment the request metric
	statCounterMetric.WithLabelValues(req.SystemConfig.Host).Inc()

	var response agaveproto.FileInfoResponse

	// invoke the internal action
	response = stat(sftpClient, req.RemotePath)

	return &response, nil
}

// Removes the remote path. If the remote path is a directory, the entire tree is deleted.
func (s *Server) Remove(ctx context.Context, req *agaveproto.SrvRemoveRequest) (*agaveproto.EmptyResponse, error) {
	log.Trace("Invoking Remove service")

	sftpClient, closeSFTP, err := s.getSftpClientFromPool(ctx, req.SystemConfig)
	if err != nil {
		return &agaveproto.EmptyResponse{Error: err.Error()}, nil
	}
	defer closeSFTP()

	log.Printf("REMOVE sftp://%s@%s:%d/%s", req.SystemConfig.Username, req.SystemConfig.Host, req.SystemConfig.Port, req.RemotePath)

	// increment the request metric
	removeCounterMetric.WithLabelValues(req.SystemConfig.Host).Inc()

	var response agaveproto.EmptyResponse

	// invoke the internal action
	response = remove(sftpClient, req.RemotePath)

	return &response, nil
}

// Fetches a file from the remote system and stores it locally
func (s *Server) Get(ctx context.Context, req *agaveproto.SrvGetRequest) (*agaveproto.TransferResponse, error) {
	log.Trace("Invoking Get service")

	sftpClient, closeSFTP, err := s.getSftpClientFromPool(ctx, req.SystemConfig)
	if err != nil {
		return &agaveproto.TransferResponse{Error: err.Error()}, nil
	}
	defer closeSFTP()

	log.Printf("GET sftp://%s@%s:%d/%s => file://localhost/%s", req.SystemConfig.Username, req.SystemConfig.Host, req.SystemConfig.Port, req.RemotePath, req.LocalPath)

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
func (s *Server) Put(ctx context.Context, req *agaveproto.SrvPutRequest) (*agaveproto.TransferResponse, error) {
	log.Trace("Invoking Put service")

	sftpClient, closeSFTP, err := s.getSftpClientFromPool(ctx, req.SystemConfig)
	if err != nil {
		return &agaveproto.TransferResponse{Error: err.Error()}, nil
	}
	defer closeSFTP()

	log.Printf("PUT file://localhost/%s => sftp://%s@%s:%d/%s", req.LocalPath, req.SystemConfig.Username, req.SystemConfig.Host, req.SystemConfig.Port, req.RemotePath)

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
func (s *Server) List(ctx context.Context, req *agaveproto.SrvListRequest) (*agaveproto.FileInfoListResponse, error) {
	log.Trace("Invoking list service")

	sftpClient, closeSFTP, err := s.getSftpClientFromPool(ctx, req.SystemConfig)
	if err != nil {
		return &agaveproto.FileInfoListResponse{Error: err.Error()}, nil
	}
	defer closeSFTP()

	log.Printf("LIST sftp://%s@%s:%d/%s",  req.SystemConfig.Username, req.SystemConfig.Host, req.SystemConfig.Port, req.RemotePath)

	// increment the request metric
	listCounterMetric.WithLabelValues(req.SystemConfig.Host).Inc()

	var response agaveproto.FileInfoListResponse

	// invoke the internal action
	response = list(sftpClient, req.RemotePath)

	return &response, nil
}

// Fetches file info for a remote path
func (s *Server) Rename(ctx context.Context, req *agaveproto.SrvRenameRequest) (*agaveproto.FileInfoResponse, error) {
	log.Trace("Invoking Rename service")

	sftpClient, closeSFTP, err := s.getSftpClientFromPool(ctx, req.SystemConfig)
	if err != nil {
		return &agaveproto.FileInfoResponse{Error: err.Error()}, nil
	}
	defer closeSFTP()

	log.Printf("RENAME sftp://%s@%s:%d/%s => sftp://%s@%s:%d/%s",
		req.SystemConfig.Username, req.SystemConfig.Host, req.SystemConfig.Port, req.RemotePath,
		req.SystemConfig.Username, req.SystemConfig.Host, req.SystemConfig.Port, req.NewName)

	// increment the request metric
	statCounterMetric.WithLabelValues(req.SystemConfig.Host).Inc()

	var response agaveproto.FileInfoResponse

	// invoke the internal action
	response = rename(sftpClient, req.RemotePath, req.NewName)

	return &response, nil
}

//func (s *Server) Close(ctx context.Context, req *agaveproto.AuthenticationCheckRequest) (*agaveproto.EmptyResponse, error) {
//	log.Trace("Invoking Rename service")
//
//	log.Printf("AUTH sftp://%s@%s:%d", req.SystemConfig.Username, req.SystemConfig.Host, req.SystemConfig.Port)
//
//	s.Pool.Close()
//	_, closeSFTP, err := s.getSftpClientFromPool(ctx, req.SystemConfig)
//	if err != nil {
//		return &agaveproto.EmptyResponse{Error: err.Error()}, nil
//	}
//	defer closeSFTP()
//
//	// increment the request metric
//	authCounterMetric.WithLabelValues(req.SystemConfig.Host).Inc()
//
//	// success returns an empty response
//	return &agaveproto.EmptyResponse{}, nil
//}

//*****************************************************************************************

/*
The last sections each implement an SFTP process.
*/

func get(sftpClient *sftp.Client, localFilePath string, remoteFilePath string, force bool, byteRanges string) agaveproto.TransferResponse {
	log.Trace("WriteTo (PUT) GRPC service function was invoked ")

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
			log.Errorf("Error opening source file %s: %v", localFilePath, err)
			return agaveproto.TransferResponse{Error: err.Error()}
		}
		defer localFile.Close()

		var bytesWritten int64

		log.Debugf("Writing to local file %s", remoteFilePath)
		// this will write the file BufferSize blocks until EOF is returned.
		//bytesWritten, err = remoteFile.WriteTo(localFile)
		bytesWritten, err = io.Copy(localFile, remoteFile)
		if err != nil {
			log.Errorf("Error after writing %d byes to file %s: %v", bytesWritten, localFilePath, err)
			return agaveproto.TransferResponse{Error: err.Error()}
		}
		log.Debugf("%d bytes copied to %s", bytesWritten, localFilePath)

		// flush the buffer to ensure everything was written
		log.Debugf("Flushing local buffer after writing %s", localFilePath)
		err = localFile.Sync()
		if err != nil {
			log.Errorf("Error flushing local file, %s, when reading from %s: %v", localFilePath, remoteFilePath, err)
			return agaveproto.TransferResponse{Error: err.Error()}
		}
		// ensure permissions are retained
		log.Debugf("Setting permissions of local file %s", localFilePath)
		err = os.Chmod(localFilePath, remoteFileInfo.Mode())
		if err != nil {
			log.Errorf("Error setting permissions of dest file %s after download: %v", localFilePath, err)
			return agaveproto.TransferResponse{Error: err.Error()}
		}

		// ensure timestamps are retained
		log.Debugf("Setting modification time of local file %s", localFilePath)
		err = os.Chtimes(localFilePath, time.Now(), remoteFileInfo.ModTime())
		if err != nil {
			log.Errorf("Error setting modifcation time of local file %s after download: %v", localFilePath, err)
			return agaveproto.TransferResponse{Error: err.Error()}
		}

		log.Debugf("Completed transfer to local file %s", localFilePath)
		localFileInfo, _ = os.Stat(localFilePath)

		rfi := NewRemoteFileInfo(localFilePath, localFileInfo)
		return agaveproto.TransferResponse{
			RemoteFileInfo:   &rfi,
			BytesTransferred: bytesWritten,
		}
	} else {
		return agaveproto.TransferResponse{Error: "file already exists"}
	}
}

func put(sftpClient *sftp.Client, localFilePath string, remoteFilePath string, force bool, append bool) agaveproto.TransferResponse {
	log.Trace("WriteTo (PUT) GRPC service function was invoked ")

	// verify existence of local path
	localFileInfo, err := os.Stat(localFilePath)
	if err != nil {
		log.Errorf("Error verifying local file %s: %v", localFilePath, err)
		return agaveproto.TransferResponse{Error: err.Error()}
	}

	// check that the local path is not a directory
	if localFileInfo.IsDir() {
		log.Errorf("Error verifying local file %s: directory uploads not supported", localFilePath)
		return agaveproto.TransferResponse{Error: "source path is a directory"}
	}

	//###################################################################################################################
	// check if the file already exists. If is  does not and Force = false then error. Otherwise remove the
	//file first then do everything else
	var remoteFileExists bool
	log.Debugf("Fetching info for %s to verify write eligibility", remoteFilePath)
	remoteFileInfo, err := sftpClient.Stat(remoteFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			log.Debugf("New file will be created at %s", remoteFilePath)
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
			log.Errorf("Error opening source file %s: %v", localFilePath, err)
			return agaveproto.TransferResponse{Error: err.Error()}
		}
		defer localFile.Close()

		log.Debugf("Opening remote destination file %s", remoteFilePath)
		remoteFile, err := sftpClient.OpenFile(remoteFilePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC)
		if err != nil {
			log.Errorf("Error opening handle to dest file. %s. %v", remoteFilePath, err)
			return agaveproto.TransferResponse{Error: err.Error()}
		}
		defer remoteFile.Close()

		var bytesRead int64
		// if there were no error opening the two files then process the file.

		log.Debugf("Writing to remote file %s", remoteFilePath)
		// this will write the file BufferSize blocks until EOF is returned.
		bytesRead, err = io.Copy(remoteFile, localFile)
		//bytesRead, err = remoteFile.ReadFrom(localFile)
		if err != nil {
			log.Errorf("Error after writing %d byes to file %s: %v", bytesRead, remoteFilePath, err)
			return agaveproto.TransferResponse{BytesTransferred: bytesRead, Error: err.Error()}
		}
		log.Debugf("%d bytes copied to %s", bytesRead, remoteFilePath)

		// flush the buffer to ensure everything was written
		log.Debugf("Flushing remote buffer after writing %s", remoteFilePath)
		err = localFile.Sync()
		if err != nil {
			log.Errorf("Error flushing source file, %s, when writing to %s: %v", localFilePath, remoteFilePath, err)
			return agaveproto.TransferResponse{Error: err.Error()}
		}

		// ensure permissions are retained
		log.Debugf("Setting permissions of dest file %s", remoteFilePath)
		err = sftpClient.Chmod(remoteFilePath, localFileInfo.Mode())
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

		log.Debugf("Completed writing to dest file %s", remoteFilePath)
		remoteFileInfo, _ = sftpClient.Stat(remoteFilePath)
		rfi := NewRemoteFileInfo(remoteFilePath, remoteFileInfo)
		return agaveproto.TransferResponse{
			RemoteFileInfo:   &rfi,
			BytesTransferred: bytesRead,
		}
	} else {
		log.Debugf("Refusing to overwrite remote file %s when force is false", remoteFilePath)
		return agaveproto.TransferResponse{Error: "file already exists"}
	}
}

func mkdir(sftpClient *sftp.Client, remoteDirectoryPath string, recursive bool) agaveproto.FileInfoResponse {
	log.Trace("Mkdir (Mkdirs) GRPC service function was invoked ")

	// Create destination directory
	if recursive {
		log.Debugf("Creating nested destination directory %s", remoteDirectoryPath)
		err := sftpClient.MkdirAll(remoteDirectoryPath)
		if err != nil {
			log.Errorf("Error creating directory. %s. %v", remoteDirectoryPath, err)
			return agaveproto.FileInfoResponse{Error: err.Error()}
		}
	} else {
		remoteFileInfo, err := sftpClient.Stat(remoteDirectoryPath)
		// ignore errors here since we can potentially get odd errors besides os.IsNotExist when performing a stat
		// on a nested tree that does not exist
		if err == nil {
			if remoteFileInfo.IsDir() {
				return agaveproto.FileInfoResponse{Error: "dir already exists"}
			} else {
				return agaveproto.FileInfoResponse{Error: "file already exists"}
			}
		}

		log.Debugf("Creating destination directory %s", remoteDirectoryPath)
		err = sftpClient.Mkdir(remoteDirectoryPath)
		if err != nil {
			log.Errorf("Error creating directory. %s. %v", remoteDirectoryPath, err)
			return agaveproto.FileInfoResponse{Error: err.Error()}
		}
	}

	// stat the remoete file to return the fileinfo data
	log.Debugf("Checking existence of new directory %s" + remoteDirectoryPath)
	remoteFileInfo, err := sftpClient.Stat(remoteDirectoryPath)
	if err != nil {
		log.Errorf("Error verifying existence of remote directory %s: %s", remoteDirectoryPath, err.Error())
		return agaveproto.FileInfoResponse{Error: err.Error()}
	}

	log.Debugf("Completed creating remote directory: %s", remoteDirectoryPath)

	rfi := NewRemoteFileInfo(remoteDirectoryPath, remoteFileInfo)
	return agaveproto.FileInfoResponse{
		RemoteFileInfo: &rfi,
	}
}

func stat(sftpClient *sftp.Client, remotePath string) agaveproto.FileInfoResponse {
	log.Trace("Stat (Stat) GRPC service function was invoked ")

	// Stat the remote path
	log.Debugf("Fetching file info for %s" + remotePath)
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
	log.Trace("Remove (Remove) GRPC service function was invoked ")

	// Get the FileInfo for the remote path so we know how to proceed
	log.Debugf("Fetching info for %s to determine delete behavior", remotePath)
	stat, err := sftpClient.Stat(remotePath)
	if err != nil {
		log.Errorf("Unable to stat %s: %v", remotePath, err)
		return agaveproto.EmptyResponse{Error: err.Error()}
	}

	// if the remote path is a directory, we need to walk the tree and delete the files before
	// deleting any of the directories.
	if stat.IsDir() {
		subdirMap := make(map[string]os.FileInfo)

		log.Debugf("Deleting nested files in destination directory %s", remotePath)
		walker := sftpClient.Walk(remotePath)
		// the sftp.Walk object will traverse the tree breadth first, allowing us to
		// delete files as they come, and build a list of directories to delete
		// after we complete walking the full tree.
		for walker.Step() {
			if err := walker.Err(); err != nil && err.Error() != "EOF" {
				log.Errorf("Error traversing directory tree: %v", err)
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
			log.Debugf("Completed deleting nested files of destination directory %s", remotePath)
			log.Debugf("Deleting nested subdirectories of destination directory %s", remotePath)
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
			log.Debugf("Completed deleting nested subdirectories of destination directory %s", remotePath)
		} else if err != nil {
			log.Errorf("Error deleting one or more nested files below %s. %v", remotePath, err)
			return agaveproto.EmptyResponse{Error: err.Error()}
		} else {
			log.Errorf("Error deleting one or more nested files below %s. %v", remotePath, walker.Err())
			return agaveproto.EmptyResponse{Error: walker.Err().Error()}
		}

		log.Debugf("Completed deleting remote directory: %s", remotePath)
	} else {
		// the remote path was a file, so we just delete it directly.
		log.Printf("Deleting destination file %s", remotePath)
		err = sftpClient.Remove(remotePath)
		if err != nil {
			log.Errorf("Error deleting file %s: %v", remotePath, err)
			return agaveproto.EmptyResponse{Error: err.Error()}
		}
		log.Debugf("Completed deleting remote file: %s", remotePath)
	}

	// everything went well so we can return a successful response
	return agaveproto.EmptyResponse{}
}

func list(sftpClient *sftp.Client, remotePath string) agaveproto.FileInfoListResponse {
	log.Trace("List (List) GRPC service function was invoked ")

	// Stat the remote path
	log.Debugf("Fetching info for %s to determine listing behavior", remotePath)
	remoteFileInfo, err := sftpClient.Stat(remotePath)
	if err != nil {
		log.Infof("Unable to stat file %s", remotePath)
		return agaveproto.FileInfoListResponse{Error: err.Error()}
	}

	var remoteFileInfoList []*agaveproto.RemoteFileInfo

	if !remoteFileInfo.IsDir() {
		log.Debugf("Listing file %s", remotePath)
		remoteFileInfoList = make([]*agaveproto.RemoteFileInfo, 1)
		rfi := NewRemoteFileInfo(filepath.Join(remotePath, remoteFileInfo.Name()), remoteFileInfo)
		remoteFileInfoList[0] = &rfi
	} else {
		var sftpFileInfoList []os.FileInfo

		// fetch the directory listing
		log.Debugf("Listing directory %s", remotePath)
		sftpFileInfoList, err = sftpClient.ReadDir(remotePath)
		if err != nil {
			log.Errorf("Unable to list directory %s", remotePath)
			return agaveproto.FileInfoListResponse{Error: err.Error()}
		}

		log.Tracef("Serializing directory listing response of %s", remotePath)
		// convert the os.FileInfo into RemoteFileInfo for the response to the user
		remoteFileInfoList = make([]*agaveproto.RemoteFileInfo, len(sftpFileInfoList))
		for index, fileInfo := range sftpFileInfoList {
			rfi := NewRemoteFileInfo(filepath.Join(remotePath, fileInfo.Name()), fileInfo)
			remoteFileInfoList[index] = &rfi
		}
	}
	log.Debugf("Completed deleting remote file: %s", remotePath)

	return agaveproto.FileInfoListResponse{
		Listing: remoteFileInfoList,
	}
}

func rename(sftpClient *sftp.Client, oldRemotePath string, newRemotePath string) agaveproto.FileInfoResponse {
	log.Trace("Rename (Rename) GRPC service function was invoked ")

	// stat the renamed path to catch permission and existence checks ahead of time
	log.Debugf("Checking existence of file item with new name %s", newRemotePath)
	remoteRenamedFileInfo, err := sftpClient.Stat(newRemotePath)
	if err != nil && !os.IsNotExist(err) {
		log.Errorf("Unable to stat renamed file, %s: %v", newRemotePath, err)
		return agaveproto.FileInfoResponse{Error: err.Error()}
	} else if err == nil {
		log.Errorf("file or dir exists")
		return agaveproto.FileInfoResponse{Error: "file or dir exists"}
	}

	// Stat the remote path
	log.Infof("Renaming %s to %s", oldRemotePath, newRemotePath)
	err = sftpClient.Rename(oldRemotePath, newRemotePath)
	if err != nil {
		log.Errorf("Unable to rename file %s to %s: %v", oldRemotePath, newRemotePath, err)
		return agaveproto.FileInfoResponse{Error: err.Error()}
	}

	// stat the remoete file to return the fileinfo data
	log.Debugf("Checking existence %s after rename", newRemotePath)
	remoteRenamedFileInfo, err = sftpClient.Stat(newRemotePath)
	if err != nil {
		log.Errorf("Unable to stat renamed file, %s: %v", newRemotePath, err)
		return agaveproto.FileInfoResponse{Error: err.Error()}
	}
	log.Debugf("Completed renaming %s to %s", oldRemotePath, newRemotePath)

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
