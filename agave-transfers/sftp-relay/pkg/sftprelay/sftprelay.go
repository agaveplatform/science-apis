package sftprelay

import (
	"entrogo.com/sshpool/pkg/clientpool"
	"entrogo.com/sshpool/pkg/sesspool"
	"fmt"
	"github.com/agaveplatform/science-apis/agave-transfers/sftp-relay/pkg/utils"
	"github.com/pkg/errors"
	"github.com/pkg/sftp"
	"github.com/spf13/viper"
	"golang.org/x/crypto/ssh"
	"io"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	agaveproto "github.com/agaveplatform/science-apis/agave-transfers/sftp-relay/pkg/sftpproto"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"os"
	"time"
)

const MAX_CONNECTION_RETRIES = 1

var log = logrus.New()

type Server struct {
	Registry    prometheus.Registry
	GrpcMetrics grpc_prometheus.ServerMetrics
	Pool        *clientpool.ClientPool
}

//The Pool and AgentSocket variables are created here and instantiated in the init function.
var (
	// key used to hash the ssh configs in the pool
	sshKeyHash                       []byte

	// dynamic metrics showing various pool dimensions.
	// these metrics are capturned every time the metrics endpoint is called by invoking the callback functions
	ConnectionPoolMaxSizeMetric = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "sftprelay",
		Name: "connection_pool_max_size",
		Help: "Connection pool max size",
	}, func() float64 {
		return float64(viper.GetInt("poolSize"))
	})
	SessionPoolMaxSizeMetric = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "sftprelay",
		Name: "session_pool_max_size",
		Help: "Maximum size of a session client pool",
	}, func() float64 {
		return float64(-1)
	})

	ConnectionPoolResetCounterMetric = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "sftprelay",
		Name: "connection_pool_active_clients",
		Help: "Total complete pool resets",
	}, nil)
	SessionPoolExhaustedCounterMetric = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "sftprelay",
		Name: "session_pool_exhausted_total",
		Help: "Total number of failed client creations due to session pool exhaustion",
	}, []string{"host", "port", "user"})
	SessionInvalidationCounterMetric = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "sftprelay",
		Name: "session_invalidation_total",
		Help: "Total number of failed client creations due to session pool exhaustion",
	}, []string{"host", "port", "user"})

	// queue dimensions capacity metrics populated through a goroutine running checking queue status every 5 seconds
	SessionPoolActiveClientsMetric = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "sftprelay",
		Name: "session_pool_active_clients",
		Help: "Total active clients for a session",
	}, []string{"host", "port", "user"})


	// Number of downloads by file size magnitude
	DownloadCounterMetric = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "sftprelay",
		Name: "get_total_count",
		Help: "File downloads to date",
	}, []string{"host", "port", "user", "magnitude"})

	// Summary stats of download file size
	DownloadBytesSummary = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Namespace: "sftprelay",
		Name: "get_total_bytes",
		Help: "File download size summary distribution in bytes",
	}, []string{"host", "port", "user"})

	// Summary stats of file download duration in sec
	DownloadDurationSummary = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Namespace: "sftprelay",
		Name: "get_duration_seconds",
		Help: "File download duration by file magnitude",
		//Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
	}, []string{"host", "port", "user", "magnitude"})

	// Number of uploads by file size magnitude
	UploadCounterMetric = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "sftprelay",
		Name: "put_total_count",
		Help: "File uploads to date",
	}, []string{"host", "port", "user", "magnitude"})

	// Summary stats of upload file size
	UploadBytesSummary = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Namespace: "sftprelay",
		Name: "put_total_bytes",
		Help: "File upload size summary distribution in bytes",
		//Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
	}, []string{"host", "port", "user"})

	// Summary stats of file upload duration in sec
	UploadDurationSummary = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Namespace: "sftprelay",
		Name: "put_duration_seconds",
		Help: "File uploaded duration by file magnitude",
		//Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
	}, []string{"host", "port", "user", "magnitude"})

	// Individual method invocation metrics
	MethodDurationSummary = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Namespace: "sftprelay",
		Name: "duration_seconds",
		Help: "Operation duration summary in seconds",
		ConstLabels: map[string]string{"grpc_service":"sftpproto.SftpRelay","grpc_type":"unary"},
	}, []string{"grpc_method", "host", "port", "user"})
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

// Sets the log level on the current logger of this relay instance
func (s *Server) SetLogLevel(level logrus.Level) {
	log.SetLevel(level)
}

// Initializes the metrics for this service
func (s *Server) InitMetrics() {

	// get current queue size
	ConnectionPoolSizeMetric := prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "sftprelay",
		Name: "connection_pool_size",
		Help: "Number of connections currently in the pool",
	}, func() float64 {
		return float64(len(s.Pool.PoolStats()))
	})

	s.Registry.MustRegister(&s.GrpcMetrics,
		ConnectionPoolSizeMetric,
		ConnectionPoolMaxSizeMetric,
		SessionPoolMaxSizeMetric,

		ConnectionPoolResetCounterMetric,
		SessionPoolActiveClientsMetric,
		SessionInvalidationCounterMetric,

		DownloadDurationSummary,
		DownloadBytesSummary,
		DownloadCounterMetric,
		UploadDurationSummary,
		UploadBytesSummary,
		UploadCounterMetric,
		MethodDurationSummary,
		)

	// generate a unique hash key to encrypt the ssh configs in the pool
	sshKeyHash = utils.RandBytes(32)

	// background process to collect metrics about active clients for each session in the connection pool
	go func() {
		for {
			poolStats := s.Pool.PoolStats()
			for id,val := range poolStats {
				tuple := strings.Split(id, ":")
				SessionPoolActiveClientsMetric.WithLabelValues(tuple[0], tuple[1], tuple[2]).Set(float64(val))
			}
			time.Sleep(time.Duration(5 * time.Second))
		}
	}()
}

// Wraps the deferred session cleanup behavior in a function to log errors
func (s *Server) logDeferClose(session *clientpool.Session, sshConfig *SSHConfig) {
	err := session.Close()
	if err != nil {
		log.Errorf("Failed closing broken session for %s@%s:%d %v", sshConfig.User, sshConfig.Host, sshConfig.Port, err.Error())
	}
}

// Fetches a sftp client from the connection pool, providing retries upon connection failure
func (s *Server) getSftpClientFromPool(ctx context.Context, systemConfig *agaveproto.RemoteSystemConfig) (*sftp.Client, func(), error) {
	sshConfig, err := NewSSHConfig(systemConfig)
	if err != nil {
		return nil, nil, err
	}

	return s.retryGetSftpClientFromPool(ctx, sshConfig, 0)
}

// Uses the agaveproto.RemoteSystemConfig sent in each request to obtain a sftp client from the pool
// This method will attempt to obtain a session from the session pool for the provided connection.
// If unable to obtain a session due to a server-side connection failure, the session and connection
// will be invalidated and removed from the pool. The connection will then be retried up to
// MAX_CONNECTION_RETRIES times, after which it will fail.
func (s *Server) retryGetSftpClientFromPool(ctx context.Context, sshConfig *SSHConfig, retryCount int) (*sftp.Client, func(), error) {
	attemptPrefix := ""
	if retryCount > 0 {
		attemptPrefix = fmt.Sprintf("[%d] ", retryCount)
	}

	defer log.Tracef("Exiting getSftpClientFromPool after attempt %d to %s@%s:%d", retryCount, sshConfig.User, sshConfig.Host, sshConfig.Port)

	cfg := &ssh.ClientConfig{
		User:            sshConfig.User,
		Auth:            sshConfig.Auth,
		Timeout:         sshConfig.Timeout,
		HostKeyCallback: sshConfig.HostKeyCallback,
	}

	addr := fmt.Sprintf("%s:%d", sshConfig.Host, sshConfig.Port)

	// get or create a session from the pool. This establishes a connection with associated session pool.
	session, err := s.Pool.ClaimSession(ctx, clientpool.WithDialArgs("tcp", addr, cfg), clientpool.WithID(sshConfig.String()))
	if err != nil {
		//	// beware this is the nuclear option. all pool connections will break when the pool closes.
		if strings.Contains(err.Error(), "handshake failed") {
			log.Errorf("Authentication failed to %s@%s:%d %v", sshConfig.User, sshConfig.Host, sshConfig.Port, err.Error())
			return nil, nil, errors.Wrap(err, "Error creating sftp client")
		} else if errors.Cause(err) == clientpool.PoolExhausted {
			log.Errorf("%sUnable to obtain a valid session to %s@%s:%d %v", attemptPrefix, sshConfig.User, sshConfig.Host, sshConfig.Port, err.Error())
			return nil, nil, errors.Wrap(err, "Connection pool is full")
		} else {
			//		//log.Infof("%sBroken connection detected to %s@%s:%d. Closing pool and recreating", attemptPrefix, sshConfig.User, sshConfig.Host, sshConfig.Port)
			//		//// This indicates that the connection has gone bad. We cannot get a handle to the session with the broken connection
			//		//// in the pool, so we have to reset the entire pool.
			//		//defer func() {
			//		//	// update the metric recording full pool resets
			//		//	ConnectionPoolResetCounterMetric.WithLabelValues(sshConfig.Host, strconv.Itoa(sshConfig.Port), sshConfig.User).Inc()
			//		//
			//		//	perr := s.Pool.Close()
			//		//	if perr != nil {
			//		//		log.Errorf("%sUnable to invalidate session pool after bad connection detected to %s@%s:%d %v", attemptPrefix, sshConfig.User, sshConfig.Host, sshConfig.Port, err.Error())
			//		//	}
			//		//}()
			//
			//		// The connection reset succeeded. We can retry the connection provided this wasn't our last attempt,
			//		// and, if successful, just carry on with the work.
			if retryCount < MAX_CONNECTION_RETRIES {
				return s.retryGetSftpClientFromPool(ctx, sshConfig, retryCount+1)
			} else {
				// Retry count exceeded. This operation will now fail, but subsequent ones should succeed.
				log.Errorf("%sMax retries exceeded attempting to create sftp client from pool for %s@%s:%d %v", attemptPrefix, sshConfig.User, sshConfig.Host, sshConfig.Port, err.Error())
				return nil, nil, errors.Wrap(err, "Error creating sftp client")
			}
		}
	}

	// use the session to create a new sftp client we can use in the rest of our app
	sftpClient, cleanup, err := sesspool.AsSFTPClient(session, nil)
	if err != nil {
		// pool exhaustion is valid. We just fail fast here to indicate to the calling service a retry is in order
		if errors.Cause(err) == sesspool.PoolExhausted {
			log.Errorf("%sUnable to obtain a valid session to %s@%s:%d %v", attemptPrefix, sshConfig.User, sshConfig.Host, sshConfig.Port, err.Error())
			return nil, nil, errors.Wrap(err, "Client pool is full")
		//	return nil, nil, errors.Wrap(err, "No sessions available in pool")
		} else if strings.Contains(strings.ToLower(err.Error()), "permission denied") {
			// retry session after cleanup
			log.Errorf("%sError creating sftp client from pool for %s@%s:%d %v", attemptPrefix, sshConfig.User, sshConfig.Host, sshConfig.Port, err.Error())
			return nil, nil, errors.Wrap(err, "Error creating sftp client")
		}
	}
		//} else {
		//	log.Debugf("%sBroken connection detected to %s@%s:%d", attemptPrefix, sshConfig.User, sshConfig.Host, sshConfig.Port)
		//
		//	// increment the session invalidation counter
		//	SessionInvalidationCounterMetric.WithLabelValues(sshConfig.Host, strconv.Itoa(sshConfig.Port), sshConfig.User).Inc()
		//
		//	// This indicates that the connection has gone bad. We need to close the session and invalidate the
		//	// connection because all subsequent operations on this connection will now fail.
		//	cerr := session.InvalidateClient()
		//	if cerr != nil {
		//		// if this fails, there's not much we can do. All calls to this remote host will fail until the
		//		// remote side releases any stuck connections, or we restart this server. Our only option is to
		//		// ensure we cleanup the session we created and return an error.
		//		defer s.logDeferClose(session, sshConfig)
		//		log.Errorf("%sUnable to invalidate connection to %s@%s:%d %v", attemptPrefix, sshConfig.User, sshConfig.Host, sshConfig.Port, err.Error())
		//		return nil, nil, errors.Wrap(cerr, "Broken connection detection. Unable to restore the connection to the pool.")
		//	}

			// The connection reset succeeded. We can retry the connection provided this wasn't our last attempt,
			// and, if successful, just carry on with the work.
			//if retryCount < MAX_CONNECTION_RETRIES {
			//	return s.retryGetSftpClientFromPool(ctx, sshConfig, retryCount+1)
			//} else {
			//	// Retry count exceeded. This operation will now fail, but subsequent ones should succeed.
			//	log.Errorf("%sMax retries exceeded attempting to create sftp client from pool for %s@%s:%d %v", attemptPrefix, sshConfig.User, sshConfig.Host, sshConfig.Port, err.Error())
			//	return nil, nil, errors.Wrap(err, "Error creating sftp client")
			//}
		//}
	//}

	log.Debugf(fmt.Sprintf("Obtained sftp client to sftp://%s@%s:%d", sshConfig.User, sshConfig.Host, sshConfig.Port))

	// update the session client gauge metric with the new value
	SessionPoolActiveClientsMetric.WithLabelValues(sshConfig.Host, strconv.Itoa(sshConfig.Port), sshConfig.User).Set(float64(s.Pool.NumSessionsForID(sshConfig.String())))

	return sftpClient, func() {
			cerr := cleanup()
			if cerr != nil && errors.Cause(cerr) != io.EOF {
				log.Errorf("Failed cleaning up sftp client for %s@%s:%d %v", sshConfig.User, sshConfig.Host, sshConfig.Port, cerr.Error())
			}
		}, nil
}

// helper util to log method duration metrics
func recordDurationMetric(s string, systemConfig *agaveproto.RemoteSystemConfig, sec float64 ) {
	MethodDurationSummary.WithLabelValues("Get", systemConfig.Host, strconv.Itoa(int(systemConfig.Port)), systemConfig.Username).Observe(sec)
}

// classifies the given bytes by file size
func fileMagnitude(bytes int64) string {
	if bytes < 1000 {
		return "B"
	} else if bytes < 1000000 {
		return "KiB"
	} else if bytes < 1000000000 {
		return "MiB"
	} else if bytes < 1000000000000 {
		return "GiB"
	} else if bytes < 1000000000000000 {
		return "TiB"
	} else if bytes < 1000000000000000000 {
		return "PiB"
	} else {
		return "max"
	}
}


// Performs a basic authentication handshake to the remote system
func (s *Server) AuthCheck(ctx context.Context, req *agaveproto.AuthenticationCheckRequest) (*agaveproto.EmptyResponse, error) {
	log.Trace("Invoking Auth service")
	log.Printf("AUTH sftp://%s@%s:%d", req.SystemConfig.Username, req.SystemConfig.Host, req.SystemConfig.Port)

	st := time.Now()

	_, closeSFTP, err := s.getSftpClientFromPool(ctx, req.SystemConfig)
	if err != nil {
		return &agaveproto.EmptyResponse{Error: err.Error()}, nil
	}
	defer closeSFTP()

	// observe the remote operation duration
	recordDurationMetric("Auth", req.SystemConfig, time.Now().Sub(st).Seconds())

	// success returns an empty response
	return &agaveproto.EmptyResponse{}, nil
}

// Invalidates the current session. This will kill all currently operating clients bound to the session pool
func (s *Server) Disconnect(ctx context.Context, req *agaveproto.AuthenticationCheckRequest) (*agaveproto.EmptyResponse, error) {
	log.Trace("Invoking Disconnect service")
	log.Printf("DISCONNECT sftp://%s@%s:%d", req.SystemConfig.Username, req.SystemConfig.Host, req.SystemConfig.Port)

	st := time.Now()

	sshConfig, err := NewSSHConfig(req.SystemConfig)
	if err != nil {
		return &agaveproto.EmptyResponse{Error: err.Error()}, nil
	}

	cfg := &ssh.ClientConfig{
		User:            sshConfig.User,
		Auth:            sshConfig.Auth,
		Timeout:         sshConfig.Timeout,
		HostKeyCallback: sshConfig.HostKeyCallback,
	}

	addr := fmt.Sprintf("%s:%d", sshConfig.Host, sshConfig.Port)

	// get or create a session from the pool. This establishes a connection with associated session pool.
	session, err := s.Pool.ClaimSession(ctx, clientpool.WithDialArgs("tcp", addr, cfg), clientpool.WithID(sshConfig.String()))
	if err != nil {
		log.Errorf("Unable to establish a valid session to %s@%s:%d %v", sshConfig.User, sshConfig.Host, sshConfig.Port, err.Error())
		return &agaveproto.EmptyResponse{Error: fmt.Sprintf("No session available in pool. %v", err.Error())}, nil
	}

	// increment the disconnect request metric
	recordDurationMetric("Disconnect", req.SystemConfig, time.Now().Sub(st).Seconds())

	// increment the session invalidation counter
	SessionInvalidationCounterMetric.WithLabelValues(sshConfig.Host, strconv.Itoa(sshConfig.Port), sshConfig.User).Inc()

	err = session.InvalidateClient()
	if err != nil {
		return &agaveproto.EmptyResponse{Error: fmt.Sprintf("Unable to invalidate connection to %s@%s:%d %v", sshConfig.User, sshConfig.Host, sshConfig.Port, err.Error())}, nil
	}

	// success returns an empty response
	return &agaveproto.EmptyResponse{}, nil
}

// Performs a mkdirs operation on a remote system.
func (s *Server) Mkdir(ctx context.Context, req *agaveproto.SrvMkdirRequest) (*agaveproto.FileInfoResponse, error) {
	log.Trace("Invoking Mkdirs service")
	log.Printf("MKDIR sftp://%s@%s:%d/%s", req.SystemConfig.Username, req.SystemConfig.Host, req.SystemConfig.Port, req.RemotePath)

	sftpClient, closeSFTP, err := s.getSftpClientFromPool(ctx, req.SystemConfig)
	if err != nil {
		return &agaveproto.FileInfoResponse{Error: err.Error()}, nil
	}
	defer closeSFTP()

	// call remote operation
	response, sec := mkdir(sftpClient, req.RemotePath, req.Recursive)

	// increment the request metric
	if response.Error != "" {
		recordDurationMetric("GET", req.SystemConfig, sec)
	}
	return response, nil
}

// Fetches file info for a remote path
func (s *Server) Stat(ctx context.Context, req *agaveproto.SrvStatRequest) (*agaveproto.FileInfoResponse, error) {
	log.Trace("Invoking Stat service")
	log.Printf("STAT sftp://%s@%s:%d/%s", req.SystemConfig.Username, req.SystemConfig.Host, req.SystemConfig.Port, req.RemotePath)

	sftpClient, closeSFTP, err := s.getSftpClientFromPool(ctx, req.SystemConfig)
	if err != nil {
		return &agaveproto.FileInfoResponse{Error: err.Error()}, nil
	}
	defer closeSFTP()

	// call remote operation
	response, sec := stat(sftpClient, req.RemotePath)

	// increment the request metric
	if response.Error != "" {
		recordDurationMetric("Stat", req.SystemConfig, sec)
	}
	return response, nil
}

// Removes the remote path. If the remote path is a directory, the entire tree is deleted.
func (s *Server) Remove(ctx context.Context, req *agaveproto.SrvRemoveRequest) (*agaveproto.EmptyResponse, error) {
	log.Trace("Invoking Remove service")
	log.Printf("REMOVE sftp://%s@%s:%d/%s", req.SystemConfig.Username, req.SystemConfig.Host, req.SystemConfig.Port, req.RemotePath)

	sftpClient, closeSFTP, err := s.getSftpClientFromPool(ctx, req.SystemConfig)
	if err != nil {
		return &agaveproto.EmptyResponse{Error: err.Error()}, nil
	}
	defer closeSFTP()

	// call remote operation
	response, sec := remove(sftpClient, req.RemotePath)

	// increment the request metric
	if response.Error != "" {
		recordDurationMetric("Remove", req.SystemConfig, sec)
	}
	return response, nil
}

// Fetches a file from the remote system and stores it locally
func (s *Server) Get(ctx context.Context, req *agaveproto.SrvGetRequest) (*agaveproto.TransferResponse, error) {
	log.Trace("Invoking Get service")
	log.Printf("GET sftp://%s@%s:%d/%s => file://localhost/%s", req.SystemConfig.Username, req.SystemConfig.Host, req.SystemConfig.Port, req.RemotePath, req.LocalPath)

	sftpClient, closeSFTP, err := s.getSftpClientFromPool(ctx, req.SystemConfig)
	if err != nil {
		return &agaveproto.TransferResponse{Error: err.Error()}, nil
	}
	defer closeSFTP()

	// invoke the internal action
	response, sec := get(sftpClient, req.LocalPath, req.RemotePath, req.Force, req.Range)

	// successful download increments counter
	if response.Error == "" {
		DownloadCounterMetric.WithLabelValues(req.SystemConfig.Host, strconv.Itoa(int(req.SystemConfig.Port)), req.SystemConfig.Username, fileMagnitude(response.BytesTransferred)).Inc()
		DownloadBytesSummary.WithLabelValues(req.SystemConfig.Host, strconv.Itoa(int(req.SystemConfig.Port)), req.SystemConfig.Username).Observe(float64(response.BytesTransferred))
		DownloadDurationSummary.WithLabelValues(req.SystemConfig.Host, strconv.Itoa(int(req.SystemConfig.Port)), req.SystemConfig.Username, fileMagnitude(response.BytesTransferred)).Observe(sec)
		// increment the request metric
		recordDurationMetric("Get", req.SystemConfig, sec)
	}

	return response, nil
}

// Transfers a file from the local system to the remote system
func (s *Server) Put(ctx context.Context, req *agaveproto.SrvPutRequest) (*agaveproto.TransferResponse, error) {
	log.Trace("Invoking Put service")
	log.Printf("PUT file://localhost/%s => sftp://%s@%s:%d/%s", req.LocalPath, req.SystemConfig.Username, req.SystemConfig.Host, req.SystemConfig.Port, req.RemotePath)

	sftpClient, closeSFTP, err := s.getSftpClientFromPool(ctx, req.SystemConfig)
	if err != nil {
		return &agaveproto.TransferResponse{Error: err.Error()}, nil
	}
	defer closeSFTP()

	// invoke the internal action
	response, sec := put(sftpClient, req.LocalPath, req.RemotePath, req.Force, req.Append)

	// successful download increments counter
	if response.Error == "" {
		UploadCounterMetric.WithLabelValues(req.SystemConfig.Host, strconv.Itoa(int(req.SystemConfig.Port)), req.SystemConfig.Username, fileMagnitude(response.BytesTransferred)).Inc()
		UploadBytesSummary.WithLabelValues(req.SystemConfig.Host, strconv.Itoa(int(req.SystemConfig.Port)), req.SystemConfig.Username).Observe(float64(response.BytesTransferred))
		UploadDurationSummary.WithLabelValues(req.SystemConfig.Host, strconv.Itoa(int(req.SystemConfig.Port)), req.SystemConfig.Username, fileMagnitude(response.BytesTransferred)).Observe(sec)
		// increment the request metric
		recordDurationMetric("Put", req.SystemConfig, sec)
	}

	return response, nil
}

// Transfers a file from the local system to the remote system
func (s *Server) List(ctx context.Context, req *agaveproto.SrvListRequest) (*agaveproto.FileInfoListResponse, error) {
	log.Trace("Invoking list service")
	log.Printf("LIST sftp://%s@%s:%d/%s",  req.SystemConfig.Username, req.SystemConfig.Host, req.SystemConfig.Port, req.RemotePath)

	sftpClient, closeSFTP, err := s.getSftpClientFromPool(ctx, req.SystemConfig)
	if err != nil {
		return &agaveproto.FileInfoListResponse{Error: err.Error()}, nil
	}
	defer closeSFTP()

	// call remote operation
	response, sec := list(sftpClient, req.RemotePath)

	// increment the request metric
	if response.Error != "" {
		recordDurationMetric("List", req.SystemConfig, sec)
	}
	return response, nil
}

// Fetches file info for a remote path
func (s *Server) Rename(ctx context.Context, req *agaveproto.SrvRenameRequest) (*agaveproto.FileInfoResponse, error) {
	log.Trace("Invoking Rename service")
	log.Printf("RENAME sftp://%s@%s:%d/%s => sftp://%s@%s:%d/%s",
		req.SystemConfig.Username, req.SystemConfig.Host, req.SystemConfig.Port, req.RemotePath,
		req.SystemConfig.Username, req.SystemConfig.Host, req.SystemConfig.Port, req.NewName)

	sftpClient, closeSFTP, err := s.getSftpClientFromPool(ctx, req.SystemConfig)
	if err != nil {
		return &agaveproto.FileInfoResponse{Error: err.Error()}, nil
	}
	defer closeSFTP()

	// call remote operation
	response, sec := rename(sftpClient, req.RemotePath, req.NewName)

	// increment the request metric
	if response.Error != "" {
		recordDurationMetric("Rename", req.SystemConfig, sec)
	}
	return response, nil

}

//*****************************************************************************************

/*
The last sections each implement an SFTP process.
*/

func get(sftpClient *sftp.Client, localFilePath string, remoteFilePath string, force bool, byteRanges string) (*agaveproto.TransferResponse, float64) {
	log.Trace("WriteTo (PUT) GRPC service function was invoked ")
	st := time.Now()

	// check if the file already exists. If is  does not and Force = false then error. Otherwise remove the
	//file first then do everything else
	var localFileExists bool
	localFileInfo, err := os.Stat(localFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			localFileExists = false
		} else {
			log.Errorf("Error attempting to stat local path %s: %v", remoteFilePath, err)
			return &agaveproto.TransferResponse{Error: err.Error()}, 0
		}
	} else {
		localFileExists = true
	}

	// Create the destination file
	if localFileExists && localFileInfo.IsDir() {
		log.Errorf("Error opening, %s. Destination is a directory.", remoteFilePath)
		return &agaveproto.TransferResponse{Error: "destination path is a directory"}, 0
	} else if (localFileExists && force) || !localFileExists {

		remoteFile, err := sftpClient.Open(remoteFilePath)
		if err != nil {
			log.Errorf("Error opening handle to dest file. %s. %v", remoteFilePath, err)
			return &agaveproto.TransferResponse{Error: err.Error()}, 0
		}
		defer remoteFile.Close()

		// verify existence of local path
		remoteFileInfo, err := sftpClient.Stat(remoteFilePath)
		if err != nil {
			log.Errorf("Error verifying remote file %s: %v", remoteFilePath, err)
			return &agaveproto.TransferResponse{Error: err.Error()}, 0
		}

		// check that the remote path is not a directory
		if remoteFileInfo.IsDir() {
			err = errors.New(fmt.Sprintf("Error opening, %s. Remote path is a directory.", remoteFilePath))
			return &agaveproto.TransferResponse{Error: "source path is a directory"}, 0
		}

		// now that the dest file is open the next step is to open the source file
		log.Debugf("Opening local destination file %s", localFilePath)

		localFile, err := os.OpenFile(localFilePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0755)
		if err != nil {
			log.Errorf("Error opening source file %s: %v", localFilePath, err)
			return &agaveproto.TransferResponse{Error: err.Error()}, 0
		}
		defer localFile.Close()

		var bytesWritten int64

		log.Debugf("Writing to local file %s", remoteFilePath)
		// this will write the file BufferSize blocks until EOF is returned.
		//bytesWritten, err = remoteFile.WriteTo(localFile)
		bytesWritten, err = io.Copy(localFile, remoteFile)
		if err != nil {
			log.Errorf("Error after writing %d byes to file %s: %v", bytesWritten, localFilePath, err)
			return &agaveproto.TransferResponse{Error: err.Error()}, 0
		}
		log.Debugf("%d bytes copied to %s", bytesWritten, localFilePath)

		// flush the buffer to ensure everything was written
		log.Debugf("Flushing local buffer after writing %s", localFilePath)
		err = localFile.Sync()
		if err != nil {
			log.Errorf("Error flushing local file, %s, when reading from %s: %v", localFilePath, remoteFilePath, err)
			return &agaveproto.TransferResponse{Error: err.Error()}, 0
		}
		// ensure permissions are retained
		log.Debugf("Setting permissions of local file %s", localFilePath)
		err = os.Chmod(localFilePath, remoteFileInfo.Mode())
		if err != nil {
			log.Errorf("Error setting permissions of dest file %s after download: %v", localFilePath, err)
			return &agaveproto.TransferResponse{Error: err.Error()}, 0
		}

		// ensure timestamps are retained
		log.Debugf("Setting modification time of local file %s", localFilePath)
		err = os.Chtimes(localFilePath, time.Now(), remoteFileInfo.ModTime())
		if err != nil {
			log.Errorf("Error setting modifcation time of local file %s after download: %v", localFilePath, err)
			return &agaveproto.TransferResponse{Error: err.Error()}, 0
		}

		log.Debugf("Completed transfer to local file %s", localFilePath)
		localFileInfo, _ = os.Stat(localFilePath)

		rfi := NewRemoteFileInfo(localFilePath, localFileInfo)
		return &agaveproto.TransferResponse{
			RemoteFileInfo:   &rfi,
			BytesTransferred: bytesWritten,
		}, time.Now().Sub(st).Seconds()
	} else {
		return &agaveproto.TransferResponse{Error: "file already exists"}, 0
	}
}

func put(sftpClient *sftp.Client, localFilePath string, remoteFilePath string, force bool, append bool) (*agaveproto.TransferResponse, float64) {
	log.Trace("WriteTo (PUT) GRPC service function was invoked ")
	st := time.Now()

	// verify existence of local path
	localFileInfo, err := os.Stat(localFilePath)
	if err != nil {
		log.Errorf("Error verifying local file %s: %v", localFilePath, err)
		return &agaveproto.TransferResponse{Error: err.Error()}, 0
	}

	// check that the local path is not a directory
	if localFileInfo.IsDir() {
		log.Errorf("Error verifying local file %s: directory uploads not supported", localFilePath)
		return &agaveproto.TransferResponse{Error: "source path is a directory"}, 0
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
			return &agaveproto.TransferResponse{Error: err.Error()}, 0
		}
	} else {
		remoteFileExists = true
	}

	// Create the destination file
	if remoteFileExists && remoteFileInfo.IsDir() {
		log.Errorf("Error opening, %s. Destination is a directory.", remoteFilePath)
		return &agaveproto.TransferResponse{Error: "destination path is a directory"}, 0
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
			return &agaveproto.TransferResponse{Error: err.Error()}, 0
		}
		defer localFile.Close()

		log.Debugf("Opening remote destination file %s", remoteFilePath)
		remoteFile, err := sftpClient.OpenFile(remoteFilePath, os.O_WRONLY|os.O_CREATE)
		if err != nil {
			log.Errorf("Error opening handle to dest file. %s. %v", remoteFilePath, err)
			return &agaveproto.TransferResponse{Error: err.Error()}, 0
		}
		defer remoteFile.Close()

		log.Debugf("Truncating remote destination file %s", remoteFilePath)
		err = remoteFile.Truncate(0)
		if err != nil {
			log.Errorf("Error truncating dest file prior to write. %s. %v", remoteFilePath, err)
			return &agaveproto.TransferResponse{Error: err.Error()}, 0
		}

		var bytesRead int64
		// if there were no error opening the two files then process the file.

		log.Debugf("Writing to remote file %s", remoteFilePath)
		// this will write the file BufferSize blocks until EOF is returned.
		//bytesRead, err = remoteFile.ReadFrom(localFile)
		bytesRead, err = io.Copy(remoteFile, localFile)

		//bytesRead, err = remoteFile.ReadFrom(localFile)
		if err != nil {
			log.Errorf("Error after writing %d byes to file %s: %v", bytesRead, remoteFilePath, err)
			return &agaveproto.TransferResponse{BytesTransferred: bytesRead, Error: err.Error()}, 0
		}
		log.Debugf("%d bytes copied to %s", bytesRead, remoteFilePath)

		// flush the buffer to ensure everything was written
		log.Debugf("Flushing remote buffer after writing %s", remoteFilePath)
		err = localFile.Sync()
		if err != nil {
			log.Errorf("Error flushing source file, %s, when writing to %s: %v", localFilePath, remoteFilePath, err)
			return &agaveproto.TransferResponse{Error: err.Error()}, 0
		}

		// ensure permissions are retained
		log.Debugf("Setting permissions of dest file %s", remoteFilePath)
		err = sftpClient.Chmod(remoteFilePath, localFileInfo.Mode())
		if err != nil {
			log.Errorf("Error setting permissions of dest file %s after upload: %v", remoteFilePath, err)
			return &agaveproto.TransferResponse{Error: err.Error()}, 0
		}

		// ensure timestamps are retained
		log.Debugf("Setting modification time of dest file %s", remoteFilePath)
		err = sftpClient.Chtimes(remoteFilePath, time.Now(), localFileInfo.ModTime())
		if err != nil {
			log.Errorf("Error setting modifcation time of dest file %s after upload: %v", remoteFilePath, err)
			return &agaveproto.TransferResponse{Error: err.Error()}, 0
		}

		log.Debugf("Completed writing to dest file %s", remoteFilePath)
		remoteFileInfo, _ = sftpClient.Stat(remoteFilePath)
		rfi := NewRemoteFileInfo(remoteFilePath, remoteFileInfo)
		return &agaveproto.TransferResponse{
			RemoteFileInfo:   &rfi,
			BytesTransferred: bytesRead,
		}, time.Now().Sub(st).Seconds()
	} else {
		log.Debugf("Refusing to overwrite remote file %s when force is false", remoteFilePath)
		return &agaveproto.TransferResponse{Error: "file already exists"}, 0
	}
}

func mkdir(sftpClient *sftp.Client, remoteDirectoryPath string, recursive bool) (*agaveproto.FileInfoResponse, float64) {
	log.Trace("Mkdir (Mkdirs) GRPC service function was invoked ")

	st := time.Now()
	// Create destination directory
	if recursive {
		log.Debugf("Creating nested destination directory %s", remoteDirectoryPath)
		err := sftpClient.MkdirAll(remoteDirectoryPath)
		if err != nil {
			log.Errorf("Error creating directory. %s. %v", remoteDirectoryPath, err)
			return &agaveproto.FileInfoResponse{Error: err.Error()}, 0
		}
	} else {
		remoteFileInfo, err := sftpClient.Stat(remoteDirectoryPath)
		// ignore errors here since we can potentially get odd errors besides os.IsNotExist when performing a stat
		// on a nested tree that does not exist
		if err == nil {
			if remoteFileInfo.IsDir() {
				return &agaveproto.FileInfoResponse{Error: "dir already exists"}, 0
			} else {
				return &agaveproto.FileInfoResponse{Error: "file already exists"}, 0
			}
		}

		log.Debugf("Creating destination directory %s", remoteDirectoryPath)
		err = sftpClient.Mkdir(remoteDirectoryPath)
		if err != nil {
			log.Errorf("Error creating directory. %s. %v", remoteDirectoryPath, err)
			return &agaveproto.FileInfoResponse{Error: err.Error()}, 0
		}
	}

	// stat the remoete file to return the fileinfo data
	log.Debugf("Checking existence of new directory %s", remoteDirectoryPath)
	remoteFileInfo, err := sftpClient.Stat(remoteDirectoryPath)
	if err != nil {
		log.Errorf("Error verifying existence of remote directory %s: %s", remoteDirectoryPath, err.Error())
		return &agaveproto.FileInfoResponse{Error: err.Error()}, 0
	}

	log.Debugf("Completed creating remote directory: %s", remoteDirectoryPath)

	rfi := NewRemoteFileInfo(remoteDirectoryPath, remoteFileInfo)
	return &agaveproto.FileInfoResponse{
		RemoteFileInfo: &rfi,
	}, time.Now().Sub(st).Seconds()
}

func stat(sftpClient *sftp.Client, remotePath string) (*agaveproto.FileInfoResponse, float64) {
	log.Trace("Stat (Stat) GRPC service function was invoked ")
	st := time.Now()
	// Stat the remote path
	log.Debugf("Fetching file info for %s", remotePath)
	remoteFileInfo, err := sftpClient.Stat(remotePath)
	if err != nil {
		if errors.Cause(err) != os.ErrNotExist && errors.Cause(err) != os.ErrPermission {
			log.Errorf("Unable to stat file %v", remotePath)
		}
		return &agaveproto.FileInfoResponse{Error: err.Error()}, 0
	}

	rfi := NewRemoteFileInfo(remotePath, remoteFileInfo)
	return &agaveproto.FileInfoResponse{
		RemoteFileInfo: &rfi,
	}, time.Now().Sub(st).Seconds()
}

func remove(sftpClient *sftp.Client, remotePath string) (*agaveproto.EmptyResponse, float64) {
	log.Trace("Remove (Remove) GRPC service function was invoked ")
	st := time.Now()

	// Get the FileInfo for the remote path so we know how to proceed
	log.Debugf("Fetching info for %s to determine delete behavior", remotePath)
	stat, err := sftpClient.Stat(remotePath)
	if err != nil {
		log.Errorf("Unable to stat %s: %v", remotePath, err)
		return &agaveproto.EmptyResponse{Error: err.Error()}, 0
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
					return &agaveproto.EmptyResponse{Error: err.Error()}, 0
				}
			}
			log.Debugf("Completed deleting nested subdirectories of destination directory %s", remotePath)
		} else if err != nil {
			log.Errorf("Error deleting one or more nested files below %s. %v", remotePath, err)
			return &agaveproto.EmptyResponse{Error: err.Error()}, 0
		} else {
			log.Errorf("Error deleting one or more nested files below %s. %v", remotePath, walker.Err())
			return &agaveproto.EmptyResponse{Error: walker.Err().Error()}, 0
		}

		log.Debugf("Completed deleting remote directory: %s", remotePath)
	} else {
		// the remote path was a file, so we just delete it directly.
		log.Printf("Deleting destination file %s", remotePath)
		err = sftpClient.Remove(remotePath)
		if err != nil {
			log.Errorf("Error deleting file %s: %v", remotePath, err)
			return &agaveproto.EmptyResponse{Error: err.Error()}, 0
		}
		log.Debugf("Completed deleting remote file: %s", remotePath)
	}

	// everything went well so we can return a successful response
	return &agaveproto.EmptyResponse{}, time.Now().Sub(st).Seconds()
}

func list(sftpClient *sftp.Client, remotePath string) (*agaveproto.FileInfoListResponse, float64) {
	log.Trace("List (List) GRPC service function was invoked ")
	st := time.Now()

	// Stat the remote path
	log.Debugf("Fetching info for %s to determine listing behavior", remotePath)
	remoteFileInfo, err := sftpClient.Stat(remotePath)
	if err != nil {
		log.Infof("Unable to stat %s", remotePath)
		return &agaveproto.FileInfoListResponse{Error: err.Error()}, 0
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
			return &agaveproto.FileInfoListResponse{Error: err.Error()}, 0
		}

		log.Tracef("Serializing directory listing response of %s", remotePath)
		// convert the os.FileInfo into RemoteFileInfo for the response to the user
		remoteFileInfoList = make([]*agaveproto.RemoteFileInfo, len(sftpFileInfoList))
		for index, fileInfo := range sftpFileInfoList {
			rfi := NewRemoteFileInfo(filepath.Join(remotePath, fileInfo.Name()), fileInfo)
			remoteFileInfoList[index] = &rfi
		}
	}
	log.Debugf("Completed listing: %s", remotePath)

	return &agaveproto.FileInfoListResponse{
		Listing: remoteFileInfoList,
	}, time.Now().Sub(st).Seconds()
}

func rename(sftpClient *sftp.Client, oldRemotePath string, newRemotePath string) (*agaveproto.FileInfoResponse, float64) {
	log.Trace("Rename (Rename) GRPC service function was invoked ")
	st := time.Now()

	// stat the renamed path to catch permission and existence checks ahead of time
	log.Debugf("Checking existence of file item with new name %s", newRemotePath)
	remoteRenamedFileInfo, err := sftpClient.Stat(newRemotePath)
	if err != nil && !os.IsNotExist(err) {
		log.Errorf("Unable to stat renamed file, %s: %v", newRemotePath, err)
		return &agaveproto.FileInfoResponse{Error: err.Error()}, 0
	} else if err == nil {
		log.Errorf("file or dir exists")
		return &agaveproto.FileInfoResponse{Error: "file or dir exists"}, 0
	}

	// Stat the remote path
	log.Infof("Renaming %s to %s", oldRemotePath, newRemotePath)
	err = sftpClient.Rename(oldRemotePath, newRemotePath)
	if err != nil {
		log.Errorf("Unable to rename file %s to %s: %v", oldRemotePath, newRemotePath, err)
		return &agaveproto.FileInfoResponse{Error: err.Error()}, 0
	}

	// stat the remoete file to return the fileinfo data
	log.Debugf("Checking existence %s after rename", newRemotePath)
	remoteRenamedFileInfo, err = sftpClient.Stat(newRemotePath)
	if err != nil {
		log.Errorf("Unable to stat renamed file, %s: %v", newRemotePath, err)
		return &agaveproto.FileInfoResponse{Error: err.Error()}, 0
	}
	log.Debugf("Completed renaming %s to %s", oldRemotePath, newRemotePath)

	rfi := NewRemoteFileInfo(newRemotePath, remoteRenamedFileInfo)
	return &agaveproto.FileInfoResponse{
		RemoteFileInfo: &rfi,
	}, time.Now().Sub(st).Seconds()
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
