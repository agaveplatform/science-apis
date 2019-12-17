package sftprelay

import (
	"errors"
	"github.com/agaveplatform/science-apis/agave-transfers/sftp-relay/pkg/ssh_sftp_connection_pool"
	//ssh_sftp_connection_pool "github.com/agaveplatform/ssh_sftp_connection_pool"
	sftppb "github.com/agaveplatform/science-apis/agave-transfers/sftp-relay/pkg/sftpproto"
	"github.com/google/uuid"
	"github.com/pkg/sftp"
	"github.com/sirupsen/logrus"
	"golang.org/x/crypto/ssh"
	"golang.org/x/net/context"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
	//"time"
)

var log = logrus.New()

type Server struct{}

/*
	This is the central struct that is used by almost everything.
*/
type SysUri struct {
	Username     string
	PassWord     string
	SystemId     string
	HostKey      string
	HostPort     string
	ClientKey    string
	FileName     string
	FileSize     int64
	BufferSize   int64
	Type         string // Valid values are:  FTP, AZURE, S3, SWIFT, SFTP, GRIDFTP, IRODS, IRODS4, LOCAL
	DestFileName string
	SftpConn     *ssh.Client
	SftpClient   *sftp.Client
	Force        bool
}

// This defines the Srce type that contains the type defined above.
type ConnParams struct {
	Srce SysUri
}

// this is the Params that is references by the various applicatons
var Params ConnParams

//The Pool and AgentSocket variables are created here and instantiated in the init() function.
var Pool *ssh_sftp_connection_pool.SSHPool
var AgentSocket string
var requestId uuid.UUID

func init() {
	// Open a log file to log the server
	f, err := os.OpenFile("sftp-relay.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	wrt := io.MultiWriter(os.Stdout, f)
	log.SetOutput(wrt)

	/*
		Now set up the ssh connection pool
	*/
	//AgentSocket = "/var/folders/14/jjtrwj5x4zl2tp72ncljn6n40000gn/T//ssh-1t1VnKoFb1xv/agent.28756"
	AgentSocket, ok := os.LookupEnv("SSH_AUTH_SOCK")
	if !ok {
		log.Fatalln("Could not connect to SSH_AUTH_SOCK. Is ssh-agent running?")
	}
	log.Infof("SSH_AUTH_SOCK = %v", AgentSocket)

	// set the pool configuration.  The MaxConns sets the maximum connections for the pool.
	poolCfg := &ssh_sftp_connection_pool.PoolConfig{
		GCInterval: 5 * time.Second,
		MaxConns:   500,
	}
	// Create the new pool
	Pool = ssh_sftp_connection_pool.NewPool(poolCfg)

	log.Infof("Active connections: %d", Pool.ActiveConns())
}

func (*Server) Authenticate(ctx context.Context, req *sftppb.AuthenticateToRemoteRequest) (*sftppb.AuthenticateToRemoteResponse, error) {
	log.Printf("Get Service function was invoked with %v\n", req)

	/*
		This section will create a params local variable and get all the parameter from the request
	*/
	params := Params
	var result error
	var message string
	params, err := setGetAuthParams(req) // return type is ConnParams
	if err != nil {
		log.Errorf("Erorr retrieving conn paramteres %v", err)
		result = err
		message = err.Error()
	}

	// get the sftp connection from the pool.
	// -------------------------------------------------------------------------------------------------
	AgentSocket, ok := os.LookupEnv("SSH_AUTH_SOCK")
	if !ok {
		log.Fatalln("Could not connect to SSH_AUTH_SOCK. Is ssh-agent running?")
	}
	log.Infof("SSH_AUTH_SOCK = %v", AgentSocket)

	// get the sftp connection from the pool.
	// -------------------------------------------------------------------------------------------------
	log.Info("SSH config pool")

	s_port := strings.Replace(req.Auth.HostPort, ":", "", -1)
	port, _ := strconv.Atoi(s_port)
	sshCfg := &ssh_sftp_connection_pool.SSHConfig{
		User:               req.Auth.Username,
		Host:               req.Auth.SystemId,
		Port:               port,
		Auth:               []ssh.AuthMethod{ssh.Password(req.Auth.PassWord)},
		Timeout:            60 * time.Second,
		TCPKeepAlive:       true,
		TCPKeepAlivePeriod: 60 * time.Second,
		AgentSocket:        AgentSocket,
		ForwardAgent:       false,
		HostKeyCallback:    ssh.InsecureIgnoreHostKey(),
	}

	log.Infof("sshCfg = %s %s %d %v %v", sshCfg.User, sshCfg.Host, sshCfg.Port, sshCfg.Auth, sshCfg.AgentSocket)

	var MaxPcktSize int
	MaxPcktSize = int(params.Srce.BufferSize)
	sftpClient, sessionCount, err := Pool.ClaimClient(sshCfg, sftp.MaxPacket(MaxPcktSize))
	if err != nil {
		log.Errorf("Error with conncection = %v", err)
		return &sftppb.AuthenticateToRemoteResponse{Response: err.Error()}, nil
	}
	defer Pool.ReleaseClient(sshCfg)
	defer sftpClient.Close()

	log.Infof("Number of active connections: %d", sessionCount)
	params.Srce.SftpClient = sftpClient

	log.Printf("SysIP= %s \n", params.Srce.SystemId)
	log.Printf("SysPort= %s \n", params.Srce.HostPort)

	/*
		Now that everything is working we then return the call
	*/
	res := &sftppb.AuthenticateToRemoteResponse{
		Response: message,
	}
	log.Info(res.String())
	return res, result
}

// Performs a mkdirs operation on a remote host. Upon success, the created
// directory name is returned.
func (*Server) Mkdirs(ctx context.Context, req *sftppb.SrvMkdirsRequest) (*sftppb.SrvMkdirsResponse, error) {
	log.Printf("Mkdirs Service function was invoked with %v\n", req)

	params := Params
	var result error

	params, err := setMkdirsParams(req) // return type is ConnParams
	if err != nil {
		log.Errorf("Error with connection %v", err)
		result = err
	}

	// get the sftp connection from the pool.
	// -------------------------------------------------------------------------------------------------
	AgentSocket, ok := os.LookupEnv("SSH_AUTH_SOCK")
	if !ok {
		log.Fatalln("Could not connect to SSH_AUTH_SOCK. Is ssh-agent running?")
	}
	log.Infof("SSH_AUTH_SOCK = %v", AgentSocket)

	// get the sftp connection from the pool.
	// -------------------------------------------------------------------------------------------------
	log.Info("SSH config pool")

	s_port := strings.Replace(req.SrceSftp.HostPort, ":", "", -1)
	port, _ := strconv.Atoi(s_port)
	sshCfg := &ssh_sftp_connection_pool.SSHConfig{
		User:               req.SrceSftp.Username,
		Host:               req.SrceSftp.SystemId,
		Port:               port,
		Auth:               []ssh.AuthMethod{ssh.Password(req.SrceSftp.PassWord)},
		Timeout:            30 * time.Second,
		TCPKeepAlive:       false,
		TCPKeepAlivePeriod: 30 * time.Second,
		AgentSocket:        AgentSocket,
		ForwardAgent:       false,
		HostKeyCallback:    ssh.InsecureIgnoreHostKey(),
	}

	log.Infof("sshCfg = %s %s %d %v %v", sshCfg.User, sshCfg.Host, sshCfg.Port, sshCfg.Auth, sshCfg.AgentSocket)

	var MaxPcktSize int
	MaxPcktSize = int(params.Srce.BufferSize)
	sftpClient, sessionCount, err := Pool.ClaimClient(sshCfg, sftp.MaxPacket(MaxPcktSize))
	if err != nil {
		log.Errorf("Error with conncection = %v", err)
		return &sftppb.SrvMkdirsResponse{FileName: "", Error: err.Error()}, nil
	}
	defer Pool.ReleaseClient(sshCfg)
	defer sftpClient.Close()
	log.Infof("Number of active connections: %d", sessionCount)
	params.Srce.SftpClient = sftpClient

	var tmpName FileTransfer

	// Read From system
	tmpName = GetSourceType(params, params.Srce.Type+"-Mkdirs")

	res := &sftppb.SrvMkdirsResponse{
		FileName: tmpName.Name,
		Error:    tmpName.e.Error(),
	}
	log.Info(res.String())
	return res, result
}

func (*Server) Stat(ctx context.Context, req *sftppb.SrvStatRequest) (*sftppb.SrvStatResponse, error) {
	log.Printf("Stat Service function was invoked with %v\n", req)

	params := Params
	var result error

	params, err := setStatParams(req) // return type is ConnParams
	if err != nil {
		log.Errorf("Error with connection %v", err)
		result = err
	}

	// get the sftp connection from the pool.
	// -------------------------------------------------------------------------------------------------
	AgentSocket, ok := os.LookupEnv("SSH_AUTH_SOCK")
	if !ok {
		log.Fatalln("Could not connect to SSH_AUTH_SOCK. Is ssh-agent running?")
	}
	log.Infof("SSH_AUTH_SOCK = %v", AgentSocket)

	// get the sftp connection from the pool.
	// -------------------------------------------------------------------------------------------------
	log.Info("SSH config pool")

	s_port := strings.Replace(req.SrceSftp.HostPort, ":", "", -1)
	port, _ := strconv.Atoi(s_port)
	sshCfg := &ssh_sftp_connection_pool.SSHConfig{
		User:               req.SrceSftp.Username,
		Host:               req.SrceSftp.SystemId,
		Port:               port,
		Auth:               []ssh.AuthMethod{ssh.Password(req.SrceSftp.PassWord)},
		Timeout:            30 * time.Second,
		TCPKeepAlive:       false,
		TCPKeepAlivePeriod: 30 * time.Second,
		AgentSocket:        AgentSocket,
		ForwardAgent:       false,
		HostKeyCallback:    ssh.InsecureIgnoreHostKey(),
	}

	log.Infof("sshCfg = %s %s %d %v %v", sshCfg.User, sshCfg.Host, sshCfg.Port, sshCfg.Auth, sshCfg.AgentSocket)

	var MaxPcktSize int
	MaxPcktSize = int(params.Srce.BufferSize)
	sftpClient, sessionCount, err := Pool.ClaimClient(sshCfg, sftp.MaxPacket(MaxPcktSize))
	if err != nil {
		log.Errorf("Error with conncection = %v", err)
		return &sftppb.SrvStatResponse{FileName: "", Error: err.Error()}, nil
	}

	defer Pool.ReleaseClient(sshCfg)
	defer sftpClient.Close()
	log.Infof("Number of active connections: %d", sessionCount)
	params.Srce.SftpClient = sftpClient

	// Read From system
	statTxfrFactory := SFTP_Stat_Factory{}
	res := statTxfrFactory.Stat(params)

	log.Info(res.String())
	return res, result
}

// Removes the remote path. If the remote path is a directory,
// the entire tree is deleted.
func (*Server) Remove(ctx context.Context, req *sftppb.SrvRemoveRequest) (*sftppb.SrvRemoveResponse, error) {
	log.Printf("Remove Service function was invoked with %v\n", req)

	params := Params
	var result error
	params, err := setRemoveParams(req) // return type is ConnParams
	if err != nil {
		log.Errorf("Error with connection %v", err)
		result = err
	}

	// get the sftp connection from the pool.
	// -------------------------------------------------------------------------------------------------
	AgentSocket, ok := os.LookupEnv("SSH_AUTH_SOCK")
	if !ok {
		log.Fatalln("Could not connect to SSH_AUTH_SOCK. Is ssh-agent running?")
	}
	log.Infof("SSH_AUTH_SOCK = %v", AgentSocket)

	// get the sftp connection from the pool.
	// -------------------------------------------------------------------------------------------------
	log.Info("SSH config pool")

	s_port := strings.Replace(req.SrceSftp.HostPort, ":", "", -1)
	port, _ := strconv.Atoi(s_port)
	sshCfg := &ssh_sftp_connection_pool.SSHConfig{
		User:               req.SrceSftp.Username,
		Host:               req.SrceSftp.SystemId,
		Port:               port,
		Auth:               []ssh.AuthMethod{ssh.Password(req.SrceSftp.PassWord)},
		Timeout:            30 * time.Second,
		TCPKeepAlive:       false,
		TCPKeepAlivePeriod: 30 * time.Second,
		AgentSocket:        AgentSocket,
		ForwardAgent:       false,
		HostKeyCallback:    ssh.InsecureIgnoreHostKey(),
	}

	log.Infof("sshCfg = %s %s %d %v %v", sshCfg.User, sshCfg.Host, sshCfg.Port, sshCfg.Auth, sshCfg.AgentSocket)

	var MaxPcktSize int
	MaxPcktSize = int(params.Srce.BufferSize)
	sftpClient, sessionCount, err := Pool.ClaimClient(sshCfg, sftp.MaxPacket(MaxPcktSize))
	if err != nil {
		log.Errorf("Error with conncection = %v", err)
		return &sftppb.SrvRemoveResponse{FileName: "", Error: err.Error()}, nil
	}
	defer Pool.ReleaseClient(sshCfg)
	defer sftpClient.Close()
	log.Infof("Number of active connections: %d", sessionCount)
	params.Srce.SftpClient = sftpClient

	var tmpName FileTransfer

	// Read From system
	tmpName = GetSourceType(params, Params.Srce.Type+"-Remove")

	res := &sftppb.SrvRemoveResponse{
		FileName: tmpName.Name,
		Error:    tmpName.e.Error(),
	}
	log.Info(res.String())
	return res, result
}

/*
	This service gets a single request for a file from an SFTP server.  It goes to the server and sends back the file in
	chunks sized based on the buffer_size variable.
*/
func (*Server) Get(ctx context.Context, req *sftppb.SrvGetRequest) (*sftppb.SrvGetResponse, error) {
	log.Printf("Get Service function was invoked with %v\n", req)
	//instanceUuid := uuid.New()
	//log.Infof("begin uuid = %s", instanceUuid)
	//defer log.Infof("end uuid = %s", instanceUuid)

	params := Params
	var result error

	params, err := setGetParams(req) // return type is ConnParams
	if err != nil {
		log.Errorf("Error with connection %v", err)
		result = err
	}
	// get the sftp connection from the pool.
	// -------------------------------------------------------------------------------------------------
	AgentSocket, ok := os.LookupEnv("SSH_AUTH_SOCK")
	if !ok {
		log.Fatalln("Could not connect to SSH_AUTH_SOCK. Is ssh-agent running?")
	}
	log.Infof("SSH_AUTH_SOCK = %v", AgentSocket)

	// get the sftp connection from the pool.
	// -------------------------------------------------------------------------------------------------
	log.Info("SSH config pool")

	s_port := strings.Replace(req.SrceSftp.HostPort, ":", "", -1)
	port, _ := strconv.Atoi(s_port)
	sshCfg := &ssh_sftp_connection_pool.SSHConfig{
		User:               req.SrceSftp.Username,
		Host:               req.SrceSftp.SystemId,
		Port:               port,
		Auth:               []ssh.AuthMethod{ssh.Password(req.SrceSftp.PassWord)},
		Timeout:            60 * time.Second,
		TCPKeepAlive:       true,
		TCPKeepAlivePeriod: 60 * time.Second,
		AgentSocket:        AgentSocket,
		ForwardAgent:       false,
		HostKeyCallback:    ssh.InsecureIgnoreHostKey(),
	}

	log.Infof("sshCfg = %s %s %d %v %v", sshCfg.User, sshCfg.Host, sshCfg.Port, sshCfg.Auth, sshCfg.AgentSocket)

	var MaxPcktSize int
	MaxPcktSize = int(params.Srce.BufferSize)
	sftpClient, sessionCount, err := Pool.ClaimClient(sshCfg, sftp.MaxPacket(MaxPcktSize))
	if err != nil {
		log.Errorf("Error with connection = %v", err)
		return &sftppb.SrvGetResponse{FileName: "", BytesReturned: "0", Error: err.Error()}, nil
	}
	params.Srce.SftpClient = sftpClient
	defer Pool.ReleaseClient(sshCfg)
	//defer sftpClient.Close()

	log.Infof("Number of active connections: %d", sessionCount)

	// we assume that the calling functions have already established that the system has the ability to make a connection.
	// This is assuming that we are doing SFTP right now.  This will change in the future to support other file systems

	//var bytesReadf int64 = 0
	var tmpName FileTransfer

	// Read From system
	log.Infof("calling GetSourceType(%v): ", params.Srce.FileName)

	// Write to local system from a remote file system
	tmpName = GetSourceType(params, params.Srce.Type+"-Get")
	log.Println(params.Srce.Type + "-Get")

	log.Printf("BytesWritten: %d", tmpName.i)
	log.Println("File Name = " + tmpName.Name)
	log.Println("Error : " + tmpName.e.Error())
	log.Infof("%d bytes copied\n", tmpName.i)
	res := &sftppb.SrvGetResponse{
		FileName:      tmpName.Name,
		BytesReturned: strconv.FormatInt(tmpName.i, 10),
		Error:         tmpName.e.Error(),
	}

	sftpClient.Close()

	log.Info(res.String())
	//defer log.Infof("end uuid = ", instanceUuid)
	return res, result
}

func (*Server) Put(ctx context.Context, req *sftppb.SrvPutRequest) (*sftppb.SrvPutResponse, error) {
	log.Infof("Put function was invoked with %v\n", req)

	params := Params
	var result error

	params, err := setPutParams(req) // return type is ConnParams
	if err != nil {
		log.Errorf("Error with connection %v", err)
		log.Error("Host : ", params.Srce.SystemId)
		log.Error("Host : ", params.Srce.HostPort)
		return &sftppb.SrvPutResponse{FileName: "", BytesReturned: "0", Error: err.Error()}, nil
	}

	// get the sftp connection from the pool.
	// -------------------------------------------------------------------------------------------------
	AgentSocket, ok := os.LookupEnv("SSH_AUTH_SOCK")
	if !ok {
		log.Fatalln("Could not connect to SSH_AUTH_SOCK. Is ssh-agent running?")
	}
	log.Infof("SSH_AUTH_SOCK = %s", AgentSocket)

	// get the sftp connection from the pool.
	// -------------------------------------------------------------------------------------------------
	log.Info("SSH config pool")

	s_port := strings.Replace(req.SrceSftp.HostPort, ":", "", -1)
	port, _ := strconv.Atoi(s_port)
	sshCfg := &ssh_sftp_connection_pool.SSHConfig{
		User:               req.SrceSftp.Username,
		Host:               req.SrceSftp.SystemId,
		Port:               port,
		Auth:               []ssh.AuthMethod{ssh.Password(req.SrceSftp.PassWord)},
		Timeout:            30 * time.Second,
		TCPKeepAlive:       true,
		TCPKeepAlivePeriod: 30 * time.Second,
		AgentSocket:        AgentSocket,
		ForwardAgent:       false,
		HostKeyCallback:    ssh.InsecureIgnoreHostKey(),
	}

	log.Infof("sshCfg = %s %s %d %v %v", sshCfg.User, sshCfg.Host, sshCfg.Port, sshCfg.Auth, sshCfg.AgentSocket)

	var MaxPcktSize int
	MaxPcktSize = int(params.Srce.BufferSize)
	sftpClient, sessionCount, err := Pool.ClaimClient(sshCfg, sftp.MaxPacket(MaxPcktSize))

	if err != nil {
		log.Errorf("Error with conncection = %v", err)
		return &sftppb.SrvPutResponse{FileName: "", BytesReturned: "0", Error: err.Error()}, nil
	}
	defer Pool.ReleaseClient(sshCfg)
	defer sftpClient.Close()

	params.Srce.SftpClient = sftpClient
	log.Info("ouputing pool info")
	log.Infof("Number of active connections: %d", sessionCount)

	// now assign the ssh connection to the sftp
	log.Println("Create new SFTP client")
	// -------------------------------------------------------------------------------------------------

	// we assume that the calling functions have already established that the system has the ability to make a connection.
	// This is assuming that we are doing SFTP right now.  This will change in the future to support other file systems
	//
	//var filename string
	//log.Info(filename)

	//var bytesReadf int64 = 0
	var tmpName FileTransfer

	// Read From system
	log.Infof("calling GetSourceType(%v): ", params.Srce.FileName)

	// Get file from remote file system To a file
	tmpName = GetSourceType(params, params.Srce.Type+"-Put")
	log.Println(params.Srce.Type + "-Put")

	log.Printf("BytesWritten: %d", tmpName.i)
	log.Println("File Name = " + tmpName.Name)
	log.Println("Error : " + tmpName.e.Error())
	log.Infof("%d bytes copied\n", tmpName.i)
	res := &sftppb.SrvPutResponse{
		FileName:      tmpName.Name,
		BytesReturned: strconv.FormatInt(tmpName.i, 10),
		Error:         tmpName.e.Error(),
	}
	log.Info(res.String())
	return res, result
}

/*
Each one of the 5 setParams takes the specific request "req" and loads the ConnParams which is then returned to the
various functions.

Note we could move these back into the functions.  I had originally thought of making one call for all the parameters but
I dont think that will work without more coding.
*/
func setMkdirsParams(req *sftppb.SrvMkdirsRequest) (ConnParams, error) {
	/*
		This will take the various parameters of the req and return an sftp connection.  It will check the connection pool
		for the sftp connection before making another connection.

		It should speed up multi file performance significantly.
	*/
	var result error
	connParams := ConnParams{
		Srce: SysUri{Username: req.SrceSftp.Username,
			PassWord:     req.SrceSftp.PassWord,
			SystemId:     req.SrceSftp.SystemId,
			HostKey:      req.SrceSftp.HostKey,
			HostPort:     req.SrceSftp.HostPort,
			ClientKey:    req.SrceSftp.ClientKey,
			FileName:     req.SrceSftp.FileName,
			FileSize:     req.SrceSftp.FileSize,
			BufferSize:   req.SrceSftp.BufferSize,
			Type:         req.SrceSftp.Type,
			DestFileName: req.SrceSftp.DestFileName,
			Force:        req.SrceSftp.Force,
		},
	}
	return connParams, result
}
func setRemoveParams(req *sftppb.SrvRemoveRequest) (ConnParams, error) {
	/*
		This will take the various parameters of the req and return an sftp connection.  It will check the connection pool
		for the sftp connection before making another connection.

		It should speed up multi file performance significantly.
	*/
	var result error

	connParams := ConnParams{
		Srce: SysUri{Username: req.SrceSftp.Username,
			PassWord:     req.SrceSftp.PassWord,
			SystemId:     req.SrceSftp.SystemId,
			HostKey:      req.SrceSftp.HostKey,
			HostPort:     req.SrceSftp.HostPort,
			ClientKey:    req.SrceSftp.ClientKey,
			FileName:     req.SrceSftp.FileName,
			FileSize:     req.SrceSftp.FileSize,
			BufferSize:   req.SrceSftp.BufferSize,
			Type:         req.SrceSftp.Type,
			DestFileName: req.SrceSftp.DestFileName,
			Force:        req.SrceSftp.Force,
		},
	}
	return connParams, result
}
func setGetParams(req *sftppb.SrvGetRequest) (ConnParams, error) {
	/*
		This will take the various parameters of the req and return an sftp connection.  It will check the connection pool
		for the sftp connection before making another connection.

		It should speed up multi file performance significantly.
	*/
	var result error

	connParams := ConnParams{
		Srce: SysUri{Username: req.SrceSftp.Username,
			PassWord:     req.SrceSftp.PassWord,
			SystemId:     req.SrceSftp.SystemId,
			HostKey:      req.SrceSftp.HostKey,
			HostPort:     req.SrceSftp.HostPort,
			ClientKey:    req.SrceSftp.ClientKey,
			FileName:     req.SrceSftp.FileName,
			FileSize:     req.SrceSftp.FileSize,
			BufferSize:   req.SrceSftp.BufferSize,
			Type:         req.SrceSftp.Type,
			DestFileName: req.SrceSftp.DestFileName,
			Force:        req.SrceSftp.Force,
		},
	}
	return connParams, result
}
func setPutParams(req *sftppb.SrvPutRequest) (ConnParams, error) {
	/*
		This will take the various parameters of the req and return an sftp connection.  It will check the connection pool
		for the sftp connection before making another connection.

		It should speed up multi file performance significantly.
	*/
	var result error

	connParams := ConnParams{
		Srce: SysUri{Username: req.SrceSftp.Username,
			PassWord:     req.SrceSftp.PassWord,
			SystemId:     req.SrceSftp.SystemId,
			HostKey:      req.SrceSftp.HostKey,
			HostPort:     req.SrceSftp.HostPort,
			ClientKey:    req.SrceSftp.ClientKey,
			FileName:     req.SrceSftp.FileName,
			FileSize:     req.SrceSftp.FileSize,
			BufferSize:   req.SrceSftp.BufferSize,
			Type:         req.SrceSftp.Type,
			DestFileName: req.SrceSftp.DestFileName,
			Force:        req.SrceSftp.Force,
		},
	}
	return connParams, result
}
func setGetAuthParams(req *sftppb.AuthenticateToRemoteRequest) (ConnParams, error) {
	/*
		This will take the various parameters of the req and return an sftp connection.  It will check the connection pool
		for the sftp connection before making another connection.

		It should speed up multi file performance sigificantly.
	*/
	var result error

	connParams := ConnParams{Srce: SysUri{
		Username:     req.Auth.Username,
		PassWord:     req.Auth.PassWord,
		SystemId:     req.Auth.SystemId,
		HostKey:      req.Auth.HostKey,
		HostPort:     req.Auth.HostPort,
		ClientKey:    req.Auth.ClientKey,
		FileName:     req.Auth.FileName,
		FileSize:     req.Auth.FileSize,
		BufferSize:   req.Auth.BufferSize,
		Type:         req.Auth.Type,
		DestFileName: req.Auth.DestFileName,
		Force:        req.Auth.Force,
	}}
	return connParams, result
}
func setStatParams(req *sftppb.SrvStatRequest) (ConnParams, error) {
	/*
		This will take the various parameters of the req and return an sftp connection.  It will check the connection pool
		for the sftp connection before making another connection.

		It should speed up multi file performance significantly.
	*/
	var result error
	connParams := ConnParams{
		Srce: SysUri{Username: req.SrceSftp.Username,
			PassWord:     req.SrceSftp.PassWord,
			SystemId:     req.SrceSftp.SystemId,
			HostKey:      req.SrceSftp.HostKey,
			HostPort:     req.SrceSftp.HostPort,
			ClientKey:    req.SrceSftp.ClientKey,
			FileName:     req.SrceSftp.FileName,
			FileSize:     req.SrceSftp.FileSize,
			BufferSize:   req.SrceSftp.BufferSize,
			Type:         req.SrceSftp.Type,
			DestFileName: req.SrceSftp.DestFileName,
			Force:        req.SrceSftp.Force,
		},
	}
	return connParams, result
}

/* ***************************************************************************************
Factory
*/

type FileTransfer struct {
	Name string
	i    int64
	e    error
}
type FileItemInfo struct {
	Name        string
	Size        int64
	Mode        string
	LastUpdated int64
	IsDirectory bool
	IsLink      bool
}
type ReadTransferFactory interface {
	ReadFrom(params ConnParams) FileTransfer
}
type WriteTransferFactory interface {
	WriteTo(params ConnParams) FileTransfer
}
type MkdirsTransferFactory interface {
	Mkdirs(params ConnParams) FileTransfer
}
type StatTransferFactory interface {
	Stat(params ConnParams) FileTransfer
}
type RemoveTransferFactory interface {
	Remove(params ConnParams) FileTransfer
}
type SFTP_ReadFrom_Factory struct{}
type SFTP_WriteTo_Factory struct{}
type SFTP_Mkdirs_Factory struct{}
type SFTP_Stat_Factory struct{}
type SFTP_Remove_Factory struct{}

// main factory method
func GetSourceType(params ConnParams, source string) FileTransfer {
	log.Info("Got into the GetSourceType function")
	var readTxfrFactory ReadTransferFactory
	var writeTxfrFactory WriteTransferFactory
	var mkdirsTxfrFactory MkdirsTransferFactory
	var removeTxfrFactory RemoveTransferFactory
	//source := params.Srce.Type

	switch source {
	case "SFTP-Get":
		log.Info("Calling the SFTP Get class")
		readTxfrFactory = SFTP_ReadFrom_Factory{}
		return readTxfrFactory.ReadFrom(params)
	case "SFTP-Put":
		log.Println("Calling the SFTP Put class")
		log.Info("Calling the SFTP Put class")
		writeTxfrFactory = SFTP_WriteTo_Factory{}
		return writeTxfrFactory.WriteTo(params)
	case "SFTP-Mkdirs":
		log.Println("Calling the SFTP Mkdirs class")
		log.Info("Calling the SFTP Mkdirs class")
		mkdirsTxfrFactory = SFTP_Mkdirs_Factory{}
		return mkdirsTxfrFactory.Mkdirs(params)
	case "SFTP-Remove":
		log.Println("Calling the SFTP Remove class")
		log.Info("Calling the SFTP Remove class")
		removeTxfrFactory = SFTP_Remove_Factory{}
		return removeTxfrFactory.Remove(params)
	default:
		log.Println("No classes were called")
		log.Info("No classes were called.")
		return FileTransfer{"", 0, errors.New("No classes were called")}
	}
}

//*****************************************************************************************

/*
The last sections each implement an SFTP process.
*/

/*
The assumption is that we are going to copy a file from a remote server to a local file.
This is synonymous to a get for sftp
*/

func (a SFTP_ReadFrom_Factory) ReadFrom(params ConnParams) FileTransfer {
	// log all the parameters
	log.Println("SFTP read from (GET) txfr")
	log.Println("User name: " + params.Srce.Username)
	log.Println("Password: " + params.Srce.PassWord)
	log.Println("System = " + params.Srce.SystemId)
	log.Println("Port = " + params.Srce.HostPort)
	log.Println("File name: " + params.Srce.FileName)

	// get the connection from the pool now
	result := ""

	log.Printf("SysIP = %s ", params.Srce.SystemId)
	log.Printf("SysPort = %s \n", params.Srce.HostPort)
	//var conn *ssh.Client

	// get the MaxPcktSize (BufferSize)
	var MaxPcktSize int
	MaxPcktSize = int(params.Srce.BufferSize)

	log.Printf("Packet size is: %v \n", MaxPcktSize)
	// create new SFTP client on the specified connection
	client := params.Srce.SftpClient
	//if client == nil {
	//var err error
	//client, err := sftp.NewClient(params.Srce.SftpConn, sftp.MaxPacket(MaxPcktSize))
	//if err != nil {
	//	log.Errorf("Error creating new client, %v \n", err)
	//	result = err.Error()
	//	return FileTransfer{"", 0, err}
	//}
	//defer client.Close()
	////}
	log.Println("Connection Data:  " + params.Srce.Username + "  " + params.Srce.SystemId + params.Srce.HostPort)

	// check if the file already exists. If is  does not and Force = false then error. Otherwise remove the
	//file first then do everything else
	fileExists := fileExistsLocal(params.Srce.DestFileName)
	// Create the destination file
	if (fileExists && params.Srce.Force) || !fileExists {
		if fileExists {
			err := os.Remove(params.Srce.DestFileName)
			if err != nil {
				log.Errorf("Error removing file %v", err)
			}
		}
		log.Infof("Create destination file %s", params.Srce.FileName)
		dstFile, err := os.OpenFile(params.Srce.DestFileName, os.O_RDWR|os.O_CREATE, 0755) //local file
		if err != nil {
			log.Errorf("E"+
				"Error creating 1 dest file. %v \n", err)
			result = err.Error()
			return FileTransfer{"", 0, err}
		}
		defer dstFile.Close()
		destFstat, err := os.Stat(params.Srce.DestFileName)
		if err != nil {
			log.Info("file info: ", err)
		}
		log.Info("Dest file stat: ", destFstat.Size())

		// Now open the source file and read the contents.
		log.Printf("Open source file %v \n", params.Srce.FileName)
		srcFile, err := client.Open(params.Srce.FileName)
		if err != nil {
			log.Errorf("Opening source 1 file: %v \n", err)
			result = err.Error()
			return FileTransfer{"", 0, err}
		}
		fileStat, err := os.Stat(params.Srce.FileName)
		if err != nil {
			log.Info("file info: ", err)
		}
		log.Info("Srce file stat: ", fileStat.Size())

		defer srcFile.Close()
		log.Printf("Source file is opened. %s", params.Srce.FileName)

		//const size = 1e10
		//Get the file stats.
		fsize, err := srcFile.Stat()
		if err != nil {
			log.Info("File size error = ", err)
		}
		log.Info("File stats: ", fsize)
		fileSize := uint64(fsize.Size())
		log.Infof("File name = %s  File size = %v ", dstFile.Name(), fileSize)

		var bytesWritten int64
		bytesWritten = 0
		if fileSize < 100000 {
			// copy source file to destination file
			bytesWritten, err = io.Copy(dstFile, srcFile)
			if err != nil {
				log.Error(err)
			}
			log.Printf(" file size < 100000  %d bytes copied\n", bytesWritten)

			// flush in-memory copy
			err = dstFile.Sync()
			if err != nil {
				log.Error(err)
			}
			log.Info("Memory has been flushed 0")
			dstFile.Close()
		} else {
			//// Write the contents to the destination.
			bytesWritten, err = srcFile.WriteTo(dstFile)
			if err != nil {
				log.Errorf("Error writing 1 to file: %v \n", err)
				result = err.Error()
				return FileTransfer{"", 0, err}
			}
			err = dstFile.Sync()
			if err != nil {
				log.Error(err)
				result = err.Error()
				return FileTransfer{"", 0, err}
			}

			dstFile.Close()

			log.Info("Memory has been flushed 1")
			log.Infof("%d bytes copied\n", bytesWritten)
		}
		srcFile.Close()

		// If everything worked ok return the DestFileName, bytesWritten, and any Errors, with should be empty
		return FileTransfer{params.Srce.DestFileName, int64(bytesWritten), errors.New(result)}
	} else {
		return FileTransfer{params.Srce.DestFileName, 0, errors.New("file already exists")}
	}

}

func (a SFTP_WriteTo_Factory) WriteTo(params ConnParams) FileTransfer {
	log.Info("WriteTo (PUT) sFTP Service function was invoked ")

	// write the parmas out to the log file
	log.Println("User name: " + params.Srce.Username)
	log.Println("Password: " + params.Srce.PassWord)
	log.Println("System = " + params.Srce.SystemId)
	log.Println("Port = " + params.Srce.HostPort)
	log.Println("File name: " + params.Srce.FileName)

	// Make the ssh/sftp connection
	log.Println("Make the ssh/sftp connection")
	result := ""

	var MaxPcktSize int
	MaxPcktSize = int(params.Srce.BufferSize)

	client := params.Srce.SftpClient
	log.Info("Client =", client)
	log.Println("Create new SFTP client")
	if client == nil {
		client, err := sftp.NewClient(params.Srce.SftpConn, sftp.MaxPacket(MaxPcktSize))
		if err != nil {
			log.Println("Error creating new client", err)
			return FileTransfer{"", 0, err}
		}
		defer client.Close()
	}

	log.Println("Connection Data:  " + params.Srce.Username + "  " + params.Srce.SystemId + params.Srce.HostPort)

	// If the connection works then the next step is to create the dest file
	log.Println("Create destination file")
	log.Info("DestFileName = " + params.Srce.DestFileName)

	//###################################################################################################################
	// check if the file already exists. If is  does not and Force = false then error. Otherwise remove the
	//file first then do everything else
	fileExists := fileExistsRemote(params.Srce.DestFileName, client)
	// Create the destination file
	if (fileExists && params.Srce.Force) || !fileExists {
		if fileExists {
			err := os.Remove(params.Srce.DestFileName)
			if err != nil {
				log.Errorf("Error removing file %v", err)
			}
		}

		// now that the dest file is created the next step is to open the source file
		log.Println("Open source file: " + params.Srce.FileName)
		// note this should include the /scratch directory
		s := params.Srce.FileName
		log.Info(strings.Trim(s, "\""))

		srceFile, err := os.Open(params.Srce.FileName)
		if err != nil {
			log.Errorf("Opening source file: %v \n", err)
			result = err.Error()
			return FileTransfer{"", 0, err}
		}

		dstFile, err := client.OpenFile(params.Srce.DestFileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC)
		if err != nil {
			log.Errorf("Error creating dest file. %s. %v \n", params.Srce.DestFileName, err)
			result = err.Error()
			return FileTransfer{"", 0, err}
		}

		var bytesWritten int64
		// if there were no error opening the two files then process the file.
		if result == "" {
			log.Println("Write to dstFile: " + dstFile.Name())

			// this will write the file BufferSize blocks until EOF is returned.
			bytesWritten, err = dstFile.ReadFrom(srceFile)
			if err != nil {
				log.Errorf("Error writing to file: %v \n", err)
				result = err.Error()
			}
			log.Println("Name: " + dstFile.Name())
			log.Println(bytesWritten)
			err = errors.New(result)
			log.Printf("%d bytes copied\n", bytesWritten)
			log.Println("Errors: " + err.Error())
			return FileTransfer{dstFile.Name(), bytesWritten, err}
		}
		err = srceFile.Sync()
		if err != nil {
			log.Error(err)
			result = err.Error()
			return FileTransfer{"", 0, err}
		}
		srceFile.Close()

		log.Println(result)
		err = errors.New(result)
		log.Errorf("There was an error with the transfer.  Err = %v \n", err)
		return FileTransfer{"", 0, err}
	} else {
		return FileTransfer{params.Srce.DestFileName, 0, errors.New("file already exists")}
	}
}

func (a SFTP_Mkdirs_Factory) Mkdirs(params ConnParams) FileTransfer {
	log.Info("Mkdirs sFTP Service function was invoked ")

	// log the parameters
	log.Println("User name: " + params.Srce.Username)
	log.Println("Password: " + params.Srce.PassWord)
	log.Println("System = " + params.Srce.SystemId)
	log.Println("Port = " + params.Srce.HostPort)
	log.Println("Dest File name: " + params.Srce.DestFileName)

	//var config ssh.ClientConfig
	log.Println("Make the ssh/sftp connection")

	// Create the new sftp client from the pool
	result := ""
	//log.Println("Create new SFTP client")
	//client, err := sftp.NewClient(params.Srce.SftpConn)
	//if err != nil {
	//	log.Println("Error creating new client", err)
	//	return FileTransfer{"", 0, err}
	//}
	//defer client.Close()
	client := params.Srce.SftpClient
	log.Println("Connection Data:  " + params.Srce.Username + "  " + params.Srce.SystemId + params.Srce.HostPort)

	// Create destination directory
	log.Println("Create destination directory")
	err := client.MkdirAll(params.Srce.DestFileName)
	if err != nil {
		log.Errorf("Error creating directory. %s. %v \n", params.Srce.DestFileName, err)
		result = err.Error()
		return FileTransfer{"", 0, err}
	}
	defer client.Close()

	// if there were no error opening the two files then process them
	if result == "" {
		log.Println("Created destination directory: " + params.Srce.DestFileName)
		err = errors.New(result)
		return FileTransfer{params.Srce.DestFileName, 0, err}
	} else {
		// TODO this should be unreachable by the function.  We should confirm and remove if it is not used.
		log.Println(result)
		err = errors.New(result)
		log.Errorf("There was an error creating the directory.  Err = %v \n", err)
		return FileTransfer{"", 0, err}
	}
}

func (a SFTP_Stat_Factory) Stat(params ConnParams) *sftppb.SrvStatResponse {
	log.Info("Stat sFTP Service function was invoked ")

	// log the parameters
	log.Println("User name: " + params.Srce.Username)
	log.Println("Password: " + params.Srce.PassWord)
	log.Println("System = " + params.Srce.SystemId)
	log.Println("Port = " + params.Srce.HostPort)
	log.Println("Dest File name: " + params.Srce.DestFileName)

	//var config ssh.ClientConfig
	log.Println("Make the ssh/sftp connection")

	// Create the new sftp client from the pool

	//log.Println("Create new SFTP client")
	//client, err := sftp.NewClient(params.Srce.SftpConn)
	//if err != nil {
	//	log.Println("Error creating new client", err)
	//	return FileTransfer{"", 0, err}
	//}
	//defer client.Close()
	client := params.Srce.SftpClient
	log.Println("Connection Data:  " + params.Srce.Username + "  " + params.Srce.SystemId + params.Srce.HostPort)

	// Create destination directory
	fstat, err := client.Stat(params.Srce.DestFileName)

	if err != nil {
		log.Infof("Unable to stat file %v", params.Srce.DestFileName)
		return &sftppb.SrvStatResponse{
			FileName: params.Srce.DestFileName,
			Error:    err.Error(),
		}
	}

	log.Infof("File stats: %v", fstat)
	return &sftppb.SrvStatResponse{
		FileName:    fstat.Name(),
		Size:        fstat.Size(),
		Mode:        fstat.Mode().String(),
		LastUpdated: fstat.ModTime().Unix(),
		IsDirectory: fstat.Mode().IsDir(),
		IsLink:      (fstat.Mode().IsRegular() && !fstat.Mode().IsDir()),
		Error:       "",
	}

}

func (a SFTP_Remove_Factory) Remove(params ConnParams) FileTransfer {
	log.Info("Remove sFTP Service function was invoked ")

	// log all the params
	log.Println("User name: " + params.Srce.Username)
	log.Println("Password: " + params.Srce.PassWord)
	log.Println("System = " + params.Srce.SystemId)
	log.Println("Port = " + params.Srce.HostPort)
	log.Println("Dest File name: " + params.Srce.DestFileName)

	//var config ssh.ClientConfig
	log.Println("Make the ssh/sftp connection")

	result := ""

	//// Create the new sftp client from the pool
	//log.Println("Create new SFTP client")
	//client, err := sftp.NewClient(params.Srce.SftpConn)
	//if err != nil {
	//	log.Println("Error creating new client", err)
	//	return FileTransfer{"", 0, err}
	//}
	//defer client.Close()
	client := params.Srce.SftpClient
	log.Println("Connection Data:  " + params.Srce.Username + "  " + params.Srce.SystemId + params.Srce.HostPort)

	var stat os.FileInfo

	stat, err := client.Stat(params.Srce.DestFileName)
	if err != nil {
		log.Errorf("Unable to stat. %s. %v \n", params.Srce.DestFileName, err)
		result = err.Error()
		return FileTransfer{"", 0, err}
	}
	log.Println("starting IsDir")
	if stat.IsDir() {
		log.Printf("Deleting destination directory %s", params.Srce.DestFileName)
		err = client.RemoveDirectory(params.Srce.DestFileName)
	} else {
		log.Printf("Deleting destination file %s", params.Srce.DestFileName)
		err = client.Remove(params.Srce.DestFileName)
	}
	if err != nil {
		log.Errorf("Error deleting %s. %v \n", params.Srce.DestFileName, err)
		result = err.Error()
		return FileTransfer{"", 0, err}
	}
	defer client.Close()

	// if there were no error opening the two files then process them
	if result == "" {
		log.Printf("Deleted destination: %s", params.Srce.DestFileName)
		err = errors.New(result)
		return FileTransfer{params.Srce.DestFileName, 0, err}
	} else {
		log.Println(result)
		err = errors.New(result)
		log.Errorf("Unexpected error occurred.  Err = %v \n", err)
		return FileTransfer{"", 0, err}
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

//
func fileExistsLocal(filename string) bool {
	_, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return true
}
func fileExistsRemote(filename string, client *sftp.Client) bool {
	_, err := client.Stat(filename)
	if !errors.Is(err, sftp.ErrSshFxNoSuchFile) {
		return false
	}
	return true

}
