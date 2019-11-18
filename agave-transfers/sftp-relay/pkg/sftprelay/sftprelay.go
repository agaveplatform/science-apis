package sftprelay

import (
	"errors"
	sftppb "github.com/agaveplatform/science-apis/agave-transfers/sftp-relay/pkg/sftpproto"
	"github.com/pkg/sftp"
	"github.com/sirupsen/logrus"
	"golang.org/x/crypto/ssh"
	"golang.org/x/net/context"
	"io"
	"os"
	"strconv"
)

var log = logrus.New()

type Server struct{}

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
}

type ConnParams struct {
	Srce SysUri
}

var Params ConnParams
var Conn *ssh.Client

func init() {
	// log to console and file
	f, err := os.OpenFile("SFTPServer.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	wrt := io.MultiWriter(os.Stdout, f)

	log.SetOutput(wrt)
}

func (*Server) Authenticate(ctx context.Context, req *sftppb.AuthenticateToRemoteRequest) (*sftppb.AuthenticateToRemoteResponse, error) {
	log.Printf("Get Service function was invoked with %v\n", req)

	Params = setGetAuthParams(req) // return type is ConnParams

	var filename string
	log.Info(filename)
	from := Params.Srce.Type
	log.Println(from)

	// Read From system
	result := ""
	log.Info("Dial the connection now")
	log.Printf("SysIP= %s \n", Params.Srce.SystemId)
	log.Printf("SysPort= %s \n", Params.Srce.HostPort)

	var config ssh.ClientConfig
	config = ssh.ClientConfig{
		User: Params.Srce.Username,
		Auth: []ssh.AuthMethod{
			ssh.Password(Params.Srce.PassWord),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}

	conn, err := ssh.Dial("tcp", Params.Srce.SystemId+Params.Srce.HostPort, &config)
	if err != nil {
		log.Printf("Error Dialing the server: %v", err)
		log.Error(err)
		result = err.Error()
	}
	defer conn.Close()

	res := &sftppb.AuthenticateToRemoteResponse{
		Response: result,
	}
	log.Info(res.String())
	return res, nil
}

/*
	This service gets a single request for a file from an SFTP server.  It goes to the server and sends back the file in
	chunks sized based on the buffer_size variable.
*/
func (*Server) Get(ctx context.Context, req *sftppb.SrvGetRequest) (*sftppb.SrvGetResponse, error) {
	log.Printf("Get Service function was invoked with %v\n", req)

	Params = setGetParams(req) // return type is ConnParams

	var filename string
	log.Info(filename)
	from := Params.Srce.Type
	//to := Params.Dest.Type
	//name := Params.Dest.FileName
	//toFile := factory.TransferFactory.ReadFrom(Params)
	var bytesReadf int64 = 0

	log.Println(from)
	//log.Info(to)

	var tmpName FileTransfer

	// Read From system
	tmpName = GetSourceType(Params, Params.Srce.Type+"-Get")

	bytesReadf = tmpName.i
	log.Infof("%d bytes copied\n", bytesReadf)
	res := &sftppb.SrvGetResponse{
		FileName:      tmpName.s,
		BytesReturned: strconv.FormatInt(bytesReadf, 10),
		Error:         tmpName.e.Error(),
	}
	log.Info(res.String())
	return res, nil
}

func (*Server) Put(ctx context.Context, req *sftppb.SrvPutRequest) (*sftppb.SrvPutResponse, error) {
	log.Infof("Put function was invoked with %v\n", req)
	Params := setPutParams(req) // return type is ConnParams

	// we assume that the calling functions have already established that the system has the ability to make a connection.
	// This is assuming that we are doing SFTP right now.  This will change in the future to support other file systems
	//
	var filename string
	log.Info(filename)
	log.Println("Hi")
	from := Params.Srce.Type
	//to := Params.Dest.Type
	//log.Info(to)

	//var bytesReadf int64 = 0
	var tmpName FileTransfer

	// Read From system
	log.Infof("calling GetSourceType(%v): ", from)

	// Write from local system To a file
	tmpName = GetSourceType(Params, Params.Srce.Type+"-Put")
	log.Println(Params.Srce.Type + "-Put")

	log.Printf("BytesWritten: %d", tmpName.i)
	log.Println("File Name = " + tmpName.s)
	log.Println("Error : " + tmpName.e.Error())
	log.Infof("%d bytes copied\n", tmpName.i)
	res := &sftppb.SrvPutResponse{
		FileName:      tmpName.s,
		BytesReturned: strconv.FormatInt(tmpName.i, 10),
		Error:         tmpName.e.Error(),
	}
	log.Info(res.String())
	return res, nil
}

func setGetParams(req *sftppb.SrvGetRequest) ConnParams {
	connParams := ConnParams{
		Srce: SysUri{Username: req.SrceSftp.Username, PassWord: req.SrceSftp.PassWord, SystemId: req.SrceSftp.SystemId,
			HostKey: req.SrceSftp.HostKey, HostPort: req.SrceSftp.HostPort, ClientKey: req.SrceSftp.ClientKey,
			FileName: req.SrceSftp.FileName, FileSize: req.SrceSftp.FileSize, BufferSize: req.SrceSftp.BufferSize,
			Type: req.SrceSftp.Type, DestFileName: req.SrceSftp.DestFileName},
	}
	return connParams
}
func setPutParams(req *sftppb.SrvPutRequest) ConnParams {
	connParams := ConnParams{
		Srce: SysUri{Username: req.SrceSftp.Username, PassWord: req.SrceSftp.PassWord, SystemId: req.SrceSftp.SystemId,
			HostKey: req.SrceSftp.HostKey, HostPort: req.SrceSftp.HostPort, ClientKey: req.SrceSftp.ClientKey,
			FileName: req.SrceSftp.FileName, FileSize: req.SrceSftp.FileSize, BufferSize: req.SrceSftp.BufferSize,
			Type: req.SrceSftp.Type, DestFileName: req.SrceSftp.DestFileName},
	}
	return connParams
}
func setGetAuthParams(req *sftppb.AuthenticateToRemoteRequest) ConnParams {
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
	}}
	return connParams
}

//****************************************************************************************
//factory
//****************************************************************************************

type FileTransfer struct {
	s string
	i int64
	e error
}
type ReadTransferFactory interface {
	ReadFrom(params ConnParams) FileTransfer
}
type WriteTransferFactory interface {
	WriteTo(params ConnParams) FileTransfer
}
type SFTP_ReadFrom_Factory struct{}
type SFTP_WriteTo_Factory struct{}

// main factory method
func GetSourceType(params ConnParams, source string) FileTransfer {
	log.Info("Got into the GetSourceType function")
	var readTxfrFactory ReadTransferFactory
	var writeTxfrFactory WriteTransferFactory
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
	default:
		log.Println("No classes were called")
		log.Info("No classes were called.")
		return FileTransfer{"", 0, errors.New("No classes were called")}
	}
}

//****************************************************************************************
//SFTP
//****************************************************************************************

//The assumption is that we are going to copy a file from a remote server to a local temp file.
// it will then be processed by a second client that will take the client and move the file to a second remote server.
// this is synonymous to a get for sftp
func (a SFTP_ReadFrom_Factory) ReadFrom(params ConnParams) FileTransfer { // return temp filename, bytecount, and error

	log.Println("SFTP read from (GET) txfr")
	result := ""
	log.Info("Dial the connection now")
	log.Printf("SysIP= %s \n", params.Srce.SystemId)
	log.Printf("SysPort= %s \n", params.Srce.HostPort)

	var config ssh.ClientConfig
	config = ssh.ClientConfig{
		User: params.Srce.Username,
		Auth: []ssh.AuthMethod{
			ssh.Password(params.Srce.PassWord),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}

	conn, err := ssh.Dial("tcp", params.Srce.SystemId+params.Srce.HostPort, &config)
	if err != nil {
		log.Printf("Error Dialing the server: %v", err)
		log.Error(err)
		result = err.Error()
		return FileTransfer{"", 0, err}
	}
	defer conn.Close()

	//log.Info(params.Srce.FileSize)
	//
	//log.Info(result)

	var MaxPcktSize int
	MaxPcktSize = 1024

	log.Printf("Packet size is: %v \n", MaxPcktSize)
	// create new SFTP client
	client, err := sftp.NewClient(conn, sftp.MaxPacket(MaxPcktSize))
	if err != nil {
		log.Errorf("Error creating new client, %v \n", err)
		result = err.Error()
		return FileTransfer{"", 0, err}
	}
	defer client.Close()

	log.Println("Connection Data:  " + conn.User() + "  " + conn.Conn.RemoteAddr().String())

	log.Printf("Create destination file  %v \n", params.Srce.FileName)
	dstFile, _ := os.Create(params.Srce.FileName) //local file
	if err != nil {
		log.Errorf("Error creating dest file. %v \n", err)
		result = err.Error()
		return FileTransfer{"", 0, err}
	}
	defer dstFile.Close()

	log.Printf("Open source file %v \n", params.Srce.FileName)
	srcFile, err := client.Open(params.Srce.FileName)
	if err != nil {
		log.Errorf("Opening source file: %v \n", err)
		result = err.Error()
		return FileTransfer{"", 0, err}
	}
	log.Println("Source file is opened.")

	bytesWritten, err := srcFile.WriteTo(dstFile)
	if err != nil {
		log.Errorf("Error writing to file: %v \n", err)
		result = err.Error()
		return FileTransfer{"", 0, err}
	}
	log.Infof("%d bytes copied\n", bytesWritten)

	return FileTransfer{params.Srce.FileName, int64(bytesWritten), errors.New(result)}

}
func (a SFTP_WriteTo_Factory) WriteTo(params ConnParams) FileTransfer {
	log.Info("WriteTo sFTP Service function was invoked ")

	//conn, err := connPool.ConnectionPool(params.Srce.Username, params.Srce.PassWord, params.Srce.SystemId, params.Srce.HostKey,
	//	params.Srce.HostPort, params.Srce.ClientKey, params.Srce.FileName, params.Srce.FileSize)
	//log.Info(params.Dest.FileSize)
	log.Println("User name: " + params.Srce.Username)
	log.Println("Password: " + params.Srce.PassWord)
	log.Println("System = " + params.Srce.SystemId)
	log.Println("port = " + params.Srce.HostPort)
	log.Println("File name: " + params.Srce.FileName)

	//var config ssh.ClientConfig
	log.Println("Make the ssh/sftp connection")
	config := ssh.ClientConfig{
		User: params.Srce.Username,
		Auth: []ssh.AuthMethod{
			ssh.Password(params.Srce.PassWord),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		//HostKeyCallback: ssh.FixedHostKey(HostKey),
	}
	log.Println("Dial the conenction now")
	conn, err := ssh.Dial("tcp", params.Srce.SystemId+params.Srce.HostPort, &config)
	if err != nil {
		log.Errorf("Error Dialing the server: %f", err)
		return FileTransfer{"", 0, err}
	}
	defer conn.Close()

	result := ""

	log.Println("Create new SFTP client")
	client, err := sftp.NewClient(conn)
	if err != nil {
		log.Println("Error creating new client", err)
		return FileTransfer{"", 0, err}
	}
	defer client.Close()

	log.Println("Create destination file")
	dstFile, err := client.Create(params.Srce.DestFileName)
	if err != nil {
		log.Errorf("Error creating dest file. %s. %s \n", params.Srce.DestFileName, err)
		result = err.Error()
		return FileTransfer{"", 0, err}
	}
	defer dstFile.Close()

	log.Println("Open source file: " + params.Srce.FileName)
	// note this should include the /scratch directory
	srcFile, err := os.Open(params.Srce.FileName)
	if err != nil {
		log.Errorf("Opening source file: %s \n", err)
		result = err.Error()
		return FileTransfer{"", 0, err}
	}
	var bytesWritten int64
	// if there were no error opening the two files then process them
	if result == "" {
		log.Println("Write to dstFile: " + dstFile.Name())
		//bytesWritten, err := srcFile.WriteTo(dstFile)
		bytesWritten, err = dstFile.ReadFrom(srcFile)
		if err != nil {
			log.Errorf("Error writing to file: %s \n", err)
			result = err.Error()
		}
		log.Println("Name: " + dstFile.Name())
		log.Println(bytesWritten)
		err = errors.New(result)
		log.Printf("%d bytes copied\n", bytesWritten)
		log.Println("Errors: " + err.Error())
		return FileTransfer{dstFile.Name(), bytesWritten, err}
	}
	log.Println(result)
	err = errors.New(result)
	log.Errorf("There was an error with the transfer.  Err = %s \n", err)
	return FileTransfer{"", 0, err}
}

func GetFileSize(filepath string) (int64, error) {
	fi, err := os.Stat(filepath)
	if err != nil {
		return 0, err
	}
	// get the size
	return fi.Size(), nil
}
