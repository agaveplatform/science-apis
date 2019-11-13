package sftprelay

import (
	"bufio"
	"errors"
	"fmt"
	//connPool "github.com/agaveplatform/science-apis/agave-transfers/sftp-relay/pkg/connectionpool"
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
	Username   string
	PassWord   string
	SystemId   string
	HostKey    string
	HostPort   string
	ClientKey  string
	FileName   string
	FileSize   int64
	BufferSize int64
	Type       string // Valid values are:  FTP, AZURE, S3, SWIFT, SFTP, GRIDFTP, IRODS, IRODS4, LOCAL
}

type ConnParams struct {
	Srce SysUri
}

var Params ConnParams
var Conn *ssh.Client

func (*Server) Authenticate(ctx context.Context, req *sftppb.AuthenticateToRemoteRequest) (*sftppb.AuthenticateToRemoteResponse, error) {
	fmt.Printf("Get Service function was invoked with %v\n", req)

	Params = setGetAuthParams(req) // return type is ConnParams

	var filename string
	log.Info(filename)
	from := Params.Srce.Type
	fmt.Println(from)

	// Read From system
	result := ""
	log.Info("Dial the connection now")
	fmt.Printf("SysIP= %s \n", Params.Srce.SystemId)
	fmt.Printf("SysPort= %s \n", Params.Srce.HostPort)

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
		fmt.Printf("Error Dialing the server: %v", err)
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
	fmt.Printf("Get Service function was invoked with %v\n", req)

	Params = setGetParams(req) // return type is ConnParams

	var filename string
	log.Info(filename)
	from := Params.Srce.Type
	//to := Params.Dest.Type
	//name := Params.Dest.FileName
	//toFile := factory.TransferFactory.ReadFrom(Params)
	var bytesReadf int64 = 0

	fmt.Println(from)
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
	fmt.Println("Hi")
	from := Params.Srce.Type
	//to := Params.Dest.Type
	//log.Info(to)

	//var bytesReadf int64 = 0
	var tmpName FileTransfer

	// Read From system
	log.Infof("calling GetSourceType(%v): ", from)

	// Write from local system To a file
	tmpName = GetSourceType(Params, Params.Srce.Type+"-Put")
	fmt.Println(Params.Srce.Type + "-Put")

	fmt.Printf("BytesWritten: %d", tmpName.i)
	fmt.Println("File Name = " + tmpName.s)
	fmt.Println("Error : " + tmpName.e.Error())
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
			Type: req.SrceSftp.Type},
	}
	return connParams
}
func setPutParams(req *sftppb.SrvPutRequest) ConnParams {
	connParams := ConnParams{
		Srce: SysUri{Username: req.SrceSftp.Username, PassWord: req.SrceSftp.PassWord, SystemId: req.SrceSftp.SystemId,
			HostKey: req.SrceSftp.HostKey, HostPort: req.SrceSftp.HostPort, ClientKey: req.SrceSftp.ClientKey,
			FileName: req.SrceSftp.FileName, FileSize: req.SrceSftp.FileSize, BufferSize: req.SrceSftp.BufferSize,
			Type: req.SrceSftp.Type},
	}
	return connParams
}
func setGetAuthParams(req *sftppb.AuthenticateToRemoteRequest) ConnParams {
	connParams := ConnParams{Srce: SysUri{
		Username:   req.Auth.Username,
		PassWord:   req.Auth.PassWord,
		SystemId:   req.Auth.SystemId,
		HostKey:    req.Auth.HostKey,
		HostPort:   req.Auth.HostPort,
		ClientKey:  req.Auth.ClientKey,
		FileName:   req.Auth.FileName,
		FileSize:   0,
		BufferSize: 16138,
		Type:       req.Auth.Type,
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
type LOCAL_Factory struct{}
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
		fmt.Println("Calling the SFTP Put class")
		log.Info("Calling the SFTP Put class")
		writeTxfrFactory = SFTP_WriteTo_Factory{}
		return writeTxfrFactory.WriteTo(params)
	case "LOCAL":
		fmt.Println("Calling the Local class")
		log.Info("Calling the Local class")
		readTxfrFactory = LOCAL_Factory{}
		return readTxfrFactory.ReadFrom(params)
	default:
		fmt.Println("No classes were called")
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

	fmt.Println("SFTP read from (GET) txfr")
	result := ""
	log.Info("Dial the connection now")
	fmt.Printf("SysIP= %s \n", params.Srce.SystemId)
	fmt.Printf("SysPort= %s \n", params.Srce.HostPort)

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
		fmt.Printf("Error Dialing the server: %v", err)
		log.Error(err)
		result = err.Error()
	}
	defer conn.Close()

	//log.Info(params.Srce.FileSize)
	//
	//log.Info(result)

	var MaxPcktSize int
	MaxPcktSize = 1024

	fmt.Printf("Packet size is: %v \n", MaxPcktSize)
	// create new SFTP client
	client, err := sftp.NewClient(conn, sftp.MaxPacket(MaxPcktSize))
	if err != nil {
		fmt.Printf("Error creating new client, %v \n", err)
		result = err.Error()
	}
	defer client.Close()

	fmt.Println("Connection Data:  " + conn.User() + "  " + conn.Conn.RemoteAddr().String())
	// create destination file
	// make a unique file name here /scratch/filename
	//fmt.Println("Verify that /scratch is present")
	//tempDir, _ := ioutil.TempDir("", "/tmp")
	//fmt.Println("Temp Dir = "+ tempDir)

	fmt.Printf("Create destination file  %v \n", params.Srce.FileName)
	dstFile, _ := os.Create(params.Srce.FileName) //local file
	//dstFile, err := os.Create(params.uriReader.FileName)
	if err != nil {
		fmt.Printf("Error creating dest file. %v \n", err)
		result = err.Error()
	}
	defer dstFile.Close()

	fmt.Printf("Open source file %v \n", params.Srce.FileName)
	srcFile, err := client.Open(params.Srce.FileName)
	if err != nil {
		fmt.Printf("Opening source file: %v \n", err)
		result = err.Error()
	}
	fmt.Println("Source file is opened.")

	bytesWritten, err := srcFile.WriteTo(dstFile)
	if err != nil {
		fmt.Printf("Error writing to file: %v \n", err)
		result = err.Error()
	}
	log.Infof("%d bytes copied\n", bytesWritten)

	return FileTransfer{params.Srce.FileName, int64(bytesWritten), errors.New(result)}

}
func (a SFTP_WriteTo_Factory) WriteTo(params ConnParams) FileTransfer {
	log.Info("WriteTo sFTP Service function was invoked ")

	//conn, err := connPool.ConnectionPool(params.Srce.Username, params.Srce.PassWord, params.Srce.SystemId, params.Srce.HostKey,
	//	params.Srce.HostPort, params.Srce.ClientKey, params.Srce.FileName, params.Srce.FileSize)
	//log.Info(params.Dest.FileSize)
	fmt.Println("User name: " + params.Srce.Username)
	fmt.Println("Password: " + params.Srce.PassWord)
	fmt.Println("System = " + params.Srce.SystemId)
	fmt.Println("port = " + params.Srce.HostPort)
	fmt.Println("File name: " + params.Srce.FileName)

	//var config ssh.ClientConfig
	fmt.Println("Make the ssh/sftp connection")
	config := ssh.ClientConfig{
		User: params.Srce.Username,
		Auth: []ssh.AuthMethod{
			ssh.Password(params.Srce.PassWord),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		//HostKeyCallback: ssh.FixedHostKey(HostKey),
	}
	fmt.Println("Dial the conenction now")
	conn, err := ssh.Dial("tcp", params.Srce.SystemId+params.Srce.HostPort, &config)
	if err != nil {
		fmt.Printf("Error Dialing the server: %f", err)
		//log.Error(err)
	}
	defer conn.Close()

	result := ""
	log.Info(result)

	fmt.Println("Create new SFTP client")
	client, err := sftp.NewClient(conn)
	if err != nil {
		fmt.Println("Error creating new client", err)
		result = err.Error()
	}
	defer client.Close()

	fmt.Println("Create destination file")
	dstFile, err := client.Create(params.Srce.FileName + "_1")
	if err != nil {
		fmt.Println("Error creating dest file", err)
		result = err.Error()
	}
	defer dstFile.Close()

	fmt.Println("Open source file: " + params.Srce.FileName)
	// note this should include the /scratch directory
	srcFile, err := os.Open(params.Srce.FileName)
	if err != nil {
		log.Info("Opening source file: ", err)
		result = err.Error()
	}

	fmt.Println("Write to dstFile: " + dstFile.Name())
	//bytesWritten, err := srcFile.WriteTo(dstFile)
	bytesWritten, err := dstFile.ReadFrom(srcFile)
	if err != nil {
		log.Info("Error writing to file: ", err)
		result = err.Error()
	}
	fmt.Println(bytesWritten)
	err = errors.New(result)
	fmt.Printf("%d bytes copied\n", bytesWritten)
	fmt.Println("Name: " + dstFile.Name())
	fmt.Println("Errors: " + err.Error())
	return FileTransfer{dstFile.Name(), bytesWritten, err}
}

func (a LOCAL_Factory) ReadFrom(params ConnParams) FileTransfer {
	log.Info("got into ReadFrom Local")
	name := params.Srce.FileName

	// open input file
	file, err := os.Open(name)
	if err != nil {
		log.Errorf("Error opening input file: %s", err)
		return FileTransfer{"", 0, err}
	}
	// close fi on exit and check for its returned error
	defer func() {
		if err := file.Close(); err != nil {
			log.Errorf("Error closing output file: %s", err)
		}
	}()
	// make a read buffer
	r := bufio.NewReader(file)

	// open output file
	fileOut, err := os.Create("output.txt")
	if err != nil {
		log.Errorf("Error opening output file: %s", err)
		return FileTransfer{"", 0, err}
	}
	// close fo on exit and check for its returned error
	defer func() {
		if err := fileOut.Close(); err != nil {
			log.Errorf("Error opening output file: %s", err)
		}
	}()
	// make a write buffer
	w := bufio.NewWriter(fileOut)

	// make a buffer to keep chunks that are read
	buf := make([]byte, 1024)
	for {
		// read a chunk
		n, err := r.Read(buf)
		if err != nil && err != io.EOF {
			log.Errorf("Error reading input file: %s", err)
			return FileTransfer{"", 0, err}
		}
		if n == 0 {
			break
		}
		// write a chunk
		if _, err := w.Write(buf[:n]); err != nil {
			log.Errorf("Error writing output file: %s", err)
			return FileTransfer{"", 0, err}
		}
	}

	if err = w.Flush(); err != nil {
		log.Errorf("Unexpected error: %s", err)
		return FileTransfer{"", 0, err}
		//panic(err)
	}

	fileSize, err := GetFileSize(fileOut.Name())
	if err != nil {
		log.Errorf("File size error: %s", err)
		return FileTransfer{"", 0, err}
	}

	return FileTransfer{name, fileSize, nil}
}
func GetFileSize(filepath string) (int64, error) {
	fi, err := os.Stat(filepath)
	if err != nil {
		return 0, err
	}
	// get the size
	return fi.Size(), nil
}
