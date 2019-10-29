package sftprelay

import (
	connPool "github.com/agaveplatform/science-apis/agave-transfers/sftp-relay/pkg/connectionpool"
	"io/ioutil"
	"strconv"

	sftppb "github.com/agaveplatform/science-apis/agave-transfers/sftp-relay/pkg/sftpproto"
	"github.com/pkg/sftp"
	"github.com/sirupsen/logrus"
	ssh "golang.org/x/crypto/ssh"
	"golang.org/x/net/context"
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
	TxfrType   string // Valid values are:  FTP, AZURE, S3, SWIFT, SFTP, GRIDFTP, IRODS, IRODS4, LOCAL
}

type ConnParams struct {
	Srce SysUri
	Dest SysUri
}

func (c ConnParams) ReadFrom() FileTransfer {
	panic("implement me")
}

var Conn *ssh.Client

//var UriParams = make ([]*URI,0)

/*
	This service gets a single request for a file from an SFTP server.  It goes to the server and sends back the file in
	chunks sized based on the buffer_size variable.
*/
func (*Server) Get(ctx context.Context, req *sftppb.SrvGetRequest) *sftppb.SrvGetResponse {
	log.Info("Get Service function was invoked with %v\n", req)

	params := setGetParams(req) // return type is ConnParams

	// we assume that the calling functions have already established that the system has the ability to make a connection.
	// This is assuming that we are doing SFTP right now.  This will change in the future to support other file systems
	//

	if params.Srce.TxfrType == "SFTP" || params.Dest.TxfrType == "SFTP" {
		c, err := connPool.ConnectionPool(params.Srce.Username, req.SrceSftp.PassWord, req.SrceSftp.SystemId, req.SrceSftp.HostKey,
			req.SrceSftp.HostPort, req.SrceSftp.ClientKey, req.SrceSftp.FileName, req.SrceSftp.FileSize)
		if err != nil {
			log.Error(err)
		}
		Conn = c
	}

	var filename string
	from := params.Srce.TxfrType
	to := params.Dest.TxfrType
	//toFile := factory.TransferFactory.ReadFrom(params)
	var bytesReadf int64 = 0

	log.Info(from)
	log.Info(to)
	///log.Info(toFile)

	// Read From system
	GetSourceType(from)
	//GetDestType(to)

	if params.Srce.TxfrType == "SFTP" {
		f, bytesRead, err := ReadFrom(params, Conn)
		if err != nil {
			log.Error(err)
		}
		if bytesRead <= 1 {
			log.Error("It looks like nothing was done. There was a total of  %v bytes read ", bytesRead)
		}
		filename = f
		bytesReadf = bytesRead
	}
	log.Info(filename)

	// Write from local system To a file
	if params.Dest.TxfrType == "LOCAL" {

	}

	log.Info("%d bytes copied\n", bytesReadf)
	res := &sftppb.SrvGetResponse{
		Result: strconv.FormatInt(bytesReadf, 10),
	}
	log.Info(res.String())
	return res
}

func (*Server) Put(ctx context.Context, req *sftppb.SrvPutRequest) *sftppb.SrvPutResponse {
	log.Info("CopyFromRemoteService function was invoked with %v\n", req)
	params := setPutParams(req) // return type is ConnParams

	// we assume that the calling functions have already established that the system has the ability to make a connection.
	// This is assuming that we are doing SFTP right now.  This will change in the future to support other file systems
	//
	var filename string
	from := params.Srce.TxfrType
	to := params.Dest.TxfrType
	log.Info(to)
	//toFile := factory.TransferFactory.ReadFrom(params)
	var bytesReadf int64 = 0

	if params.Srce.TxfrType == "SFTP" || params.Dest.TxfrType == "SFTP" {
		c, err := connPool.ConnectionPool(params.Srce.Username, req.SrceSftp.PassWord, req.SrceSftp.SystemId, req.SrceSftp.HostKey,
			req.SrceSftp.HostPort, req.SrceSftp.ClientKey, req.SrceSftp.FileName, req.SrceSftp.FileSize)
		if err != nil {
			log.Error(err)
		}
		Conn = c
	}
	// Read From system
	GetSourceType(from)
	//GetDestType(to)

	if params.Srce.TxfrType == "SFTP" {
		f, bytesRead, err := ReadFrom(params, Conn)
		if err != nil {
			log.Error(err)
		}
		if bytesRead <= 1 {
			log.Error("It looks like nothing was done. There was a total of  %v bytes read ", bytesRead)
		}
		filename = f
		bytesReadf = bytesRead
	}
	log.Info(filename)

	// Write from local system To a file
	if params.Dest.TxfrType == "LOCAL" {

	}

	log.Info("%d bytes copied\n", bytesReadf)
	res := &sftppb.SrvPutResponse{
		Result: strconv.FormatInt(bytesReadf, 10),
	}
	log.Info(res.String())
	return res
}

func setGetParams(req *sftppb.SrvGetRequest) ConnParams {
	connParams := ConnParams{
		Srce: SysUri{Username: req.SrceSftp.Username, PassWord: req.SrceSftp.PassWord, SystemId: req.SrceSftp.SystemId, HostKey: req.SrceSftp.HostKey, HostPort: req.SrceSftp.HostPort, ClientKey: req.SrceSftp.ClientKey, FileName: req.SrceSftp.FileName, FileSize: req.SrceSftp.FileSize, BufferSize: req.SrceSftp.BufferSize, TxfrType: req.SrceSftp.Type},
		Dest: SysUri{Username: req.DestSftp.Username, PassWord: req.DestSftp.PassWord, SystemId: req.DestSftp.SystemId, HostKey: req.DestSftp.HostKey, HostPort: req.DestSftp.HostPort, ClientKey: req.DestSftp.ClientKey, FileName: req.DestSftp.FileName, FileSize: req.DestSftp.FileSize, BufferSize: req.DestSftp.BufferSize, TxfrType: req.DestSftp.Type},
	}
	return connParams
}
func setPutParams(req *sftppb.SrvPutRequest) ConnParams {
	connParams := ConnParams{
		Srce: SysUri{Username: req.SrceSftp.Username, PassWord: req.SrceSftp.PassWord, SystemId: req.SrceSftp.SystemId, HostKey: req.SrceSftp.HostKey, HostPort: req.SrceSftp.HostPort, ClientKey: req.SrceSftp.ClientKey, FileName: req.SrceSftp.FileName, FileSize: req.SrceSftp.FileSize, BufferSize: req.SrceSftp.BufferSize, TxfrType: req.SrceSftp.Type},
		Dest: SysUri{Username: req.DestSftp.Username, PassWord: req.DestSftp.PassWord, SystemId: req.DestSftp.SystemId, HostKey: req.DestSftp.HostKey, HostPort: req.DestSftp.HostPort, ClientKey: req.DestSftp.ClientKey, FileName: req.DestSftp.FileName, FileSize: req.DestSftp.FileSize, BufferSize: req.DestSftp.BufferSize, TxfrType: req.DestSftp.Type},
	}
	return connParams
}

//****************************************************************************************
//factory
//****************************************************************************************

type FileTransfer struct {
	params ConnParams
}

type TransferFactory interface {
	ReadFrom() FileTransfer
}

type LOCAL struct{}

type SFTP_Factory struct{}

func (a SFTP_Factory) ReadFrom() ConnParams {
	//var params ConnParams
	params := ConnParams{}
	return params
}

// main factory method
func GetSourceType(typeTxfr string) FileTransfer {

	var txfrFact TransferFactory

	switch typeTxfr {
	case "SFTP":
		txfrFact = SFTP_Factory{}
		return txfrFact.ReadFrom()
		//case "LOCAL-SFTP":
		//	txfrFact = LOCAL_Factory{}
		//	return txfrFact.CreateFileTxfr()
		//case "SFTP-SFTP":
		//	txfrFact = SFTP_SFTP_Factory{}
		//	return txfrFact.CreateFileTxfr()
	}
	return FileTransfer{}
}

//****************************************************************************************
//SFTP
//****************************************************************************************

//The assumption is that we are going to copy a file from a remote server to a local temp file.
// it will then be processed by a second client that will take the client and move the file to a second remote server.
// this is synonymous to a get for sftp
func ReadFrom(params ConnParams, conn *ssh.Client) (string, int64, error) { // return temp filename, bytecount, and error
	log.Info("SFTP read from (GET) txfr")

	log.Info(params.Srce.FileSize)
	result := ""
	log.Info(result)

	// create new SFTP client
	client, err := sftp.NewClient(conn)
	if err != nil {
		log.Info("Error creating new client", err)
		result = err.Error()
	}
	defer client.Close()

	// create destination file
	// make a unique file name here /scratch/filename
	//verify that /scratch is present
	tempDir, _ := ioutil.TempDir("", "/scratch")

	dstFile, _ := ioutil.TempFile(tempDir, params.Srce.FileName)
	//dstFile, err := os.Create(params.uriReader.FileName)
	if err != nil {
		log.Info("Error creating dest file", err)
		result = err.Error()
	}
	defer dstFile.Close()

	// open source file
	srcFile, err := client.Open(params.Srce.FileName)
	if err != nil {
		log.Info("Opening source file: ", err)
		result = err.Error()
	}

	bytesWritten, err := srcFile.WriteTo(dstFile)
	if err != nil {
		log.Info("Error writing to file: ", err)
	}
	log.Info("%d bytes copied\n", bytesWritten)
	nameIs := dstFile.Name()
	return nameIs, bytesWritten, nil
}

// the following will take a name and move that to an SFTP.  Basically doing a put on the file
func WriteTo(params ConnParams, conn *ssh.Client, name string) { // name is a temporary file name that was created at the put end of the stream
	log.Info("Put sFTP Service function was invoked with")

	log.Info(params.Dest.FileSize)
	result := ""
	log.Info(result)

	// create new SFTP client
	client, err := sftp.NewClient(conn)
	if err != nil {
		log.Info("Error creating new client", err)
		result = err.Error()
	}
	defer client.Close()

	// create destination file
	dstFile, err := client.Create(params.Dest.FileName)
	if err != nil {
		log.Info("Error creating dest file", err)
		result = err.Error()
	}
	defer dstFile.Close()

	// open source file
	// note this should include the /scratch directory
	srcFile, err := client.Open(name)
	if err != nil {
		log.Info("Opening source file: ", err)
		result = err.Error()
	}

	bytesWritten, err := srcFile.WriteTo(dstFile)
	if err != nil {
		log.Info("Error writing to file: ", err)
	}
	log.Info("%d bytes copied\n", bytesWritten)
}
