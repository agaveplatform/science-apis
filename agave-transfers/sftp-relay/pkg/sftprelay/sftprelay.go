package sftprelay

import (
	"bufio"
	connPool "github.com/agaveplatform/science-apis/agave-transfers/sftp-relay/pkg/connectionpool"
	"io"
	"io/ioutil"
	"os"
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
	Type       string // Valid values are:  FTP, AZURE, S3, SWIFT, SFTP, GRIDFTP, IRODS, IRODS4, LOCAL
}

type ConnParams struct {
	Srce SysUri
	Dest SysUri
}

var Params ConnParams
var Conn *ssh.Client

/*
	This service gets a single request for a file from an SFTP server.  It goes to the server and sends back the file in
	chunks sized based on the buffer_size variable.
*/
func (*Server) Get(ctx context.Context, req *sftppb.SrvGetRequest) (*sftppb.SrvGetResponse, error) {
	log.Info("Get Service function was invoked with %v\n", req)

	Params = setGetParams(req) // return type is ConnParams

	var filename string
	log.Info(filename)
	from := Params.Srce.Type
	to := Params.Dest.Type
	//name := Params.Dest.FileName
	//toFile := factory.TransferFactory.ReadFrom(params)
	var bytesReadf int64 = 0

	log.Info(from)
	log.Info(to)

	var tmpName FileTransfer

	// Read From system
	tmpName = GetSourceType(Params, from, "")
	name := tmpName.s
	// Write from local system To a file
	GetSourceType(Params, to, name)

	log.Info("%d bytes copied\n", bytesReadf)
	res := &sftppb.SrvGetResponse{
		Result: strconv.FormatInt(bytesReadf, 10),
	}
	log.Info(res.String())
	return res, nil
}

func (*Server) Put(ctx context.Context, req *sftppb.SrvPutRequest) (*sftppb.SrvPutResponse, error) {
	log.Info("Put function was invoked with %v\n", req)
	Params := setPutParams(req) // return type is ConnParams

	// we assume that the calling functions have already established that the system has the ability to make a connection.
	// This is assuming that we are doing SFTP right now.  This will change in the future to support other file systems
	//
	var filename string
	log.Info(filename)
	from := Params.Srce.Type
	to := Params.Dest.Type
	log.Info(to)

	var bytesReadf int64 = 0
	var tmpName FileTransfer

	// Read From system
	log.Info("calling GetSourceType(%v): ", from)
	tmpName = GetSourceType(Params, from, "")
	// Write from local system To a file
	name := tmpName.s
	GetSourceType(Params, to, name)

	log.Info("%d bytes copied\n", bytesReadf)
	res := &sftppb.SrvPutResponse{
		Result: strconv.FormatInt(bytesReadf, 10),
	}
	log.Info(res.String())
	return res, nil
}

func setGetParams(req *sftppb.SrvGetRequest) ConnParams {
	connParams := ConnParams{
		Srce: SysUri{Username: req.SrceSftp.Username, PassWord: req.SrceSftp.PassWord, SystemId: req.SrceSftp.SystemId, HostKey: req.SrceSftp.HostKey, HostPort: req.SrceSftp.HostPort, ClientKey: req.SrceSftp.ClientKey, FileName: req.SrceSftp.FileName, FileSize: req.SrceSftp.FileSize, BufferSize: req.SrceSftp.BufferSize, Type: req.SrceSftp.Type},
		Dest: SysUri{Username: req.DestSftp.Username, PassWord: req.DestSftp.PassWord, SystemId: req.DestSftp.SystemId, HostKey: req.DestSftp.HostKey, HostPort: req.DestSftp.HostPort, ClientKey: req.DestSftp.ClientKey, FileName: req.DestSftp.FileName, FileSize: req.DestSftp.FileSize, BufferSize: req.DestSftp.BufferSize, Type: req.DestSftp.Type},
	}
	return connParams
}
func setPutParams(req *sftppb.SrvPutRequest) ConnParams {
	connParams := ConnParams{
		Srce: SysUri{Username: req.SrceSftp.Username, PassWord: req.SrceSftp.PassWord, SystemId: req.SrceSftp.SystemId, HostKey: req.SrceSftp.HostKey, HostPort: req.SrceSftp.HostPort, ClientKey: req.SrceSftp.ClientKey, FileName: req.SrceSftp.FileName, FileSize: req.SrceSftp.FileSize, BufferSize: req.SrceSftp.BufferSize, Type: req.SrceSftp.Type},
		Dest: SysUri{Username: req.DestSftp.Username, PassWord: req.DestSftp.PassWord, SystemId: req.DestSftp.SystemId, HostKey: req.DestSftp.HostKey, HostPort: req.DestSftp.HostPort, ClientKey: req.DestSftp.ClientKey, FileName: req.DestSftp.FileName, FileSize: req.DestSftp.FileSize, BufferSize: req.DestSftp.BufferSize, Type: req.DestSftp.Type},
	}
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
	ReadFrom(params ConnParams, name string) FileTransfer
}
type LOCAL_Factory struct{}
type SFTP_Factory struct{}

// main factory method
func GetSourceType(params ConnParams, source string, tmpName string) FileTransfer {
	log.Info("Got into the GetSourceType function")
	var readTxfrFact ReadTransferFactory

	switch source {
	case "SFTP":
		log.Info("Calling the SFTP class")
		readTxfrFact = SFTP_Factory{}
		return readTxfrFact.ReadFrom(params, tmpName)
	case "LOCAL":
		log.Info("Calling the Local class")
		readTxfrFact = LOCAL_Factory{}
		return readTxfrFact.ReadFrom(params, tmpName)
	}

	return FileTransfer{}

}

//****************************************************************************************
//SFTP
//****************************************************************************************

//The assumption is that we are going to copy a file from a remote server to a local temp file.
// it will then be processed by a second client that will take the client and move the file to a second remote server.
// this is synonymous to a get for sftp
func (a SFTP_Factory) ReadFrom(params ConnParams, name string) FileTransfer { // return temp filename, bytecount, and error

	var nameIs string
	if name == "" {
		log.Info("SFTP read from (GET) txfr")
		result := ""
		log.Info("Dial the conenction now")
		log.Info("Sys= %s", params.Srce.SystemId)
		log.Info("Sys= %s", params.Srce.HostPort)

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
			log.Info("Error Dialing the server: %f", err)
			log.Error(err)
			result = err.Error()
		}
		defer conn.Close()

		log.Info(params.Srce.FileSize)

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
		nameIs = dstFile.Name()
		return FileTransfer{params.Srce.FileName, bytesWritten, nil}

	} else {
		log.Info("WriteTo sFTP Service function was invoked ")

		conn, err := connPool.ConnectionPool(params.Srce.Username, params.Srce.PassWord, params.Srce.SystemId, params.Srce.HostKey,
			params.Srce.HostPort, params.Srce.ClientKey, params.Srce.FileName, params.Srce.FileSize)
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
		return FileTransfer{params.Srce.FileName, bytesWritten, nil}
	}
	return FileTransfer{nameIs, 0, nil}
}

func (a LOCAL_Factory) ReadFrom(params ConnParams, name string) FileTransfer {
	log.Info("got into ReadFrom Local")

	if name == "" {
		// open input file
		file, err := os.Open(params.Srce.FileName)
		if err != nil {
			panic(err)
		}
		// close fi on exit and check for its returned error
		defer func() {
			if err := file.Close(); err != nil {
				panic(err)
			}
		}()
		// make a read buffer
		r := bufio.NewReader(file)

		// open output file
		fileOut, err := os.Create("output.txt")
		if err != nil {
			panic(err)
		}
		// close fo on exit and check for its returned error
		defer func() {
			if err := fileOut.Close(); err != nil {
				panic(err)
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
				panic(err)
			}
			if n == 0 {
				break
			}

			// write a chunk
			if _, err := w.Write(buf[:n]); err != nil {
				panic(err)
			}
		}

		if err = w.Flush(); err != nil {
			panic(err)
		}
	} else {
		// copy from tmp (name)
		// open input file
		file, err := os.Open(name)
		if err != nil {
			panic(err)
		}
		// close fi on exit and check for its returned error
		defer func() {
			if err := file.Close(); err != nil {
				panic(err)
			}
		}()
		// make a read buffer
		r := bufio.NewReader(file)

		// open output file
		fileOut, err := os.Create("output.txt")
		if err != nil {
			panic(err)
		}
		// close fo on exit and check for its returned error
		defer func() {
			if err := fileOut.Close(); err != nil {
				panic(err)
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
				panic(err)
			}
			if n == 0 {
				break
			}

			// write a chunk
			if _, err := w.Write(buf[:n]); err != nil {
				panic(err)
			}
		}

		if err = w.Flush(); err != nil {
			panic(err)
		}
	}

	return FileTransfer{"", 0, nil}
}
