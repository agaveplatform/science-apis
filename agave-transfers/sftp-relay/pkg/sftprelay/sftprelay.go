package sftprelay

import (
	"github.com/pkg/sftp"
	"github.com/sirupsen/logrus"
	"golang.org/x/crypto/ssh"
	"golang.org/x/net/context"
	"io"
	"os"
	"strconv"
	sftppb "github.com/agaveplatform/science-apis/agave-transfers/sftp-relay/pkg/sftpproto"
)

var log = logrus.New()

type Server struct{}

type URI struct {
	Username string
	PassWord string
	SystemId string
	HostKey string
	HostPort string
	ClientKey string
	FileName string
	FileSize int64
}
//var ConnParams param
var UriParams = make ([]*URI,0)

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


func getConnection(uri URI) (*ssh.Client, error){


	// get host public key
	//HostKey := getHostKey(systemId)
	config := ssh.ClientConfig{
		User: uri.Username,
		Auth: []ssh.AuthMethod{
			ssh.Password(uri.PassWord),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		//HostKeyCallback: ssh.FixedHostKey(HostKey),
	}

	// connect
	conn, err := ssh.Dial("tcp", uri.SystemId + uri.HostPort, &config)
	if err != nil {
		log.Info("Error Dialing the server: %f", err)
		log.Error(err)
	}
	defer conn.Close()
	return conn, nil
}


/*
	This service gets a single request for a file from an SFTP server.  It goes to the server and sends back the file in
	chunks sized based on the buffer_size variable.
*/
func (*Server) StreamingReader(ctx context.Context, req *sftppb.SrvGetRequest) (*sftppb.SrvGetResponse){
	log.Info("Streaming Get Service function was invoked with %v\n", req)

	fileName := req.HostSftp.FileName
	fileSize := req.HostSftp.FileSize
	bufferSize := req.HostSftp.BufferSize
	ConnParams.PassWord = req.HostSftp.PassWord
	ConnParams.SystemId = req.HostSftp.SystemId
	ConnParams.Username = req.HostSftp.Username
	ConnParams.HostKey = req.HostSftp.HostKey
	ConnParams.HostPort = req.HostSftp.HostPort
	ConnParams.ClientKey = req.HostSftp.ClientKey

	conn, err := getConnection()
	log.Info(fileSize)
	result := ""
	log.Info(result)

	// create new SFTP client
	client, err := sftp.NewClient(conn)
	if err != nil {
		log.Info("Error creating new client", err)
		result = err.Error()
	}
	defer client.Close()

	// open source file
	srcFile, err := client.Open(fileName)
	if err != nil {
		log.Info("Opening source file: ", err)
		result = err.Error()
	}
	defer srcFile.Close()


	log.Info("reading %v bytes", bufferSize)
	// Copy the file

	for _, file := range files {
		res := &sftppb.SrvGetResponse {
			Result        string   `protobuf:"bytes,1,opt,name=result,proto3" json:"result,omitempty"`
			XXX_NoUnkeyedLiteral struct{} `json:"-"`
			XXX_unrecognized     []byte   `json:"-"`
			XXX_sizecache        int32    `json:"-"`
		}
		stream.Send(res)
	}

	return nil
}

func (*Server) StreamingWriter(ctx context.Context, req *sftppb.SrvPutRequest) (*sftppb.SrvPutResponse){
	log.Info("CopyFromRemoteService function was invoked with %v\n", req)
	fileName := req.Sftp.FileName
	fileSize := req.Sftp.FileSize
	bufferSize := req.Sftp.BufferSize
	ConnParams.PassWord = req.Sftp.PassWord
	ConnParams.SystemId = req.Sftp.SystemId
	ConnParams.Username = req.Sftp.Username
	ConnParams.HostKey = req.Sftp.HostKey
	ConnParams.HostPort = req.Sftp.HostPort
	ConnParams.ClientKey = req.Sftp.ClientKey

	conn, err := getConnection()

	result := ""

	// create new SFTP client
	client, err := sftp.NewClient(conn)
	if err != nil {
		log.Info("Error creating new client", err)
		result = err.Error()
	}
	defer client.Close()

	// create destination file
	dstFile, err := os.Create(fileName)
	if err != nil {
		log.Info("Error creating dest file", err)
		result = err.Error()
	}
	defer dstFile.Close()

	// open source file
	srcFile, err := client.Open(fileName)
	if err != nil {
		log.Info("Opening source file: ", err)
		result = err.Error()
	}

	for i
	// copy with the WriteTo function
	bytesWritten, err := srcFile.WriteTo(dstFile)
	if err != nil {
		log.Info("Error writing to file: ", err)
	}


	log.Info("%d bytes copied\n", bytesWritten)
	log.Info(result)
	res := &sftppb.CopyFromRemoteResponse{
		Result: strconv.FormatInt(bytesWritten, 10),
	}
	log.Println(res.String())
	return  nil
}
