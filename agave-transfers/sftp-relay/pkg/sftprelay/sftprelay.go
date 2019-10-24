package sftprelay

import (
	cp "github.com/agaveplatform/science-apis/agave-transfers/sftp-relay/pkg/connectionpool"
	iread "github.com/agaveplatform/science-apis/agave-transfers/sftp-relay/pkg/ireader"
	iwrite "github.com/agaveplatform/science-apis/agave-transfers/sftp-relay/pkg/iwriter"
	sftppb "github.com/agaveplatform/science-apis/agave-transfers/sftp-relay/pkg/sftpproto"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"io"
	"os"
	"strconv"
)

var log = logrus.New()

type Server struct{}

type URI struct {
	Username   string
	PassWord   string
	SystemId   string
	HostKey    string
	HostPort   string
	ClientKey  string
	FileName   string
	FileSize   int64
	BufferSize int64
}

var ConnParams URI

//var UriParams = make ([]*URI,0)

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

/*
	This service gets a single request for a file from an SFTP server.  It goes to the server and sends back the file in
	chunks sized based on the buffer_size variable.
*/
func (*Server) Get(ctx context.Context, req *sftppb.SrvGetRequest) *sftppb.SrvGetResponse {
	log.Info("Get Service function was invoked with %v\n", req)

	ConnParams.Username = req.HostSftp.Username
	ConnParams.PassWord = req.HostSftp.PassWord
	ConnParams.SystemId = req.HostSftp.SystemId
	ConnParams.HostKey = req.HostSftp.HostKey
	ConnParams.HostPort = req.HostSftp.HostPort
	ConnParams.ClientKey = req.HostSftp.ClientKey
	ConnParams.FileName = req.HostSftp.FileName
	ConnParams.FileSize = req.HostSftp.FileSize
	ConnParams.BufferSize = req.HostSftp.BufferSize

	var bytes_Read int64 = 0
	if ConnParams.Username != "" && ConnParams.PassWord != "" {
		if ConnParams.SystemId != "" && ConnParams.HostPort != "" {

			conn, err := cp.ConnectionPool(ConnParams.Username, ConnParams.PassWord, ConnParams.SystemId, ConnParams.HostKey, ConnParams.HostPort, ConnParams.ClientKey, ConnParams.FileName, ConnParams.FileSize)
			if err != nil {
				log.Error(err)
			}
			readsys := iread.UriReader{ConnParams.Username,
				ConnParams.PassWord,
				ConnParams.SystemId,
				ConnParams.HostKey,
				ConnParams.HostPort,
				ConnParams.ClientKey,
				ConnParams.FileName,
				ConnParams.FileSize,
				ConnParams.BufferSize,
				true}
			writesys := iread.UriWriter{ConnParams.Username,
				ConnParams.PassWord,
				ConnParams.SystemId,
				ConnParams.HostKey,
				ConnParams.HostPort,
				ConnParams.ClientKey,
				ConnParams.FileName,
				ConnParams.FileSize,
				ConnParams.BufferSize,
				true}
			bytesRead, err := iread.IReader(readsys, writesys, conn)
			if err != nil {
				log.Error(err)
			}
			if bytesRead <= 1 {
				log.Error("It looks like nothing was done. There was a total of  %v bytes read ", bytesRead)
			}

			read_sys := iwrite.UriReader{ConnParams.Username,
				ConnParams.PassWord,
				ConnParams.SystemId,
				ConnParams.HostKey,
				ConnParams.HostPort,
				ConnParams.ClientKey,
				ConnParams.FileName,
				ConnParams.FileSize,
				ConnParams.BufferSize,
				true}
			write_sys := iwrite.UriWriter{ConnParams.Username,
				ConnParams.PassWord,
				ConnParams.SystemId,
				ConnParams.HostKey,
				ConnParams.HostPort,
				ConnParams.ClientKey,
				ConnParams.FileName,
				ConnParams.FileSize,
				ConnParams.BufferSize,
				true}
			bytes_Read, err := iwrite.IWriter(read_sys, write_sys, conn)
			if err != nil {
				log.Error(err)
			}

			if bytes_Read <= 1 {
				log.Error("It looks like nothing was done. There was a total of  %v bytes read ", bytes_Read)
				return nil
			}
		}
	}
	log.Info("%d bytes copied\n", bytes_Read)
	res := &sftppb.SrvGetResponse{
		Result: strconv.FormatInt(bytes_Read, 10),
	}
	log.Info(res.String())
	return res
}

func (*Server) Put(ctx context.Context, req *sftppb.SrvPutRequest) *sftppb.SrvPutResponse {
	log.Info("CopyFromRemoteService function was invoked with %v\n", req)

	ConnParams.Username = req.HostSftp.Username
	ConnParams.PassWord = req.HostSftp.PassWord
	ConnParams.SystemId = req.HostSftp.SystemId
	ConnParams.HostKey = req.HostSftp.HostKey
	ConnParams.HostPort = req.HostSftp.HostPort
	ConnParams.ClientKey = req.HostSftp.ClientKey
	ConnParams.FileName = req.HostSftp.FileName
	ConnParams.FileSize = req.HostSftp.FileSize
	ConnParams.BufferSize = req.HostSftp.BufferSize

	var bytes_Read int64 = 0
	if ConnParams.Username != "" && ConnParams.PassWord != "" {
		if ConnParams.SystemId != "" && ConnParams.HostPort != "" {

			read_conn, err := cp.ConnectionPool(ConnParams.Username, ConnParams.PassWord, ConnParams.SystemId, ConnParams.HostKey, ConnParams.HostPort, ConnParams.ClientKey, ConnParams.FileName, ConnParams.FileSize)
			if err != nil {
				log.Error(err)
			}
			readsys := iread.UriReader{ConnParams.Username,
				ConnParams.PassWord,
				ConnParams.SystemId,
				ConnParams.HostKey,
				ConnParams.HostPort,
				ConnParams.ClientKey,
				ConnParams.FileName,
				ConnParams.FileSize,
				ConnParams.BufferSize,
				true}

			writesys := iread.UriWriter{ConnParams.Username,
				ConnParams.PassWord,
				ConnParams.SystemId,
				ConnParams.HostKey,
				ConnParams.HostPort,
				ConnParams.ClientKey,
				ConnParams.FileName,
				ConnParams.FileSize,
				ConnParams.BufferSize,
				true}
			bytesRead, err := iread.IReader(readsys, writesys, read_conn)
			if err != nil {
				log.Error(err)
			}
			if bytesRead <= 1 {
				log.Error("It looks like nothing was done. There was a total of  %v bytes read ", bytesRead)
			}

			read_sys := iwrite.UriReader{ConnParams.Username,
				ConnParams.PassWord,
				ConnParams.SystemId,
				ConnParams.HostKey,
				ConnParams.HostPort,
				ConnParams.ClientKey,
				ConnParams.FileName,
				ConnParams.FileSize,
				ConnParams.BufferSize,
				true}
			write_sys := iwrite.UriWriter{ConnParams.Username,
				ConnParams.PassWord,
				ConnParams.SystemId,
				ConnParams.HostKey,
				ConnParams.HostPort,
				ConnParams.ClientKey,
				ConnParams.FileName,
				ConnParams.FileSize,
				ConnParams.BufferSize,
				true}
			bytes_Read, err := iwrite.IWriter(read_sys, write_sys, read_conn)
			if err != nil {
				log.Error(err)
			}

			if bytes_Read <= 1 {
				log.Error("It looks like nothing was done. There was a total of  %v bytes read ", bytes_Read)
				return nil
			}
		}
	}
	log.Info("%d bytes copied\n", bytes_Read)
	res := &sftppb.SrvPutResponse{
		Result: strconv.FormatInt(bytes_Read, 10),
	}
	log.Info(res.String())
	return res
}
