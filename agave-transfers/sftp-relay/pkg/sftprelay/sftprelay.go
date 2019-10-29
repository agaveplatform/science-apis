package sftprelay

import (
	connPool "github.com/agaveplatform/science-apis/agave-transfers/sftp-relay/pkg/connectionpool"
	factory "github.com/agaveplatform/science-apis/agave-transfers/sftp-relay/pkg/factory"
	sftp "github.com/agaveplatform/science-apis/agave-transfers/sftp-relay/pkg/sftp"
	sftppb "github.com/agaveplatform/science-apis/agave-transfers/sftp-relay/pkg/sftpproto"
	"github.com/sirupsen/logrus"
	"golang.org/x/crypto/ssh"
	"golang.org/x/net/context"
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
	TxfrType   string // Valid values are:  FTP, AZURE, S3, SWIFT, SFTP, GRIDFTP, IRODS, IRODS4, LOCAL
}

type ConnParams struct {
	Srce SysUri
	Dest SysUri
}

func (c ConnParams) ReadFrom() factory.FileTransfer {
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

	params := setParams(req) // return type is ConnParams

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
	toFile := factory.TransferFactory.ReadFrom(params)
	var bytesReadf int64 = 0

	log.Info(from)
	log.Info(to)
	log.Info(toFile)

	// Read From system
	factory.GetSourceType(from)
	//factory.GetDestType(to)

	if params.Srce.TxfrType == "SFTP" {
		f, bytesRead, err := sftp.ReadFrom(params, Conn)
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

	//		read_sys := iwrite.UriReader{ConnParams.Username,
	//			ConnParams.PassWord,
	//			ConnParams.SystemId,
	//			ConnParams.HostKey,
	//			ConnParams.HostPort,
	//			ConnParams.ClientKey,
	//			ConnParams.FileName,
	//			ConnParams.FileSize,
	//			ConnParams.BufferSize,
	//			true}
	//		write_sys := iwrite.UriWriter{ConnParams.Username,
	//			ConnParams.PassWord,
	//			ConnParams.SystemId,
	//			ConnParams.HostKey,
	//			ConnParams.HostPort,
	//			ConnParams.ClientKey,
	//			ConnParams.FileName,
	//			ConnParams.FileSize,
	//			ConnParams.BufferSize,
	//			true}
	//		bytes_Read, err := iwrite.IWriter(read_sys, write_sys, conn)
	//		if err != nil {
	//			log.Error(err)
	//		}
	//
	//		if bytes_Read <= 1 {
	//			log.Error("It looks like nothing was done. There was a total of  %v bytes read ", bytes_Read)
	//			return nil
	//		}
	//	}
	//}

	log.Info("%d bytes copied\n", bytesReadf)
	res := &sftppb.SrvGetResponse{
		Result: strconv.FormatInt(bytesReadf, 10),
	}
	log.Info(res.String())
	return res
}

//func (*Server) Put(ctx context.Context, req *sftppb.SrvPutRequest) *sftppb.SrvPutResponse {
//	log.Info("CopyFromRemoteService function was invoked with %v\n", req)
//
//	ConnParams.Username = req.HostSftp.Username
//	ConnParams.PassWord = req.HostSftp.PassWord
//	ConnParams.SystemId = req.HostSftp.SystemId
//	ConnParams.HostKey = req.HostSftp.HostKey
//	ConnParams.HostPort = req.HostSftp.HostPort
//	ConnParams.ClientKey = req.HostSftp.ClientKey
//	ConnParams.FileName = req.HostSftp.FileName
//	ConnParams.FileSize = req.HostSftp.FileSize
//	ConnParams.BufferSize = req.HostSftp.BufferSize
//
//	var bytes_Read int64 = 0
//	if ConnParams.Username != "" && ConnParams.PassWord != "" {
//		if ConnParams.SystemId != "" && ConnParams.HostPort != "" {
//
//			read_conn, err := cp.ConnectionPool(ConnParams.Username, ConnParams.PassWord, ConnParams.SystemId, ConnParams.HostKey, ConnParams.HostPort, ConnParams.ClientKey, ConnParams.FileName, ConnParams.FileSize)
//			if err != nil {
//				log.Error(err)
//			}
//			readsys := iread.UriReader{ConnParams.Username,
//				ConnParams.PassWord,
//				ConnParams.SystemId,
//				ConnParams.HostKey,
//				ConnParams.HostPort,
//				ConnParams.ClientKey,
//				ConnParams.FileName,
//				ConnParams.FileSize,
//				ConnParams.BufferSize,
//				true}
//
//			writesys := iread.UriWriter{ConnParams.Username,
//				ConnParams.PassWord,
//				ConnParams.SystemId,
//				ConnParams.HostKey,
//				ConnParams.HostPort,
//				ConnParams.ClientKey,
//				ConnParams.FileName,
//				ConnParams.FileSize,
//				ConnParams.BufferSize,
//				true}
//			bytesRead, err := iread.IReader(readsys, writesys, read_conn)
//			if err != nil {
//				log.Error(err)
//			}
//			if bytesRead <= 1 {
//				log.Error("It looks like nothing was done. There was a total of  %v bytes read ", bytesRead)
//			}
//
//			read_sys := iwrite.UriReader{ConnParams.Username,
//				ConnParams.PassWord,
//				ConnParams.SystemId,
//				ConnParams.HostKey,
//				ConnParams.HostPort,
//				ConnParams.ClientKey,
//				ConnParams.FileName,
//				ConnParams.FileSize,
//				ConnParams.BufferSize,
//				true}
//			write_sys := iwrite.UriWriter{ConnParams.Username,
//				ConnParams.PassWord,
//				ConnParams.SystemId,
//				ConnParams.HostKey,
//				ConnParams.HostPort,
//				ConnParams.ClientKey,
//				ConnParams.FileName,
//				ConnParams.FileSize,
//				ConnParams.BufferSize,
//				true}
//			bytes_Read, err := iwrite.IWriter(read_sys, write_sys, read_conn)
//			if err != nil {
//				log.Error(err)
//			}
//
//			if bytes_Read <= 1 {
//				log.Error("It looks like nothing was done. There was a total of  %v bytes read ", bytes_Read)
//				return nil
//			}
//		}
//	}
//	log.Info("%d bytes copied\n", bytes_Read)
//	res := &sftppb.SrvPutResponse{
//		Result: strconv.FormatInt(bytes_Read, 10),
//	}
//	log.Info(res.String())
//	return res
//}

func setParams(req *sftppb.SrvGetRequest) ConnParams {
	connParams := ConnParams{
		Srce: SysUri{Username: req.SrceSftp.Username, PassWord: req.SrceSftp.PassWord, SystemId: req.SrceSftp.SystemId, HostKey: req.SrceSftp.HostKey, HostPort: req.SrceSftp.HostPort, ClientKey: req.SrceSftp.ClientKey, FileName: req.SrceSftp.FileName, FileSize: req.SrceSftp.FileSize, BufferSize: req.SrceSftp.BufferSize, TxfrType: req.SrceSftp.Type},
		Dest: SysUri{Username: req.DestSftp.Username, PassWord: req.DestSftp.PassWord, SystemId: req.DestSftp.SystemId, HostKey: req.DestSftp.HostKey, HostPort: req.DestSftp.HostPort, ClientKey: req.DestSftp.ClientKey, FileName: req.DestSftp.FileName, FileSize: req.DestSftp.FileSize, BufferSize: req.DestSftp.BufferSize, TxfrType: req.DestSftp.Type},
	}
	return connParams
}
