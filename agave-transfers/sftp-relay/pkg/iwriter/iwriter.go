package iwriter

import (
	"github.com/pkg/sftp"
	"github.com/sirupsen/logrus"
	"io"
	"os"
	"strconv"
)
var log = logrus.New()

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


func IWriter(URI)  {
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
