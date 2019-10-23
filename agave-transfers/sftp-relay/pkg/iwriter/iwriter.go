package iwriter

import (
	"github.com/pkg/sftp"
	"github.com/sirupsen/logrus"
	"golang.org/x/crypto/ssh"
	"io"
	"os"
)

var log = logrus.New()

type UriReader struct {
	Username   string
	PassWord   string
	SystemId   string
	HostKey    string
	HostPort   string
	ClientKey  string
	FileName   string
	FileSize   int64
	BufferSize int64
	IsLocal    bool
}
type UriWriter struct {
	Username   string
	PassWord   string
	SystemId   string
	HostKey    string
	HostPort   string
	ClientKey  string
	FileName   string
	FileSize   int64
	BufferSize int64
	IsLocal    bool
}

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

func IWriter(uriReader UriReader, UriWriter UriWriter, conn *ssh.Client) (int64, error) {
	log.Info("Streaming Get Service function was invoked with")

	log.Info(uriReader.FileSize)
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
	//TODO make a unique file name here /scratch/filename
	dstFile, err := os.Create(uriReader.FileName)
	if err != nil {
		log.Info("Error creating dest file", err)
		result = err.Error()
	}
	defer dstFile.Close()

	// open source file
	srcFile, err := client.Open(uriReader.FileName)
	if err != nil {
		log.Info("Opening source file: ", err)
		result = err.Error()
	}

	bytesWritten, err := srcFile.WriteTo(dstFile)
	if err != nil {
		log.Info("Error writing to file: ", err)
	}
	log.Info("%d bytes copied\n", bytesWritten)

	return bytesWritten, nil
}
