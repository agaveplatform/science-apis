package sftp

import (
	relay "github.com/agaveplatform/science-apis/agave-transfers/sftp-relay/pkg/sftprelay"
	"github.com/pkg/sftp"
	"github.com/sirupsen/logrus"
	"golang.org/x/crypto/ssh"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"time"
)

var log = logrus.New()

func init() {
	// log to console and file
	f, err := os.OpenFile("SFTPServer.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	wrt := io.MultiWriter(os.Stdout, f)
	rand.Seed(time.Now().UnixNano())
	log.SetOutput(wrt)
	log.Info("Set up loggin for server")
}

//The assumption is that we are going to copy a file from a remote server to a local temp file.
// it will then be processed by a second client that will take the client and move the file to a second remote server.
// this is synonymous to a get for sftp
func ReadFrom(params relay.ConnParams, conn *ssh.Client) (string, int64, error) { // return temp filename, bytecount, and error
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
func WriteTo(params relay.ConnParams, conn *ssh.Client, name string) { // name is a temporary file name that was created at the put end of the stream
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
