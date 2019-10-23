package ireader

import (
	"github.com/pkg/sftp"
	"github.com/sirupsen/logrus"
	conn "./pkg/connectionpol"
	"io"
	"os"
)
var log = logrus.New()

type URI struct {
	Username string
	PassWord string
	SystemId string
	HostKey string
	HostPort string
	ClientKey string
	FileName string
	FileSize int64
	IsLocal bool
}

//var ConnUri = make ([]*URI,0)

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


func IReader(URI)  {
	log.Info("Streaming Get Service function was invoked with %v\n", req)

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
