package helper

import (
	"fmt"
	agaveproto "github.com/agaveplatform/science-apis/agave-transfers/sftp-relay/pkg/sftpproto"

	"github.com/spf13/pflag"
	"google.golang.org/grpc"
)


func GetSftpRelay() agaveproto.SftpRelayClient {
	fmt.Println("Starting SFTP client")

	conn, err := grpc.Dial("0.0.0.0:50051", grpc.WithInsecure())
	if err != nil {
		//log.Fatalf("could not connect: %v", err)
	}
	//log.Printf("Got a connection to the grpc server on %v", conn.Target())
	defer conn.Close()

	return agaveproto.NewSftpRelayClient(conn)
}

func ParseSftpConfig(flags *pflag.FlagSet) *agaveproto.RemoteSystemConfig {
	hostname, _ := flags.GetString("host")
	port, _ := flags.GetInt("port")
	username, _ := flags.GetString("username")
	password, _ := flags.GetString("passwd")
	privateKey, _ := flags.GetString("key")
	publicKey, _ := flags.GetString("pubkey")
	//remotePath, _ := flags.GetString("dest")
	//localPath, _ := flags.GetString("src")
	//force, _ := flags.GetBool("force")
	//append, _ := flags.GetBool("append")
	//byteRange, _ := flags.GetString("range")

	//fmt.Println(&agaveproto.Sftp )

	return &agaveproto.RemoteSystemConfig{
		Host:     	hostname,
		Port:     	int32(port),
		Username:   username,
		Password:   password,
		PrivateKey: privateKey,
		PublicKey:  publicKey,

	}
}
