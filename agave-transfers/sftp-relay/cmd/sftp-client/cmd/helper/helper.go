package helper

import (
	"fmt"
	agaveproto "github.com/agaveplatform/science-apis/agave-transfers/sftp-relay/pkg/sftpproto"
	"github.com/spf13/pflag"
	"google.golang.org/grpc"
	"log"
)

func GetSftpRelay() agaveproto.SftpRelayClient {
	fmt.Println("Starting SFTP client")

	conn, err := grpc.Dial("localhost:50051", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("could not connect: %v", err)
	}
	defer conn.Close()

	return agaveproto.NewSftpRelayClient(conn)
}

func ParseSftpConfig(flags *pflag.FlagSet) *agaveproto.Sftp {
	hostname, _ := flags.GetString("host")
	port, _ := flags.GetInt("port")
	username, _ := flags.GetString("username")
	password, _ := flags.GetString("passwd")
	key, _ := flags.GetString("key")
	dest, _ := flags.GetString("dest")
	src, _ := flags.GetString("src")

	return &agaveproto.Sftp{
		SystemId:     hostname,
		HostPort:     fmt.Sprintf("%d", port),
		Username:     username,
		PassWord:     password,
		HostKey:      key,
		DestFileName: dest,
		FileName:     src,
		Type:         "SFTP",
	}
}
