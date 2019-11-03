package helper

import (
	"fmt"
	agaveproto "github.com/agaveplatform/science-apis/agave-transfers/sftp-relay/pkg/sftpproto"
	"github.com/spf13/pflag"
	"google.golang.org/grpc"
	"log"
)

func GetSftpRelay() agaveproto.SFTPRelayClient {
	fmt.Println("Starting SFTP client")

	conn, err := grpc.Dial("localhost:50051", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("could not connect: %v", err)
	}
	defer conn.Close()

	return agaveproto.NewSFTPRelayClient(conn)
}

func ParseSftpConfig(flags *pflag.FlagSet) *agaveproto.SftpConfig {
	hostname, _ := flags.GetString("host")
	port, _ := flags.GetInt("port")
	username, _ := flags.GetString("username")
	password, _ := flags.GetString("passwd")
	key, _ := flags.GetString("key")

	return  &agaveproto.SftpConfig{
		Host:                 hostname,
		Port:                 fmt.Sprintf("%d", port),
		Username:             username,
		Password:             []byte(password),
		Key:                  []byte(key),
	}
}

