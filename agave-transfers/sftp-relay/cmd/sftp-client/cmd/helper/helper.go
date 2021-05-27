package helper

import (
	agaveproto "github.com/agaveplatform/science-apis/agave-transfers/sftp-relay/pkg/sftpproto"
	"github.com/spf13/viper"

	"github.com/spf13/pflag"
	"google.golang.org/grpc"
)


func NewGrpcServiceConn() (*grpc.ClientConn, error) {
	grpcservice := viper.GetString("grpcservice")
	return grpc.Dial(grpcservice, grpc.WithInsecure())
}

func ParseSftpConfig(flags *pflag.FlagSet) *agaveproto.RemoteSystemConfig {
	sshHost := viper.GetString("host")
	sshPort := viper.GetInt32("port")
	username := viper.GetString("username")
	password := viper.GetString("passwd")
	pemKey := viper.GetString("privateKey")
	//remotePath, _ := flags.GetString("dest")
	//localPath, _ := flags.GetString("src")
	//force, _ := flags.GetBool("force")
	//append, _ := flags.GetBool("append")
	//byteRange, _ := flags.GetString("range")

	//fmt.Println(&agaveproto.Sftp )

	return &agaveproto.RemoteSystemConfig{
		Host:     	sshHost,
		Port:     	sshPort,
		Username:   username,
		Password:   password,
		PrivateKey: pemKey,
	}


}
