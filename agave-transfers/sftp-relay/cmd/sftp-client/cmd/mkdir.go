/*
Copyright © 2019 NAME HERE <EMAIL ADDRESS>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package cmd

import (
	"context"
	"fmt"
	"github.com/agaveplatform/science-apis/agave-transfers/sftp-relay/cmd/sftp-client/cmd/helper"
	sftppb "github.com/agaveplatform/science-apis/agave-transfers/sftp-relay/pkg/sftpproto"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"os"
)

var recursive = false

// getCmd represents the get command
var mkdirCmd = &cobra.Command{
	Use:   "mkdir",
	Short: "Creates the given directory path on the remote host",
	Long: `Creates the full directory path on the remote host. Equivalent to the mkdir 
command in linux.`,
	Run: func(cmd *cobra.Command, args []string) {
		//log.Infof("Mkdir Command =====================================")
		//log.Infof("remotePath = %s", remotePath)

		conn, err := helper.NewGrpcServiceConn()
		if err != nil {
			fmt.Printf("Unable to establish a connection to service at %s: %v", grpcservice, err.Error())
			os.Exit(1)
		}
		defer conn.Close()

		sftpRelay := sftppb.NewSftpRelayClient(conn)

		//log.Println("Starting Mkdirs rpc client: ")
		//startPushtime := time.Now()
		//log.Printf("Start Time = %v", startPushtime)

		req := &sftppb.SrvMkdirRequest{
			SystemConfig: helper.ParseSftpConfig(cmd.Flags()),
			RemotePath: remotePath,
			Recursive: recursive,
		}
		//log.Debugf("Connecting to grpc service at: %s:%d", host, port)

		res, err := sftpRelay.Mkdir(context.Background(), req)
		//secs := time.Since(startPushtime).Seconds()
		if err != nil {
			fmt.Printf("Error while calling gRPC Remove: %s\n", err.Error())
			os.Exit(1)
		} else if res == nil {
			fmt.Println("Empty response received from gRPC server")
		} else {
			if res.Error != "" {
				fmt.Println(res.Error)
			} else {
				fmt.Printf("%s\n", res.RemoteFileInfo.Path)
			}
		}
		//log.Debugf("%v", res)
		//log.Infof("End Time %f", time.Since(startPushtime).Seconds())
		//log.Info("RPC Get Time: " + strconv.FormatFloat(secs, 'f', -1, 64))
	},
}

func init() {
	rootCmd.AddCommand(mkdirCmd)

	mkdirCmd.Flags().BoolVarP(&recursive, "recursive", "r", false, " Create intermediate directories as required.  If this option is not specified, the full path prefix of each operand must already exist.  On the other hand, with this option specified, no error will be reported if a directory given as an operand already exists.")

	viper.BindPFlag("recursive", mkdirCmd.Flags().Lookup("recursive"))
}
