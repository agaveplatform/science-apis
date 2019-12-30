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
	agaveproto "github.com/agaveplatform/science-apis/agave-transfers/sftp-relay/pkg/sftpproto"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"os"
)

// getCmd represents the get command
var getCmd = &cobra.Command{
	Use:   "get",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		//log.Debugf("Get Command =====================================")
		//log.Debugf("localPath = %v", localPath)
		//log.Debugf("remotePath = %v", remotePath)

		conn, err := grpc.Dial(grpcservice, grpc.WithInsecure())
		if err != nil {
			//log.Fatalf("Could not connect: %v", err)
		}
		defer conn.Close()

		sftpRelay := agaveproto.NewSftpRelayClient(conn)

		//log.Tracef("Starting Push rpc client: ")
		//startPushtime := time.Now()
		//log.Debugf("Start Time = %v", startPushtime)

		req := &agaveproto.SrvGetRequest{
			SystemConfig: helper.ParseSftpConfig(cmd.Flags()),
			RemotePath: remotePath,
			LocalPath: localPath,
			Force: force,
			Range: byteRange,
		}
		//log.Tracef("Connecting to grpc service at: %s:%d", host, port)

		res, err := sftpRelay.Get(context.Background(), req)
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
				fmt.Printf("Transfer complete: %s (%d bytes)", res.RemoteFileInfo.Path, res.BytesTransferred)
			}
		}
		//log.Debugf("%v", res)
		//log.Infof("End Time %f", time.Since(startPushtime).Seconds())
		//log.Info("RPC Get Time: " + strconv.FormatFloat(secs, 'f', -1, 64))
	},
}

func init() {
	rootCmd.AddCommand(getCmd)

	getCmd.Flags().StringVarP(&localPath, "localPath", "s", DefaultLocalPath, "Path of the local file item.")
	getCmd.Flags().BoolVarP(&force, "force", "f", DefaultForce, "Force true = overwrite the existing remote file.")
	getCmd.Flags().StringVarP(&byteRange, "byteRange", "b", DefaultByteRange, "Byte range to fetch.  Defaults to the entire file")


	viper.BindPFlag("localPath", getCmd.Flags().Lookup("localPath"))
	viper.BindPFlag("force", getCmd.Flags().Lookup("force"))
	viper.BindPFlag("byteRange", getCmd.Flags().Lookup("byteRange"))
}
