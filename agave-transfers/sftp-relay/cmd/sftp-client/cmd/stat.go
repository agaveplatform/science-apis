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
	"os"
	"time"
)

// getCmd represents the get command
var statCmd = &cobra.Command{
	Use:   "stat",
	Short: "Makes a remote stat on a file or folder",
	Long: `Fetches the file metadata for a remote file or directory. Equivalent 
 to the rm -rf command in linux.`,
	Run: func(cmd *cobra.Command, args []string) {
		//log.Infof("Stat Command =====================================")
		//log.Infof("remotePath = %v", remotePath)

		conn, err := helper.NewGrpcServiceConn()
		if err != nil {
			fmt.Printf("Unable to establish a connection to service at %s: %v", grpcservice, err.Error())
			os.Exit(1)
		}
		defer conn.Close()

		sftpRelay := agaveproto.NewSftpRelayClient(conn)

		//log.Println("Starting Remove rpc client: ")
		//startPushtime := time.Now()
		//log.Printf("Start Time = %v", startPushtime)

		req := &agaveproto.SrvStatRequest{
			SystemConfig: helper.ParseSftpConfig(cmd.Flags()),
			RemotePath: remotePath,
		}
		//log.Debugf("Connecting to grpc service at: %s:%d", host, port)

		res, err := sftpRelay.Stat(context.Background(), req)
		//secs := time.Since(startPushtime).Seconds()
		if err != nil {
			fmt.Println("Error while calling gRPC Remove: %s\n", err.Error())
			os.Exit(1)
		} else if res == nil {
			fmt.Println("Empty response received from gRPC server")
		} else {
			if res.Error != "" {
				fmt.Printf("%s\n",res.Error)
			} else {
				fmt.Printf("%s\t%s\t%s\t%s\t%d\t%s\n", res.RemoteFileInfo.Mode, username, username, time.Unix(res.RemoteFileInfo.LastUpdated, 0).String(), res.RemoteFileInfo.Size, res.RemoteFileInfo.Name)
				//fmt.Printf("Name: %s\n", res.RemoteFileInfo.Name)
				//fmt.Printf("Path: %s", res.RemoteFileInfo.Path)
				//fmt.Printf("Mode: %s", res.RemoteFileInfo.Mode)
				//fmt.Printf("Size: %d", res.RemoteFileInfo.Size)
				//fmt.Printf("LastUpdated: %s", time.Unix(res.RemoteFileInfo.LastUpdated, 0).String())
				//fmt.Printf("IsDirector: %b", res.RemoteFileInfo.IsDirectory)
				//fmt.Printf("IsLink: %b", res.RemoteFileInfo.IsLink)
			}
		}
		//log.Debugf("%v", res)
		//log.Infof("End Time %f", time.Since(startPushtime).Seconds())
		//log.Info("RPC Get Time: " + strconv.FormatFloat(secs, 'f', -1, 64))

	},
}

func init() {
	rootCmd.AddCommand(statCmd)

}
