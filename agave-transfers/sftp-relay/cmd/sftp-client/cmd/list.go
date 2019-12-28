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
	"github.com/agaveplatform/science-apis/agave-transfers/sftp-relay/cmd/sftp-client/cmd/helper"
	agaveproto "github.com/agaveplatform/science-apis/agave-transfers/sftp-relay/pkg/sftpproto"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"io"
	"os"
	"strconv"
	"time"
)

// listCmd represents the ls command
var listCmd = &cobra.Command{
	Use:   "list",
	Short: "Lists a remote directory",
	Long: `Fetches the directory listing of the remote path. Equivalent 
 to the ls -al command in linux.`,
	Run: func(cmd *cobra.Command, args []string) {
		log.Infof("List Command =====================================")
		log.Infof("remotePath = %v", remotePath)

		// log to console and file
		f, err := os.OpenFile("sftp-client.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			log.Fatalf("Error opening file: %v", err)
		}
		wrt := io.MultiWriter(os.Stdout, f)
		log.SetOutput(wrt)

		conn, err := grpc.Dial(grpcservice, grpc.WithInsecure())
		if err != nil {
			log.Fatalf("could not connect: %v", err)
		}
		defer conn.Close()

		sftpRelay := agaveproto.NewSftpRelayClient(conn)

		log.Println("Starting Remove rpc client: ")
		startPushtime := time.Now()
		log.Printf("Start Time = %v", startPushtime)

		req := &agaveproto.SrvListRequest{
			SystemConfig: helper.ParseSftpConfig(cmd.Flags()),
			RemotePath: remotePath,
		}
		log.Debugf("Connecting to grpc service at: %s:%d", host, port)

		res, err := sftpRelay.List(context.Background(), req)
		secs := time.Since(startPushtime).Seconds()
		if err != nil {
			log.Errorf("Error while calling gRPC Remove: %v", err)
		}
		if res == nil {
			log.Error("Empty response received from gRPC server")
		}
		log.Infof("End Time %f", time.Since(startPushtime).Seconds())

		if res.Error != "" {
			log.Errorf("Error message: %s", res.Error)
		} else {
			for _,remoteFileInfo := range res.Listing {
				log.Printf("%s %s %s %s %d %s", remoteFileInfo.Mode, username, username, time.Unix(remoteFileInfo.LastUpdated, 0).String(), remoteFileInfo.Size, remoteFileInfo.Name)
			}
			log.Debugf("%v", res)
		}
		log.Println("RPC Get Time: " + strconv.FormatFloat(secs, 'f', -1, 64))
	},
}

func init() {
	rootCmd.AddCommand(listCmd)

}
