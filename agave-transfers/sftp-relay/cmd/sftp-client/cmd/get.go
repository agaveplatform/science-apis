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
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"io"
	"os"
	"strconv"
	"time"
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
		log.Debugf("Get Command =====================================")
		log.Debugf("localPath = %v", localPath)
		log.Debugf("remotePath = %v", remotePath)

		// log to console and file
		f, err := os.OpenFile("sftp-client.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			log.Fatalf("Error opening file: %v", err)
		}
		wrt := io.MultiWriter(os.Stdout, f)
		log.SetOutput(wrt)

		conn, err := grpc.Dial(grpcservice, grpc.WithInsecure())
		if err != nil {
			log.Fatalf("Could not connect: %v", err)
		}
		defer conn.Close()

		sftpRelay := agaveproto.NewSftpRelayClient(conn)

		log.Tracef("Starting Push rpc client: ")
		startPushtime := time.Now()
		log.Debugf("Start Time = %v", startPushtime)

		req := &agaveproto.SrvGetRequest{
			SystemConfig: helper.ParseSftpConfig(cmd.Flags()),
			RemotePath: remotePath,
			LocalPath: localPath,
			Force: force,
			Range: byteRange,
		}
		log.Tracef("Connecting to grpc service at: %s:%d", host, port)

		res, err := sftpRelay.Get(context.Background(), req)
		secs := time.Since(startPushtime).Seconds()
		if err != nil {
			log.Errorf("Error while calling gRPC Put: %v", err)
			log.Exit(1)
		}
		if res == nil {
			log.Error("Empty response received from gRPC server")
			log.Exit(1)
		}
		log.Infof("End Time %s", time.Since(startPushtime).Seconds())
		if res.Error != "" {
			log.Errorf("Error response: %s", res.Error)
			log.Exit(1)
		} else {
			log.Printf("Transfer complete: %s (%d bytes)", res.RemoteFileInfo.Path, res.BytesTransferred)
			log.Debugf("%v", res)
		}
		log.Debugf("%v", res)
		log.Info("RPC Get Time: " + strconv.FormatFloat(secs, 'f', -1, 64))
	},
}

func init() {
	rootCmd.AddCommand(getCmd)

	getCmd.Flags().StringVarP(&localPath, "localPath", "s", DefaultLocalPath, "Path of the local file item.")
	getCmd.Flags().BoolVarP(&force, "force", "f", DefaultForce, "Force true = overwrite the existing remote file.")
	getCmd.Flags().StringVarP(&byteRange, "byteRange", "b", DefaultByteRange, "Byte range to fetch.  Defaults to the entire file")


	viper.BindPFlag("localPath", putCmd.Flags().Lookup("localPath"))
	viper.BindPFlag("force", putCmd.Flags().Lookup("force"))
	viper.BindPFlag("byteRange", putCmd.Flags().Lookup("byteRange"))
}
