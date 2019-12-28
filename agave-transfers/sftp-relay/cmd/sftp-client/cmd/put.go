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
	sftppb "github.com/agaveplatform/science-apis/agave-transfers/sftp-relay/pkg/sftpproto"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"io"
	"os"
	"strconv"
	"time"
)

//var log = logrus.New()

// putCmd represents the put command
var putCmd = &cobra.Command{
	Use:   "put",
	Short: "Perform a PUT operations",
	Long:  `Performs a put operation copying a file on the relay server to a remote server`,
	Run: func(cmd *cobra.Command, args []string) {
		log.Infof("Put Command =====================================")
		log.Infof("localPath = %v", localPath)
		log.Infof("remotePath = %v", remotePath)

		// log to console and file
		f, err := os.OpenFile("sftp-client.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			log.Fatalf("Error opening file: %v", err)
		}
		wrt := io.MultiWriter(os.Stdout, f)
		log.SetOutput(wrt)

		log.Infof("connecting to %v...", grpcservice)

		conn, err := grpc.Dial(grpcservice, grpc.WithInsecure())
		if err != nil {
			log.Fatalf("could not connect: %v", err)
		}
		log.Infof("Connected to %s", conn.Target())
		defer conn.Close()

		sftpRelay := sftppb.NewSftpRelayClient(conn)

		log.Infof("Starting Push rpc client: ")
		startPushtime := time.Now()
		log.Infof("Start Time = %v", startPushtime)

		req := &sftppb.SrvPutRequest{
			SystemConfig: helper.ParseSftpConfig(cmd.Flags()),
			RemotePath: remotePath,
			LocalPath: localPath,
			Force: force,
			Append: append,
		}
		log.Debugf("Connecting to grpc service at: %s:%d", host, port)

		res, err := sftpRelay.Put(context.Background(), req)
		secs := time.Since(startPushtime).Seconds()
		if err != nil {
			log.Errorf("Error while calling gRPC Put: %v", err)
			//log.Exit(1)
		} else if res == nil {
			log.Error("Empty response received from gRPC server")
			//log.Exit(1)
		} else {
			if res.Error != "" {
				log.Errorf("Error response: %v", res.Error)
				//log.Exit(1)
			} else {
				log.Printf("Transfer complete: %s (%d bytes)", res.RemoteFileInfo.Path, res.BytesTransferred)
			}
			log.Debugf("%v", res)
			log.Info("gRPC Put Time: " + strconv.FormatFloat(secs, 'f', -1, 64))
		}
	},
}

func init() {
	rootCmd.AddCommand(putCmd)

	putCmd.Flags().StringVarP(&localPath, "localPath", "s", DefaultLocalPath, "Path of the local file item.")
	putCmd.Flags().BoolVarP(&force, "force", "f", DefaultForce, "Force true = overwrite the existing local file.")
	putCmd.Flags().BoolVarP(&append, "append", "a", DefaultAppend, "Append true = append the contents to the remote path.")

	viper.BindPFlag("localPath", putCmd.Flags().Lookup("localPath"))
	viper.BindPFlag("force", putCmd.Flags().Lookup("force"))
	viper.BindPFlag("append", putCmd.Flags().Lookup("append"))

}
