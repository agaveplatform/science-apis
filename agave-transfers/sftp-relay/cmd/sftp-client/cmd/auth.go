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

// authCmd represents the auth command
var authCmd = &cobra.Command{
	Use:   "auth",
	Short: "Test authentication to the remote host",
	Long:  `Attempts to login to the remote sftp host`,
	Run: func(cmd *cobra.Command, args []string) {
		log.Println("Auth command")

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

		req := &agaveproto.AuthenticationCheckRequest{
			SystemConfig: helper.ParseSftpConfig(cmd.Flags()),
		}
		log.Tracef("Connecting to grpc service at: %s:%d", host, port)

		res, err := sftpRelay.AuthCheck(context.Background(), req)
		secs := time.Since(startPushtime).Seconds()
		if err != nil {
			log.Fatalf("Error while calling RPC auth: %v", err)
		}

		if res.Error != "" {
			log.Errorf("Error response: %s", res.Error)
		} else {
			log.Println("Authentication success")
		}
		log.Debugf("%v", res)
		log.Info("RPC Auth Time: " + strconv.FormatFloat(secs, 'f', -1, 64))
	},
}

func init() {
	rootCmd.AddCommand(authCmd)
}
