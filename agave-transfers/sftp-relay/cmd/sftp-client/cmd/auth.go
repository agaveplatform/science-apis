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
	sftphelper "github.com/agaveplatform/science-apis/agave-transfers/sftp-relay/cmd/sftp-client/cmd/helper"
	agaveproto "github.com/agaveplatform/science-apis/agave-transfers/sftp-relay/pkg/sftpproto"
	"github.com/spf13/cobra"
	"log"
	"strconv"
	"time"
)

// authCmd represents the auth command
var authCmd = &cobra.Command{
	Use:   "auth",
	Short: "Test authentication to the remote host",
	Long: `Attempts to login to the remote sftp host`,
	Run: func(cmd *cobra.Command, args []string) {

		sftpRelay := sftphelper.GetSftpRelay()

		log.Println("Starting Auth rpc client: ")
		startTime := time.Now()

		req := &agaveproto.AuthenticateToRemoteRequest{
			SftpConfig: sftphelper.ParseSftpConfig(cmd.Flags()),
		}

		res, err := sftpRelay.AuthenticateToRemoteService(context.Background(), req)
		if err != nil {
			log.Fatalf("error while calling RPC auth: %v", err)
		}
		secs := time.Since(startTime).Seconds()
		log.Println("Response from Auth: " + strconv.FormatBool(res.GetResult()))
		log.Println("RPC Auth Time: " + strconv.FormatFloat(secs, 'f', -1, 64))
	},
}

func init() {
	rootCmd.AddCommand(authCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// authCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// authCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
