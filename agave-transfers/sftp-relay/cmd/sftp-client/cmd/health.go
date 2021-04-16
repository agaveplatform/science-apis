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
	"github.com/spf13/cobra"
	hpb "google.golang.org/grpc/health/grpc_health_v1"
	"os"
)

// healthCmd represents the health command
var healthCmd = &cobra.Command{
	Use:   "health",
	Aliases: []string{"status"},
	Short: "Health check on grpc server",
	Long: `Performs a synchronous health check on the grpc server 
by calling its "Check" method.`,
	Run: func(cmd *cobra.Command, args []string) {
		//log.Debugf("Put Command =====================================")
		//log.Debugf("localPath = %v", localPath)
		//log.Debugf("remotePath = %v", remotePath)

		//log.Tracef("Connecting to %v...", grpcservice)

		conn, err := helper.NewGrpcServiceConn()
		if err != nil {
			fmt.Printf("Unable to establish a connection to service at %s: %v", grpcservice, err.Error())
			os.Exit(1)
		}
		defer conn.Close()

		hclient := hpb.NewHealthClient(conn)
		//startPushtime := time.Now()
		//log.Debugf("Start Time = %v", startPushtime)

		resp, err := hclient.Check(context.Background(),&hpb.HealthCheckRequest{})
		//secs := time.Since(startPushtime).Seconds()

		if err != nil {
			fmt.Printf("Error while calling gRPC Health: %s\n", err.Error())
			os.Exit(1)
		} else {
			fmt.Printf("%s\n", resp.Status.String())
		}
		//log.Debugf("%v", resp)
		//log.Infof("End Time %f", time.Since(startPushtime).Seconds())
		//log.Info("RPC Get Time: " + strconv.FormatFloat(secs, 'f', -1, 64))
	},
}

func init() {
	rootCmd.AddCommand(healthCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// healthCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// healthCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
