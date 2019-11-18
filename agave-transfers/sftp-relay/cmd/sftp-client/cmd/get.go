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
		log.Println("Get Command =====================================")
		log.Printf("srce file= %v \n", src)
		log.Printf("dst file= %v \n", dest)

		// log to console and file
		f, err := os.OpenFile("SFTPServer.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			log.Fatalf("error opening file: %v", err)
		}
		wrt := io.MultiWriter(os.Stdout, f)
		log.SetOutput(wrt)

		conn, err := grpc.Dial(host+":"+Grpcport, grpc.WithInsecure())
		if err != nil {
			log.Fatalf("could not connect: %v", err)
		}
		defer conn.Close()

		sftpRelay := sftppb.NewSftpRelayClient(conn)

		log.Println("Starting Push rpc client: ")
		startPushtime := time.Now()
		log.Printf("Start Time = %v", startPushtime)

		req := &sftppb.SrvGetRequest{
			SrceSftp: &sftppb.Sftp{
				Username:     username,
				PassWord:     passwd,
				SystemId:     host,
				HostKey:      "",
				FileName:     src,
				FileSize:     0,
				HostPort:     ":" + strconv.Itoa(port),
				ClientKey:    key,
				BufferSize:   16138,
				Type:         "SFTP",
				DestFileName: dest,
			},
		}
		log.Println("got past req :=")

		res, err := sftpRelay.Get(context.Background(), req)
		if err != nil {
			log.Errorf("error while calling gRPC Put: %v", err)
		}
		if res == nil {
			log.Errorf("error with res varable while calling RPC")
		}
		log.Println("End Time %s", time.Since(startPushtime).Seconds())
		secs := time.Since(startPushtime).Seconds()
		if res.Error != "" {
			log.Printf("Response from Error Code: %v \n", res.Error)
		} else {
			log.Printf("Response from FileName: %v \n", res.FileName)
			log.Printf("Response from BytesReturned: %v \n", res.BytesReturned)
		}
		log.Println("RPC Get Time: " + strconv.FormatFloat(secs, 'f', -1, 64))
	},
}

func init() {
	rootCmd.AddCommand(getCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// getCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// getCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
	getCmd.Flags().StringVarP(&src, "src", "s", DefaultSrc, "Path of the source file item.")
	//getCmd.MarkFlagRequired("src")
	getCmd.Flags().StringVarP(&dest, "dest", "d", DefaultDest, "Path of the dest file item.")
	//rootCmd.MarkFlagRequired("dest")

	viper.BindPFlag("src", rootCmd.Flags().Lookup("src"))
	viper.BindPFlag("dest", rootCmd.Flags().Lookup("dest"))

}
