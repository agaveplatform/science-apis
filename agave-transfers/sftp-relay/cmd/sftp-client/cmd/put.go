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
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"os"
	"path/filepath"
	"strings"
	"time"
)

//var log = logrus.New()

// putCmd represents the put command
var putCmd = &cobra.Command{
	Use:   "put",
	Short: "Perform a PUT operations",
	Long:  `Performs a put operation copying a file on the relay server to a remote server`,
	Run: func(cmd *cobra.Command, args []string) {
		//log.Infof("Put Command =====================================")
		//log.Infof("localPath = %v", localPath)
		//log.Infof("remotePath = %v", remotePath)

		//log.Infof("connecting to %v...", grpcservice)

		conn, err := helper.NewGrpcServiceConn()
		if err != nil {
			fmt.Printf("Unable to establish a connection to service at %s: %v", grpcservice, err.Error())
			os.Exit(1)
		}
		defer conn.Close()

		sftpRelay := sftppb.NewSftpRelayClient(conn)

		//log.Infof("Starting Push rpc client: ")
		//startPushtime := time.Now()
		//log.Infof("Start Time = %v", startPushtime)


		//log.Debugf("Connecting to grpc service at: %s:%d", host, port)

		localFileInfo, err := os.Stat(localPath)
		if err != nil {
			fmt.Printf("No such path found: %s\n", localPath)
			os.Exit(1)
		}

		var putStats TransferStats
		sftpConfig := helper.ParseSftpConfig(cmd.Flags())
		// recursively put directories
		if localFileInfo.IsDir() {
			putStats, err = putDir(sftpRelay, sftpConfig, localPath, remotePath)
			if err != nil {
				fmt.Printf(err.Error())
				os.Exit(1)
			}

			fmt.Printf("%s (%d bytes, %d files, %d ms)\n", localPath, putStats.Bytes, putStats.Files, putStats.Runtime)
		} else {
			putStats, err = putFile(sftpRelay, sftpConfig, localPath, remotePath)
			if err != nil {
				fmt.Printf(err.Error())
				os.Exit(1)
			}

			fmt.Printf("%s (%d bytes, %d ms)\n", localPath, putStats.Bytes, putStats.Runtime)
		}

		//log.Debugf("%v", res)
		//log.Infof("End Time %f", time.Since(startPushtime).Seconds())
		//log.Info("RPC Get Time: " + strconv.FormatFloat(secs, 'f', -1, 64))
	},
}

// upload a directory tree to the remote system using mkdir and put calls
func putDir(sftpRelay sftppb.SftpRelayClient, remoteSystemConfig *sftppb.RemoteSystemConfig, localDirPath string, remoteDirPath string) (TransferStats, error) {
	treeStats := TransferStats{0,0,0,0,0}
	start := time.Now()
	oldPwd, err := os.Getwd()
	if err != nil {
		return treeStats, errors.New(fmt.Sprintf("Unable to find current working directory prior to walking %s: %+f\n", localDirPath))
	} else {
		// return to original directory
		defer os.Chdir(oldPwd)
	}

	os.Chdir(localDirPath)
	err = filepath.Walk(".", func(path string, info os.FileInfo, err error) error {
		if err != nil {
			fmt.Printf("Unable to fetch file info while walking local directory tree %q: %v\n", path, err.Error())
			return err
		}
		// build the remote path to which we will be createing a directory, or uploading a file
		remoteChildpath := filepath.Join(remoteDirPath, path)
		localChildPath := filepath.Join(localDirPath, path)
		if info.IsDir() {
			// count visited regardless of success
			treeStats.Dirs = treeStats.Dirs + 1
			fmt.Printf("Creating remote directory: %q ...\n", remoteChildpath)

			mkdirRequest := &sftppb.SrvMkdirRequest{
				SystemConfig: remoteSystemConfig,
				RemotePath: remoteChildpath,
				Recursive: true,
			}
			resp, err := sftpRelay.Mkdir(context.Background(), mkdirRequest)
			if err != nil {
			 return errors.New(fmt.Sprintf("Error while calling GRPC Mkdir: %q %v + \n", remoteChildpath, err.Error()))
			} else if resp.GetError() != "" {
			 return errors.New(fmt.Sprintf("Error creating remote directory: %q %+v \n", remoteChildpath, resp.GetError()))
			}

			// capture deepest part of tree
			pathDepth := len(strings.Split(path, "/"))
			//fmt.Printf("Depth of %q is %d \n", path, pathDepth)
			if  pathDepth > treeStats.Depth {
				treeStats.Depth = pathDepth
			}

		} else {
			treeStats.Files = treeStats.Files + 1
			fmt.Printf("Uploading local file: %q [%d]...\n", localChildPath, info.Size())
			req := &sftppb.SrvPutRequest{
				SystemConfig: remoteSystemConfig,
				RemotePath: remoteChildpath,
				LocalPath: localChildPath,
				Force: force,
				Append: append,
			}

			resp, err := sftpRelay.Put(context.Background(), req)
			if err != nil {
				return errors.New(fmt.Sprintf("Error while calling GRPC Put: %q %v + \n", filepath.Join(localDirPath, path), filepath.Join(remoteDirPath, path), err.Error()))
			} else if resp.GetError() != "" {
				return errors.New(fmt.Sprintf("Error uploading local file to remote directory: %q %+v \n", filepath.Join(localDirPath, path), filepath.Join(remoteDirPath, path), resp.GetError()))
			}

			// count bytes transferred rather than actual byte amount
			treeStats.Bytes = treeStats.Bytes + resp.GetBytesTransferred()
		}

		return nil
	})
	// capture runtime
	treeStats.Runtime = time.Since(start).Milliseconds()

	if err != nil {
		return treeStats, errors.New(fmt.Sprintf("Error processing directory upload of %q: %+v \n", localDirPath, err.Error()))
	}

	return treeStats, nil
}

// uploads a file to the remote system via a grpc call
func putFile(sftpRelay sftppb.SftpRelayClient, remoteSystemConfig *sftppb.RemoteSystemConfig, localPath string, remotePath string, ) (TransferStats, error) {
	// create instrumentation
	treeStats := TransferStats{1,0,0,0,0}
	start := time.Now()

	info, err := os.Stat(localPath)
	if err != nil {
		return treeStats, errors.New(fmt.Sprintf("Unable to stat local filet: %q %v + \n", localPath, err.Error()))
	}

	fmt.Printf("Uploading local file: %q [%d]...\n", localPath, info.Size())
	req := &sftppb.SrvPutRequest{
		SystemConfig: remoteSystemConfig,
		RemotePath: remotePath,
		LocalPath: localPath,
		Force: force,
		Append: append,
	}

	resp, err := sftpRelay.Put(context.Background(), req)
	if err != nil {
		err = errors.New(fmt.Sprintf("Error while calling GRPC Put: %q %v + \n", localPath, err.Error()))
	} else if resp.GetError() != "" {
		err = errors.New(fmt.Sprintf("Error uploading local file to remote directory: %q %+v \n", localPath, resp.GetError()))
	}

	// update runtime stats for upload
	treeStats.Runtime = time.Since(start).Milliseconds()
	treeStats.Bytes = resp.BytesTransferred

	return treeStats, err
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
