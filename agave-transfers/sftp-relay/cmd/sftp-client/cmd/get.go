package cmd

import (
	"context"
	"fmt"
	"github.com/agaveplatform/science-apis/agave-transfers/sftp-relay/cmd/sftp-client/cmd/helper"
	agaveproto "github.com/agaveplatform/science-apis/agave-transfers/sftp-relay/pkg/sftpproto"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"os"
	"path/filepath"
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

		sftpConfig := helper.ParseSftpConfig(cmd.Flags())
		req := &agaveproto.SrvStatRequest{
			SystemConfig: sftpConfig,
			RemotePath:   remotePath,
		}
		if err != nil {
			fmt.Printf("Error while calling gRPC Stat: %s\n", err.Error())
			os.Exit(1)
		}

		// Fetch remote file info so we know whether to download a file or folder
		statResp, err := sftpRelay.Stat(context.Background(), req)
		if err != nil {
			fmt.Printf("No such path found: %s\n", localPath)
			os.Exit(1)
		} else if statResp == nil {
			fmt.Println("Empty response received from gRPC server")
			os.Exit(1)
		} else if statResp.Error != "" {
			fmt.Println(statResp.GetError())
			os.Exit(1)
		}

		var getStats TransferStats
		// now make the download request based on the file type
		if statResp.RemoteFileInfo.IsDirectory {
			getStats, err = getDir(sftpRelay, sftpConfig, remotePath, localPath, statResp.RemoteFileInfo)
			if err != nil {
				fmt.Printf(err.Error())
				os.Exit(1)
			}

			fmt.Printf("%s (%d bytes, %d files, %d ms)\n", localPath, getStats.Bytes, getStats.Files, getStats.Runtime)
		} else {
			getStats, err = getFile(sftpRelay, sftpConfig, remotePath, localPath, statResp.RemoteFileInfo)
			if err != nil {
				fmt.Printf(err.Error())
				os.Exit(1)
			}

			fmt.Printf("%s (%d bytes, %d ms)\n", localPath, getStats.Bytes, getStats.Runtime)
		}

		//log.Debugf("%v", res)
		//log.Infof("End Time %f", time.Since(startPushtime).Seconds())
		//log.Info("RPC Get Time: " + strconv.FormatFloat(secs, 'f', -1, 64))
	},
}

// Fetches a remote directory recursively
func getDir(sftpRelay agaveproto.SftpRelayClient, remoteSystemConfig *agaveproto.RemoteSystemConfig, remoteDirPath string, localDirPath string, remoteDirectoryInfo *agaveproto.RemoteFileInfo) (TransferStats, error) {
	downloadStats := TransferStats{0, 1, 0, 0, 0}
	start := time.Now()

	// ensure local directory is present for download. If not, create it.
	localDirInfo, err := os.Stat(localDirPath)
	if err != nil {
		// if path does not exist, we will create it
		if os.IsNotExist(err) {
			fmt.Printf("Creating local download directory %q ...\n", localDirPath)
			fMode := parseFileMode(remoteDirectoryInfo)
			err = os.Mkdir(localDirPath, fMode)
			if err != nil {
				return downloadStats, errors.New(fmt.Sprintf("Unable to create local directory %q: %v \n", localPath, err.Error()))
			}
		} else {
			// unknown error
			return downloadStats, errors.New(fmt.Sprintf("Unable to create local directory %q: %v \n", localPath, err.Error()))
		}
	} else if !localDirInfo.IsDir() {
		// make sure we can download
		return downloadStats, errors.New(fmt.Sprintf("Cannot download remote directory. Local path is a file %q: %v \n", localDirPath))
	}

	fmt.Printf("Fetching remote directory listing: %q ...\n", remoteDirPath)
	req := &agaveproto.SrvListRequest{
		SystemConfig: remoteSystemConfig,
		RemotePath:   remoteDirPath,
	}

	// fetch the remote directory listing
	res, err := sftpRelay.List(context.Background(), req)
	if err != nil {
		return downloadStats, errors.New(fmt.Sprintf("Error while calling gRPC List: %s\n", err.Error()))
	} else if res == nil {
		return downloadStats, errors.New(fmt.Sprintf("Empty response received from gRPC server"))
	} else {
		if res.Error != "" {
			return downloadStats, errors.New(res.GetError())
		} else {
			for _, remoteChildInfo := range res.Listing {
				localChildPath := filepath.Join(localDirPath, remoteChildInfo.Name)

				// don't process the current directory again or we'll get an infinite loop
				if remoteChildInfo.Name == "." || remoteChildInfo.Name == "" || remoteChildInfo.Path == remoteDirPath {
					continue
				}
				// process child based on file item type
				fmt.Printf("Processing remote child file item %q %q \n", remoteChildInfo.Name, remoteChildInfo.Path)
				if remoteChildInfo.IsDirectory {

					// make recursive call to get the directory contents
					childTransferStats, err := getDir(sftpRelay, remoteSystemConfig, remoteChildInfo.GetPath(), localChildPath, remoteChildInfo)
					// update the parent stats with the results from the subdirectory
					downloadStats.Add(childTransferStats)
					if err != nil {
						return downloadStats, errors.New(fmt.Sprintf("Error processing remote directory download %q: %+v \n", localDirPath, err.Error()))
					}
				} else {
					// make recursive call to get the directory contents
					childTransferStats, err := getFile(sftpRelay, remoteSystemConfig, remoteChildInfo.GetPath(), localChildPath, remoteChildInfo)
					downloadStats.Add(childTransferStats)
					if err != nil {
						return downloadStats, errors.New(fmt.Sprintf("Error processing remote file download %q: %+v \n", localDirPath, err.Error()))
					}
				}
			}
			// end processing this directory
			downloadStats.Runtime = time.Since(start).Milliseconds()
			return downloadStats, nil
		}
	}
}

// Fetches a single remote file
func getFile(sftpRelay agaveproto.SftpRelayClient, remoteSystemConfig *agaveproto.RemoteSystemConfig, remoteFilePath string, localFilePath string, remoteFileInfo *agaveproto.RemoteFileInfo) (TransferStats, error) {
	// create instrumentation
	getStats := TransferStats{1, 0, 0, 0, 0}
	start := time.Now()

	fmt.Printf("Downloading remote file: %q [%d] [%s]...\n", remotePath, remoteFileInfo.GetSize(), remoteFileInfo.GetMode())
	// update runtime stats for upload
	//getStats.Runtime = time.Since(start).Milliseconds()
	////getStats.Bytes = resp.BytesTransferred
	//getStats.Bytes = remoteFileInfo.GetSize()
	//return getStats, nil

	req := &agaveproto.SrvGetRequest{
		SystemConfig: remoteSystemConfig,
		RemotePath:   remoteFilePath,
		LocalPath:    localFilePath,
		Force:        force,
		Range:        byteRange,
	}

	resp, err := sftpRelay.Get(context.Background(), req)
	if err != nil {
		err = errors.New(fmt.Sprintf("Error while calling GRPC Get: %q %v + \n", remoteFilePath, err.Error()))
	} else if resp.GetError() != "" {
		err = errors.New(fmt.Sprintf("Error downloading remote file: %q %+v \n", remoteFilePath, resp.GetError()))
	}

	// update runtime stats for upload
	getStats.Runtime = time.Since(start).Milliseconds()
	getStats.Bytes = resp.BytesTransferred

	return getStats, err
}

// translates the file mode in the RemoteFileInfo.Mode into a uint32 sufficient
// to use as a os.FileMode
func parseFileMode(remoteDirectoryInfo *agaveproto.RemoteFileInfo) os.FileMode {
	pemLen := len(remoteDirectoryInfo.Mode)
	var stringPermissions string
	var fMode uint32 = 0
	if pemLen > 9 {
		stringPermissions = remoteDirectoryInfo.Mode[pemLen-9 : pemLen-1]

		for i, pemChar := range stringPermissions {
			if pemChar != '-' {
				fMode += 1 << (9 - i)
			}
		}
	} else {
		fMode = 0755
	}
	return os.FileMode(fMode)
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
