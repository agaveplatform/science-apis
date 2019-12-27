package sftprelay

import (
	"errors"
	"fmt"
	"github.com/agaveplatform/science-apis/agave-transfers/sftp-relay/pkg/connectionpool"
	"sort"

	agaveproto "github.com/agaveplatform/science-apis/agave-transfers/sftp-relay/pkg/sftpproto"
	"github.com/pkg/sftp"
	"github.com/sirupsen/logrus"
	"golang.org/x/crypto/ssh"
	"golang.org/x/net/context"
	"io"
	"os"
	"time"
)

var log = logrus.New()

const MAX_PACKET_SIZE = 32768

//The Pool and AgentSocket variables are created here and instantiated in the init() function.
var Pool *connectionpool.SSHPool

// The path to the unix socket on which the ssh-agent is listen
var AgentSocket string

type Server struct{}

// generates a new SSHConfig from the *agaveproto.RemoteSystemConfig included in every server request
func NewSSHConfig(systemConfig *agaveproto.RemoteSystemConfig) *connectionpool.SSHConfig {

	AgentSocket, ok := os.LookupEnv("SSH_AUTH_SOCK")
	if !ok {
		log.Fatalln("Could not connect to SSH_AUTH_SOCK. Is ssh-agent running?")
	}
	log.Debugf("SSH_AUTH_SOCK = %v", AgentSocket)

	return &connectionpool.SSHConfig{
		User:               systemConfig.Username,
		Host:               systemConfig.Host,
		Port:               int(systemConfig.Port),
		Auth:               []ssh.AuthMethod{ssh.Password(systemConfig.Password)},
		Timeout:            60 * time.Second,
		TCPKeepAlive:       true,
		TCPKeepAlivePeriod: 60 * time.Second,
		AgentSocket:        AgentSocket,
		ForwardAgent:       false,
		HostKeyCallback:    ssh.InsecureIgnoreHostKey(),
	}
}

func NewRemoteFileInfo(path string, fileInfo os.FileInfo) *agaveproto.RemoteFileInfo {
	return &agaveproto.RemoteFileInfo{
		Name:        fileInfo.Name(),
		Path:        path,
		Size:        fileInfo.Size(),
		Mode:        fileInfo.Mode().String(),
		LastUpdated: fileInfo.ModTime().Unix(),
		IsDirectory: fileInfo.IsDir(),
		IsLink:      !(fileInfo.Mode().IsRegular() || fileInfo.Mode().IsDir()),
	}
}

func init() {
	// Open a log file to log the server
	f, err := os.OpenFile("sftp-relay.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	wrt := io.MultiWriter(os.Stdout, f)
	log.SetOutput(wrt)

	// Now set up the ssh connection pool
	AgentSocket, ok := os.LookupEnv("SSH_AUTH_SOCK")
	if !ok {
		log.Fatalln("Could not connect to SSH_AUTH_SOCK. Is ssh-agent running?")
	}
	log.Debugf("SSH_AUTH_SOCK = %v", AgentSocket)

	// set the pool configuration.  The MaxConns sets the maximum connections for the pool.
	poolCfg := &connectionpool.PoolConfig{
		GCInterval: 5 * time.Second,
		MaxConns:   500,
	}
	// Create the new pool
	Pool = connectionpool.NewPool(poolCfg)
}

// Performs a basic authentication handshake to the remote system
func (*Server) AuthCheck(ctx context.Context, req *agaveproto.AuthenticationCheckRequest) (*agaveproto.EmptyResponse, error) {
	log.Tracef("Get Service function was invoked")

	sshCfg := NewSSHConfig(req.SystemConfig)

	log.Debugf(sshCfg.String())

	sftpClient, sessionCount, err := Pool.ClaimClient(sshCfg, sftp.MaxPacket(MAX_PACKET_SIZE))
	// verify we have a valid connection to use
	if err != nil {
		log.Errorf("Error obtaining connection to %s:%d. Nil client returned from pool", req.SystemConfig.Host, req.SystemConfig.Port)
		return &agaveproto.EmptyResponse{Error: fmt.Sprintf("nil client returned from pool: %v", err.Error())}, nil
	}
	defer Pool.ReleaseClient(sshCfg)
	defer sftpClient.Close()

	log.Infof("Number of active connections: %d", sessionCount)

	log.Infof(fmt.Sprintf("sftp://%s@%s:%d", req.SystemConfig.Username, req.SystemConfig.Host, req.SystemConfig.Port))

	// success returns an empty response
	return &agaveproto.EmptyResponse{}, nil
}

// Performs a mkdirs operation on a remote system.
func (*Server) Mkdirs(ctx context.Context, req *agaveproto.SrvMkdirsRequest) (*agaveproto.FileInfoResponse, error) {
	log.Printf("Mkdirs Service function was invoked with %v\n", req)

	sshCfg := NewSSHConfig(req.SystemConfig)

	log.Debugf(sshCfg.String())

	sftpClient, sessionCount, err := Pool.ClaimClient(sshCfg, sftp.MaxPacket(MAX_PACKET_SIZE))
	// verify we have a valid connection to use
	if err != nil {
		log.Errorf("Error obtaining connection to %s:%d. Nil client returned from pool", req.SystemConfig.Host, req.SystemConfig.Port)
		return &agaveproto.FileInfoResponse{Error: "nil client returned from pool"}, nil
	}
	defer Pool.ReleaseClient(sshCfg)
	defer sftpClient.Close()

	log.Infof("Number of active connections: %d", sessionCount)

	log.Infof(fmt.Sprintf("sftp://%s@%s:%d/%s", req.SystemConfig.Username, req.SystemConfig.Host, req.SystemConfig.Port, req.RemotePath))

	var response agaveproto.FileInfoResponse

	// Read From system
	response = mkdirs(sftpClient, req.RemotePath)

	return &response, nil
}

// Fetches file info for a remote path
func (*Server) Stat(ctx context.Context, req *agaveproto.SrvStatRequest) (*agaveproto.FileInfoResponse, error) {
	log.Printf("Stat Service function was invoked with %v\n", req)

	sshCfg := NewSSHConfig(req.SystemConfig)

	log.Debugf(sshCfg.String())

	sftpClient, sessionCount, err := Pool.ClaimClient(sshCfg, sftp.MaxPacket(MAX_PACKET_SIZE))
	// verify we have a valid connection to use
	if err != nil {
		log.Errorf("Error obtaining connection to %s:%d. Nil client returned from pool", req.SystemConfig.Host, req.SystemConfig.Port)
		return &agaveproto.FileInfoResponse{Error: "nil client returned from pool"}, nil
	}
	defer Pool.ReleaseClient(sshCfg)
	defer sftpClient.Close()

	log.Infof("Number of active connections: %d", sessionCount)

	log.Infof(fmt.Sprintf("sftp://%s@%s:%d/%s", req.SystemConfig.Username, req.SystemConfig.Host, req.SystemConfig.Port, req.RemotePath))

	var response agaveproto.FileInfoResponse

	// invoke the internal action
	response = stat(sftpClient, req.RemotePath)

	return &response, nil
}

// Removes the remote path. If the remote path is a directory, the entire tree is deleted.
func (*Server) Remove(ctx context.Context, req *agaveproto.SrvRemoveRequest) (*agaveproto.EmptyResponse, error) {
	log.Printf("Remove Service function was invoked with %v\n", req)

	sshCfg := NewSSHConfig(req.SystemConfig)

	log.Debugf(sshCfg.String())

	sftpClient, sessionCount, err := Pool.ClaimClient(sshCfg, sftp.MaxPacket(MAX_PACKET_SIZE))
	// verify we have a valid connection to use
	if err != nil {
		log.Errorf("Error obtaining connection to %s:%d. Nil client returned from pool", req.SystemConfig.Host, req.SystemConfig.Port)
		return &agaveproto.EmptyResponse{Error: "nil client returned from pool"}, nil
	}
	defer Pool.ReleaseClient(sshCfg)
	defer sftpClient.Close()

	log.Infof("Number of active connections: %d", sessionCount)

	log.Infof(fmt.Sprintf("sftp://%s@%s:%d/%s", req.SystemConfig.Username, req.SystemConfig.Host, req.SystemConfig.Port, req.RemotePath))

	var response agaveproto.EmptyResponse

	// invoke the internal action
	response = remove(sftpClient, req.RemotePath)

	return &response, nil
}

// Fetches a file from the remote system and stores it locally
func (*Server) Get(ctx context.Context, req *agaveproto.SrvGetRequest) (*agaveproto.TransferResponse, error) {
	log.Trace("Invoking get service")

	sshCfg := NewSSHConfig(req.SystemConfig)

	log.Debugf(sshCfg.String())

	sftpClient, sessionCount, err := Pool.ClaimClient(sshCfg, sftp.MaxPacket(MAX_PACKET_SIZE))
	// verify we have a valid connection to use
	if err != nil {
		log.Errorf("Error obtaining connection to %s:%d. Nil client returned from pool", req.SystemConfig.Host, req.SystemConfig.Port)
		return &agaveproto.TransferResponse{Error: "nil client returned from pool"}, nil
	}
	defer Pool.ReleaseClient(sshCfg)
	defer sftpClient.Close()

	log.Infof("Number of active connections: %d", sessionCount)

	log.Infof(fmt.Sprintf("sftp://%s@%s:%d/%s => file://localhost/%s", req.SystemConfig.Username, req.SystemConfig.Host, req.SystemConfig.Port, req.RemotePath, req.LocalPath))

	var response agaveproto.TransferResponse

	// invoke the internal action
	response = get(sftpClient, req.LocalPath, req.RemotePath, req.Force, req.Range)

	return &response, nil
}

// Transfers a file from the local system to the remote system
func (*Server) Put(ctx context.Context, req *agaveproto.SrvPutRequest) (*agaveproto.TransferResponse, error) {
	log.Trace("Invoking put service")

	sshCfg := NewSSHConfig(req.SystemConfig)

	log.Debugf(sshCfg.String())

	sftpClient, sessionCount, err := Pool.ClaimClient(sshCfg, sftp.MaxPacket(MAX_PACKET_SIZE))
	if err != nil {
		log.Errorf("Error with connection = %v", err)
		return &agaveproto.TransferResponse{Error: err.Error()}, nil
	}
	defer Pool.ReleaseClient(sshCfg)
	defer sftpClient.Close()

	log.Infof("Number of active connections: %d", sessionCount)

	log.Infof(fmt.Sprintf("put file://localhost/%s => sftp://%s@%s:%d/%s", req.LocalPath, req.SystemConfig.Username, req.SystemConfig.Host, req.SystemConfig.Port, req.RemotePath))

	var response agaveproto.TransferResponse

	// invoke the internal action
	response = put(sftpClient, req.LocalPath, req.RemotePath, req.Force, req.Append)

	return &response, nil
}

//*****************************************************************************************

/*
The last sections each implement an SFTP process.
*/

func get(sftpClient *sftp.Client, localFilePath string, remoteFilePath string, force bool, byteRanges string) agaveproto.TransferResponse {
	log.Trace("WriteTo (PUT) sFTP Service function was invoked ")

	// check if the file already exists. If is  does not and Force = false then error. Otherwise remove the
	//file first then do everything else
	var localFileExists bool
	localFileInfo, err := os.Stat(localFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			localFileExists = false
		} else {
			log.Errorf("Error attempting to stat local path %s: %v", remoteFilePath, err)
			return agaveproto.TransferResponse{Error: err.Error()}
		}
	} else {
		localFileExists = true
	}

	// Create the destination file
	if localFileExists && localFileInfo.IsDir() {
		log.Errorf("Error opening, %s. Destination is a directory.", remoteFilePath)
		return agaveproto.TransferResponse{Error: "destination path is a directory"}
	} else if (localFileExists && force) || !localFileExists {
		//if localFileExists {
		//	err := os.Remove(localFilePath)
		//	if err != nil {
		//		log.Errorf("Failed to remove existing local file, %s : %v", localFilePath, err)
		//	}
		//}

		remoteFile, err := sftpClient.Open(remoteFilePath)
		if err != nil {
			log.Errorf("Error opening handle to dest file. %s. %v", remoteFilePath, err)
			return agaveproto.TransferResponse{Error: err.Error()}
		}
		defer remoteFile.Close()

		// verify existence of local path
		remoteFileInfo, err := sftpClient.Stat(remoteFilePath)
		if err != nil {
			log.Errorf("Error verifying remote file %s: %v", remoteFilePath, err)
			return agaveproto.TransferResponse{Error: err.Error()}
		}

		// check that the remote path is not a directory
		if remoteFileInfo.IsDir() {
			err = errors.New(fmt.Sprintf("Error opening, %s. Remote path is a directory.", remoteFilePath))
			return agaveproto.TransferResponse{Error: "source path is a directory"}
		}

		// now that the dest file is open the next step is to open the source file
		log.Debugf("Opening local destination file %s", localFilePath)

		localFile, err := os.OpenFile(localFilePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0755)
		if err != nil {
			log.Errorf("Failed to open source file %s: %v", localFilePath, err)
			return agaveproto.TransferResponse{Error: err.Error()}
		}
		defer localFile.Close()

		var bytesWritten int64

		log.Debugf("Writing to remote file %s", remoteFilePath)
		// this will write the file BufferSize blocks until EOF is returned.
		bytesWritten, err = remoteFile.WriteTo(localFile)
		if err != nil {
			log.Errorf("Error after writing %d byes to file %s: %v", bytesWritten, localFilePath, err)
			return agaveproto.TransferResponse{Error: err.Error()}
		}
		log.Debugf("%d bytes copied to %s", bytesWritten, localFilePath)

		// flush the buffer to ensure everything was written
		err = localFile.Sync()
		if err != nil {
			log.Errorf("Error flushing local file, %s, when reading from %s: %v", localFilePath, remoteFilePath, err)
			return agaveproto.TransferResponse{Error: err.Error()}
		}
		// ensure permissions are retained
		log.Debugf("Setting permissions of dest file %s", localFilePath)
		err = os.Chmod(localFilePath, remoteFileInfo.Mode().Perm())
		if err != nil {
			log.Errorf("Error setting permissions of dest file %s after download: %v", localFilePath, err)
			return agaveproto.TransferResponse{Error: err.Error()}
		}

		// ensure timestamps are retained
		log.Debugf("Setting modification time of dest file %s", localFilePath)
		err = os.Chtimes(localFilePath, time.Now(), remoteFileInfo.ModTime())
		if err != nil {
			log.Errorf("Error setting modifcation time of dest file %s after download: %v", localFilePath, err)
			return agaveproto.TransferResponse{Error: err.Error()}
		}

		log.Debugf("Completed transfer to dest file %s", localFilePath)
		localFileInfo, _ = os.Stat(localFilePath)

		return agaveproto.TransferResponse{
			RemoteFileInfo:   NewRemoteFileInfo(localFilePath, localFileInfo),
			BytesTransferred: bytesWritten,
		}
	} else {
		return agaveproto.TransferResponse{Error: "file already exists"}
	}
}

func put(sftpClient *sftp.Client, localFilePath string, remoteFilePath string, force bool, append bool) agaveproto.TransferResponse {
	log.Trace("WriteTo (PUT) sFTP Service function was invoked ")

	// verify existence of local path
	localFileInfo, err := os.Stat(localFilePath)
	if err != nil {
		log.Errorf("Error verifying local file %s: %v", localFilePath, err)
		return agaveproto.TransferResponse{Error: err.Error()}
	}

	// check that the local path is not a directory
	if localFileInfo.IsDir() {
		err = errors.New(fmt.Sprintf("Error opening, %s. Local path is a directory.", localFilePath))
		return agaveproto.TransferResponse{Error: "source path is a directory"}
	}

	//###################################################################################################################
	// check if the file already exists. If is  does not and Force = false then error. Otherwise remove the
	//file first then do everything else
	var remoteFileExists bool
	remoteFileInfo, err := sftpClient.Stat(remoteFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			remoteFileExists = false
		} else {
			log.Errorf("Error attempting to stat remote path %s: %v", remoteFilePath, err)
			return agaveproto.TransferResponse{Error: err.Error()}
		}
	} else {
		remoteFileExists = true
	}

	// Create the destination file
	if remoteFileExists && remoteFileInfo.IsDir() {
		log.Errorf("Error opening, %s. Destination is a directory.", remoteFilePath)
		return agaveproto.TransferResponse{Error: "destination path is a directory"}
	} else if (remoteFileExists && force) || !remoteFileExists {
		//if remoteFileExists {
		//	err := sftpClient.Remove(remoteFilePath)
		//	if err != nil {
		//		log.Errorf("Failed to remove existing file, %s : %v", remoteFilePath, err)
		//	}
		//}

		// now that the dest file is created the next step is to open the source file
		log.Debugf("Opening local source file %s", localFilePath)

		localFile, err := os.Open(localFilePath)
		if err != nil {
			log.Errorf("Failed to open source file %s: %v", localFilePath, err)
			return agaveproto.TransferResponse{Error: err.Error()}
		}
		defer localFile.Close()

		remoteFile, err := sftpClient.OpenFile(remoteFilePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC)
		if err != nil {
			log.Errorf("Error opening handle to dest file. %s. %v", remoteFilePath, err)
			return agaveproto.TransferResponse{Error: err.Error()}
		}
		defer remoteFile.Close()

		var bytesWritten int64
		// if there were no error opening the two files then process the file.

		log.Debugf("Writing to remote file %s", remoteFilePath)
		// this will write the file BufferSize blocks until EOF is returned.
		bytesWritten, err = remoteFile.ReadFrom(localFile)
		if err != nil {
			log.Errorf("Error after writing %d byes to file %s: %v", bytesWritten, remoteFilePath, err)
			return agaveproto.TransferResponse{BytesTransferred: bytesWritten, Error: err.Error()}
		}
		log.Debugf("%d bytes copied to %s", bytesWritten, remoteFilePath)

		// flush the buffer to ensure everything was written
		err = localFile.Sync()
		if err != nil {
			log.Errorf("Error flushing source file, %s, when writing to %s: %v", localFilePath, remoteFilePath, err)
			return agaveproto.TransferResponse{Error: err.Error()}
		}
		// ensure permissions are retained
		log.Debugf("Setting permissions of dest file %s", remoteFilePath)
		err = sftpClient.Chmod(remoteFilePath, localFileInfo.Mode().Perm())
		if err != nil {
			log.Errorf("Error setting permissions of dest file %s after upload: %v", remoteFilePath, err)
			return agaveproto.TransferResponse{Error: err.Error()}
		}

		// ensure timestamps are retained
		log.Debugf("Setting modification time of dest file %s", remoteFilePath)
		err = sftpClient.Chtimes(remoteFilePath, time.Now(), localFileInfo.ModTime())
		if err != nil {
			log.Errorf("Error setting modifcation time of dest file %s after upload: %v", remoteFilePath, err)
			return agaveproto.TransferResponse{Error: err.Error()}
		}

		log.Debugf("Completed transfer to dest file %s", remoteFilePath)
		remoteFileInfo, _ = sftpClient.Stat(remoteFilePath)
		return agaveproto.TransferResponse{
			RemoteFileInfo:   NewRemoteFileInfo(remoteFilePath, remoteFileInfo),
			BytesTransferred: bytesWritten,
		}
	} else {
		return agaveproto.TransferResponse{Error: "file already exists"}
	}
}

func mkdirs(sftpClient *sftp.Client, remoteDirectoryPath string) agaveproto.FileInfoResponse {
	log.Trace("Mkdirs (Mkdirs) sFTP Service function was invoked ")

	// Create destination directory
	log.Debugf("Create destination directory %s", remoteDirectoryPath)
	err := sftpClient.MkdirAll(remoteDirectoryPath)
	if err != nil {
		log.Errorf("Error creating directory. %s. %v", remoteDirectoryPath, err)
		return agaveproto.FileInfoResponse{Error: err.Error()}
	}

	log.Debugf("Created destination directory: " + remoteDirectoryPath)

	// stat the remoete file to return the fileinfo data
	remoteFileInfo, err := sftpClient.Stat(remoteDirectoryPath)
	if err != nil {
		return agaveproto.FileInfoResponse{Error: err.Error()}
	}

	return agaveproto.FileInfoResponse{
		RemoteFileInfo: NewRemoteFileInfo(remoteDirectoryPath, remoteFileInfo),
	}
}

func stat(sftpClient *sftp.Client, remotePath string) agaveproto.FileInfoResponse {
	log.Trace("Stat (Stat) sFTP Service function was invoked ")

	// Stat the remote path
	remoteFileInfo, err := sftpClient.Stat(remotePath)
	if err != nil {
		log.Infof("Unable to stat file %v", remotePath)
		return agaveproto.FileInfoResponse{Error: err.Error()}
	}

	return agaveproto.FileInfoResponse{
		RemoteFileInfo: NewRemoteFileInfo(remotePath, remoteFileInfo),
	}
}

func remove(sftpClient *sftp.Client, remotePath string) agaveproto.EmptyResponse {
	log.Trace("Remove (Remove) sFTP Service function was invoked ")

	// Get the FileInfo for the remote path so we know how to proceed
	stat, err := sftpClient.Stat(remotePath)
	if err != nil {
		log.Errorf("Unable to stat %s: %v", remotePath, err)
		return agaveproto.EmptyResponse{Error: err.Error()}
	}

	// if the remote path is a directory, we need to walk the tree and delete the files before
	// deleting any of the directories.
	if stat.IsDir() {
		subdirMap := make(map[string]os.FileInfo)

		log.Printf("Deleting destination directory %s", remotePath)
		walker := sftpClient.Walk(remotePath)
		// the sftp.Walk object will traverse the tree breadth first, allowing us to
		// delete files as they come, and build a list of directories to delete
		// after we complete walking the full tree.
		for walker.Step() {
			if err := walker.Err(); err != nil && err.Error() != "EOF" {
				log.Warn("walker error", "err", err)
				break
			}

			stat := walker.Stat()

			if stat.IsDir() {
				subdirMap[walker.Path()] = stat
			} else {
				log.Debugf("Deleting nested file at %s", walker.Path())
				err = sftpClient.Remove(walker.Path())
				if err != nil {
					log.Errorf("Error deleting nested file %s. %v", remotePath, err)
					break
				}
			}
		}

		// if we finished walking the tree cleanly, then all files are deleted.
		// Now that there are no more files, we can attempt to remove the directories.
		if walker.Err() == nil && err == nil {
			// get the list of directories we need to delete from our map above
			paths := make([]string, 0, len(subdirMap))
			for k := range subdirMap {
				paths = append(paths, k)
			}

			// sort them so we can honor the proper tree
			sort.Strings(paths)

			// reverse the list so we can safely delete depth first
			for i := len(paths)/2 - 1; i >= 0; i-- {
				opp := len(paths) - 1 - i
				paths[i], paths[opp] = paths[opp], paths[i]
			}

			// delete the directories in order
			for _, path := range paths {
				log.Debugf("Deleting nested directory at %s", path)
				err = sftpClient.RemoveDirectory(path)
				if err != nil {
					log.Errorf("Error deleting nested directory %s. %v", remotePath, err)
					return agaveproto.EmptyResponse{Error: err.Error()}
				}
			}
		} else {
			log.Errorf("Error deleting one or more nested files below %s. %v", remotePath, err)
			return agaveproto.EmptyResponse{Error: err.Error()}
		}

		log.Debugf("Deleted remote directory: %s", remotePath)
	} else {
		// the remote path was a file, so we just delete it directly.
		log.Printf("Deleting destination file %s", remotePath)
		err = sftpClient.Remove(remotePath)
		if err != nil {
			log.Errorf("Error deleting remote file %s: %v", remotePath, err)
			return agaveproto.EmptyResponse{Error: err.Error()}
		}
		log.Debugf("Deleted remote file: %s", remotePath)
	}

	// everything went well so we can return a successful response
	return agaveproto.EmptyResponse{}
}

/*
This will return the size of the file in bytes.
It is not presently used.
*/
func GetFileSize(filepath string) (int64, error) {
	fi, err := os.Stat(filepath)
	if err != nil {
		return 0, err
	}
	// get the size
	return fi.Size(), nil
}
