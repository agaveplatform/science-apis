package sftprelay

import (
	"context"
	"fmt"
	agaveproto "github.com/agaveplatform/science-apis/agave-transfers/sftp-relay/pkg/sftpproto"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const bufSize = 1024 * 1024
const RELAY_SHARED_TEST_DIR = "/scratch"
const SFTP_SHARED_TEST_DIR = "/scratch"

// net.Listener that creates local, buffered net.Conns over its Accept and Dial method
var lis *bufconn.Listener

// folder used for data setup and validation between sftp container, server, and test code
var LocalSharedTestDir string

// relative path to the current local test scratch dir
//var SharedRandomTestDirPath string
// relative path to the scratch directory for each test function. This is relaolved against the *_SHARED_TEST_DIR
// on each remote system to determine the actual path used in tests.
var CurrentBaseTestDirPath string

var consolelog = &logrus.Logger{
	Out:       os.Stdout,
	Formatter: new(logrus.TextFormatter),
	Hooks:     make(logrus.LevelHooks),
	Level:     logrus.DebugLevel,
}

func init() {

	LocalSharedTestDir = resolveLocalSharedTestDir()

	// set the server log to debug
	log.SetLevel(logrus.DebugLevel)

	//set up for the servers
	lis = bufconn.Listen(bufSize)
	s := grpc.NewServer()
	agaveproto.RegisterSftpRelayServer(s, &Server{})
	go func() {
		if err := s.Serve(lis); err != nil {
			consolelog.Fatalf("Server exited with error: %v", err)
		}
	}()
}

// returns the path of the local shared directory for tests involving IO between
// the test process, sftp container, and relay server
func resolveLocalSharedTestDir() string {
	// PROJECT_ROOT is defined in the makefile used to run these tests
	projectRoot := os.Getenv("PROJECT_ROOT")
	// if we cannot find it, we resolve to the parent of the pkg directory
	if projectRoot == "" {
		projectRoot = "../../"
	}
	relativeSharedTestDir := filepath.Join(projectRoot, "scratch")
	absoluteSharedTestDir, err := filepath.Abs(relativeSharedTestDir)
	if err != nil {
		consolelog.Fatalf("Unable to resolve absolute path of local shared test directory, %s", relativeSharedTestDir)
	}
	return absoluteSharedTestDir
}

// resolves the test path against the base path. If the testPath
// begins with a slash, it is assumed to be an absolute path and
// is returned.
func _resolveTestPath(testPath string, basePath string) string {
	resolvedPath := filepath.Join(basePath, testPath)
	if len(testPath) > 0 {
		if strings.Index(testPath, "/") == 0 {
			resolvedPath = testPath
		}
	}
	resolvedPath = strings.ReplaceAll(resolvedPath, "//", "/")
	resolvedPath = strings.TrimRight(resolvedPath, "/")

	return resolvedPath
}

// resolves the local test file path to the path on the remote test system
func _resolveTestPathToRemotePath(localTestPath string) string {
	return strings.Replace(localTestPath, LocalSharedTestDir, SFTP_SHARED_TEST_DIR, 1)
}

func beforeTest(t *testing.T) {
	// create a new unique test directory for the test
	CurrentBaseTestDirPath = filepath.Join("sftprelay_test", t.Name(), uuid.New().String())

	// create the directory on the local file system within the shared folder
	sharedRandomTestDirPath := _resolveTestPath(CurrentBaseTestDirPath, LocalSharedTestDir)
	consolelog.Debugf("Creating test directory for test %s, %s", t.Name(), CurrentBaseTestDirPath)

	err := os.MkdirAll(sharedRandomTestDirPath, os.ModePerm)
	if err != nil {
		// fail if the directory does not exist on the local file system
		t.Fatalf("Failed to create test directory for test %s, %s", t.Name(), sharedRandomTestDirPath)
	}
}

func _getConnection(t *testing.T) *grpc.ClientConn {
	ctx := context.Background()
	conn, err := grpc.DialContext(ctx, "bufnet", grpc.WithDialer(bufDialer), grpc.WithInsecure())
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}
	return conn
}

// cleans up the test directory and its contents, if present on the remote host
func afterTest(t *testing.T) {
	// create a new unique test directory for the test
	if CurrentBaseTestDirPath != "" {
		sharedRandomTestDirPath := filepath.Dir(_resolveTestPath(CurrentBaseTestDirPath, LocalSharedTestDir))

		consolelog.Debugf("Removing test directory for test %s, %s", t.Name(), sharedRandomTestDirPath)

		err := os.RemoveAll(sharedRandomTestDirPath)
		if err != nil {
			consolelog.Errorf("Failed to remove directory for test %s, %s", t.Name(), sharedRandomTestDirPath)
		}
	}
}

// creates a temp directory in the CurrentBaseTestDirPath
func _createTempDirectory(prefix string) (string, error) {
	return _createTempDirectoryInDirectory(CurrentBaseTestDirPath, prefix)
}

// creates a temp directory within the parentPath. This will be resolved relative to the
// LocalSharedTestDir, so keep all paths relative to the CurrentBaseTestDirPath for simplicity
func _createTempDirectoryInDirectory(parentPath string, prefix string) (string, error) {
	// create a new unique directory name in our current test directory
	tempDir := filepath.Join(parentPath, fmt.Sprintf("%s%s", prefix, uuid.New().String()))

	// create the directory
	err := os.MkdirAll(_resolveTestPath(tempDir, LocalSharedTestDir), os.ModePerm)

	return tempDir, err
}

// creates a temp file in the currentTestDir
func _createTempFile(prefix string, suffix string) (string, error) {
	return _createTempFileInDirectory(CurrentBaseTestDirPath, prefix, suffix)
}

// creates a temp file at the given directory path. returns error when the directory does not exist
func _createTempFileInDirectory(parentPath string, prefix string, suffix string) (string, error) {

	tempFilePath := filepath.Join(parentPath, fmt.Sprintf("%s%s%s", prefix, uuid.New().String(), suffix))

	consolelog.Debugf("Creating temp file %s", tempFilePath)
	resolvedTempFilePath := _resolveTestPath(tempFilePath, LocalSharedTestDir)
	tempFile, err := os.Create(resolvedTempFilePath)
	if err != nil {
		return "", err
	}
	defer tempFile.Close()
	os.Chmod(resolvedTempFilePath, os.ModePerm)

	bigBuff := make([]byte, 32768)
	_, err = tempFile.Write(bigBuff)
	if err != nil {
		return "", err
	}

	return tempFilePath, err
}

// generates a generic Sftp object used to communicate with the relay server
func _createRemoteSystemConfig() *agaveproto.RemoteSystemConfig {
	return &agaveproto.RemoteSystemConfig{
		Username: "testuser",
		Password: "testuser",
		Host:     "sftp",
		Port:     10022,
	}
}

func bufDialer(string, time.Duration) (net.Conn, error) {
	return lis.Dial()
}

//helper  net.Conn
func TestAuthenticate(t *testing.T) {
	beforeTest(t)

	conn := _getConnection(t)
	defer conn.Close()

	client := agaveproto.NewSftpRelayClient(conn)

	req := &agaveproto.AuthenticationCheckRequest{
		SystemConfig: _createRemoteSystemConfig(),
	}

	grpcResponse, err := client.AuthCheck(context.Background(), req)
	if err != nil {
		assert.Nilf(t, err, "Error while calling RPC Auth: %v", err)
	} else {
		assert.Equal(t, "", grpcResponse.Error, "AuthCheck with valid config should return empty error")
	}

	afterTest(t)
}

func TestStatReturnsFileInfo(t *testing.T) {
	beforeTest(t)

	conn := _getConnection(t)
	defer conn.Close()

	client := agaveproto.NewSftpRelayClient(conn)

	tmpTestFilePath, err := _createTempFile("", ".txt")
	if err != nil {
		assert.FailNowf(t, err.Error(), "Unable to create temp test file: %s", err.Error())
	}

	tmpTestFileInfo, err := os.Stat(_resolveTestPath(tmpTestFilePath, LocalSharedTestDir))
	if err != nil {
		assert.FailNowf(t, err.Error(), "Unable to open temp test file: %s", err.Error())
	}

	remoteTestFilePath := _resolveTestPath(tmpTestFilePath, SFTP_SHARED_TEST_DIR)

	req := &agaveproto.SrvStatRequest{
		SystemConfig: _createRemoteSystemConfig(),
		RemotePath:   remoteTestFilePath,
	}

	grpcResponse, err := client.Stat(context.Background(), req)
	if err != nil {
		assert.Nilf(t, err, "Error while calling RPC Stat: %v", err)
	} else {
		assert.Equal(t, "", grpcResponse.Error, "Stat on existing file should return empty error")
		assert.Equal(t, tmpTestFileInfo.Size(), grpcResponse.RemoteFileInfo.Size, "Returned file size should match the test file size")
		assert.Equal(t, tmpTestFileInfo.Name(), grpcResponse.RemoteFileInfo.Name, "Returned file name should match the name of the test file")
		assert.Equal(t, remoteTestFilePath, grpcResponse.RemoteFileInfo.Path, "Returned file path should match the path of the test file")
		assert.Equal(t, tmpTestFileInfo.IsDir(), grpcResponse.RemoteFileInfo.IsDirectory, "Returned directory flag should match the flag of the test file")
		assert.Equal(t, tmpTestFileInfo.Mode().String(), grpcResponse.RemoteFileInfo.Mode, "Returned mode should match the mode of the test file")
		assert.Equal(t, tmpTestFileInfo.ModTime().Unix(), grpcResponse.RemoteFileInfo.LastUpdated, "Returned last updated timestamp should match the test file")
	}

	afterTest(t)
}

func TestStatReturnsDirectoryInfo(t *testing.T) {
	beforeTest(t)

	conn := _getConnection(t)
	defer conn.Close()

	client := agaveproto.NewSftpRelayClient(conn)

	tmpTestDirPath, err := _createTempDirectory("")
	if err != nil {
		assert.FailNowf(t, err.Error(), "Unable to create temp test directory: %s", err.Error())
	}

	tmpTestDirInfo, err := os.Stat(_resolveTestPath(tmpTestDirPath, LocalSharedTestDir))
	if err != nil {
		assert.FailNowf(t, err.Error(), "Unable to open temp test directory: %s", err.Error())
	}

	remoteTestDirPath := _resolveTestPath(tmpTestDirPath, SFTP_SHARED_TEST_DIR)

	req := &agaveproto.SrvStatRequest{
		SystemConfig: _createRemoteSystemConfig(),
		RemotePath:   remoteTestDirPath,
	}

	grpcResponse, err := client.Stat(context.Background(), req)
	if err != nil {
		assert.Nilf(t, err, "Error while invoking remote service: %v", err)
	} else {
		assert.Equal(t, "", grpcResponse.Error, "Stat on existing file should return empty Error")
		assert.Equal(t, tmpTestDirInfo.Size(), grpcResponse.RemoteFileInfo.Size, "Returned file size should match the test file size")
		assert.Equal(t, tmpTestDirInfo.Name(), grpcResponse.RemoteFileInfo.Name, "Returned file name should match the name of the test file")
		assert.Equal(t, remoteTestDirPath, grpcResponse.RemoteFileInfo.Path, "Returned file path should match the path of the test file")
		assert.Equal(t, tmpTestDirInfo.IsDir(), grpcResponse.RemoteFileInfo.IsDirectory, "Returned directory flag should match the flag of the test file")
		assert.Equal(t, tmpTestDirInfo.Mode().String(), grpcResponse.RemoteFileInfo.Mode, "Returned mode should match the mode of the test file")
		assert.Equal(t, tmpTestDirInfo.ModTime().Unix(), grpcResponse.RemoteFileInfo.LastUpdated, "Returned last updated timestamp should match the test file")
	}

	afterTest(t)
}

func TestStatReturnsErrIfPathDoesNotExist(t *testing.T) {
	beforeTest(t)

	conn := _getConnection(t)
	defer conn.Close()

	client := agaveproto.NewSftpRelayClient(conn)

	sftpBaseTestDir := _resolveTestPath(CurrentBaseTestDirPath, SFTP_SHARED_TEST_DIR)
	remoteMissingPath := fmt.Sprintf("%s/%s", sftpBaseTestDir, uuid.New().String())

	req := &agaveproto.SrvStatRequest{
		SystemConfig: _createRemoteSystemConfig(),
		RemotePath:   remoteMissingPath,
	}

	grpcResponse, err := client.Stat(context.Background(), req)
	if err != nil {
		assert.Nilf(t, err, "Error while invoking remote service: %v", err)
	} else {
		assert.Contains(t, grpcResponse.Error, "does not exist", err)
		assert.Nil(t, grpcResponse.RemoteFileInfo, "Returned file info should be nil when an error occurs")
	}

	afterTest(t)
}

func TestMkdirs(t *testing.T) {

	beforeTest(t)

	conn := _getConnection(t)
	defer conn.Close()

	client := agaveproto.NewSftpRelayClient(conn)

	// create a random directory name in our test dir
	testDirectoryPath := fmt.Sprintf("%s/%s", CurrentBaseTestDirPath, uuid.New().String())

	// resolve it to the absolute path within our shared test directory on the remote system
	remoteTestDirectoryPath := _resolveTestPath(testDirectoryPath, SFTP_SHARED_TEST_DIR)

	req := &agaveproto.SrvMkdirsRequest{
		SystemConfig: _createRemoteSystemConfig(),
		RemotePath:   remoteTestDirectoryPath,
	}
	grpcResponse, err := client.Mkdirs(context.Background(), req)
	if err != nil {
		assert.Nilf(t, err, "Error while invoking remote service: %v", err)
	} else {
		// get the test directory stat in the local shared directory
		tmpTestDirInfo, err := os.Stat(_resolveTestPath(testDirectoryPath, LocalSharedTestDir))
		if os.IsNotExist(err) {
			assert.FailNowf(t, err.Error(), "Test directory was not created on remote host: %s", err.Error())
		}
		assert.Equal(t, "", grpcResponse.Error, "Mkdirs on valid remote should return empty Error")
		assert.True(t, tmpTestDirInfo.IsDir(), "Remote path should be a directory. File found instead.")
		assert.Equal(t, remoteTestDirectoryPath, grpcResponse.RemoteFileInfo.Path, "Returned file name should match the name of the new directory")
		assert.Equal(t, tmpTestDirInfo.Name(), grpcResponse.RemoteFileInfo.Name, "Returned file info name should match the name of the new directory")
		assert.Equal(t, tmpTestDirInfo.Size(), grpcResponse.RemoteFileInfo.Size, "Returned file info size should match the size of the new directory")
		assert.Equal(t, tmpTestDirInfo.ModTime().Unix(), grpcResponse.RemoteFileInfo.LastUpdated, "Returned file info last modified date should match the last modified date of the new directory")
		assert.Equal(t, tmpTestDirInfo.Mode().String(), grpcResponse.RemoteFileInfo.Mode, "Returned file info mode should match the mode of the new directory")
		assert.True(t, grpcResponse.RemoteFileInfo.IsDirectory, "Returned file info should report as a directory")
		assert.False(t, grpcResponse.RemoteFileInfo.IsLink, "Returned file info should not report as a link")
	}

	afterTest(t)
}

func TestMkdirsReturnsErrorWhenPathIsFile(t *testing.T) {

	beforeTest(t)

	conn := _getConnection(t)
	defer conn.Close()

	client := agaveproto.NewSftpRelayClient(conn)

	// create a random directory name in our test dir
	tmpTestFilePath, err := _createTempFile("", ".txt")
	if err != nil {
		assert.FailNowf(t, err.Error(), "Unable to create temp test file: %s", err.Error())
	}

	remoteTestFilePath := _resolveTestPath(tmpTestFilePath, SFTP_SHARED_TEST_DIR)

	req := &agaveproto.SrvMkdirsRequest{
		SystemConfig: _createRemoteSystemConfig(),
		RemotePath:   remoteTestFilePath,
	}
	grpcResponse, err := client.Mkdirs(context.Background(), req)
	if err != nil {
		assert.Nilf(t, err, "Error while invoking remote service: %v", err)
	} else {
		// get the test directory stat in the local shared directory
		tmpTestFileInfo, err := os.Stat(_resolveTestPath(tmpTestFilePath, LocalSharedTestDir))
		if err != nil {
			assert.FailNowf(t, err.Error(), "Unable to open temp test file after calling mkdir: %s", err.Error())
		}
		assert.False(t, tmpTestFileInfo.IsDir(), "Remote path should still be a file after calling mkdir on it.")
		assert.Contains(t, grpcResponse.Error, "not a directory", "Mkdirs on existing file should return response saying file already exists")
		assert.Nil(t, grpcResponse.RemoteFileInfo, "Returned file info should be nil when an error occurs")
	}

	afterTest(t)
}

func TestMkdirsReturnsNoErrorWhenDirectoryExists(t *testing.T) {

	beforeTest(t)

	conn := _getConnection(t)
	defer conn.Close()

	client := agaveproto.NewSftpRelayClient(conn)

	// create a random directory name in our test dir
	tmpTestDirPath, err := _createTempDirectory("")
	if err != nil {
		assert.FailNowf(t, err.Error(), "Unable to create temp test file: %s", err.Error())
	}

	resolvedRemoteTestFilePath := _resolveTestPath(tmpTestDirPath, SFTP_SHARED_TEST_DIR)

	req := &agaveproto.SrvMkdirsRequest{
		SystemConfig: _createRemoteSystemConfig(),
		RemotePath:   resolvedRemoteTestFilePath,
	}
	grpcResponse, err := client.Mkdirs(context.Background(), req)
	if err != nil {
		assert.Nilf(t, err, "Error while invoking remote service: %v", err)
	} else {
		// get the test directory stat in the local shared directory
		tmpTestDirInfo, err := os.Stat(_resolveTestPath(tmpTestDirPath, LocalSharedTestDir))
		if err != nil {
			assert.FailNowf(t, err.Error(), "Unable to open temp test file after calling mkdir: %s", err.Error())
		}
		assert.True(t, tmpTestDirInfo.IsDir(), "Remote path should still be a file after calling mkdir on it.")
		assert.Equal(t, "", grpcResponse.Error, "Mkdirs on valid remote should return empty Error")
		assert.Equal(t, resolvedRemoteTestFilePath, grpcResponse.RemoteFileInfo.Path, "Returned file info path should match the path of the Put file")
		assert.Equal(t, tmpTestDirInfo.Name(), grpcResponse.RemoteFileInfo.Name, "Returned file info name should match the name of the Put file")
		assert.Equal(t, tmpTestDirInfo.Size(), grpcResponse.RemoteFileInfo.Size, "Returned file info size should match the size of the Put file")
		assert.Equal(t, tmpTestDirInfo.ModTime().Unix(), grpcResponse.RemoteFileInfo.LastUpdated, "Returned file info last modified date should match the last modified date of the Put file")
		assert.Equal(t, tmpTestDirInfo.Mode().String(), grpcResponse.RemoteFileInfo.Mode, "Returned file info mode should match the mode of the Put file")
		assert.True(t, grpcResponse.RemoteFileInfo.IsDirectory, "Returned file info should report as a directory")
		assert.False(t, grpcResponse.RemoteFileInfo.IsLink, "Returned file info should not report as a link")

	}

	afterTest(t)
}

func TestRemoveFile(t *testing.T) {
	beforeTest(t)

	conn := _getConnection(t)
	defer conn.Close()

	client := agaveproto.NewSftpRelayClient(conn)

	// create a random directory name in our test dir
	tmpTestFilePath, err := _createTempFile("", ".bin")
	if err != nil {
		assert.FailNowf(t, err.Error(), "Unable to create temp test file: %s", err.Error())
	}

	remoteTestFilePath := _resolveTestPath(tmpTestFilePath, SFTP_SHARED_TEST_DIR)

	req := &agaveproto.SrvRemoveRequest{
		SystemConfig: _createRemoteSystemConfig(),
		RemotePath:   remoteTestFilePath,
	}
	grpcResponse, err := client.Remove(context.Background(), req)
	if err != nil {
		assert.Nilf(t, err, "Error while invoking remote service: %v", err)
	} else {
		// get the test directory stat in the local shared directory
		_, err := os.Stat(_resolveTestPath(tmpTestFilePath, LocalSharedTestDir))
		assert.True(t, os.IsNotExist(err), "File should not be present after calling Remove")
		assert.Equal(t, "", grpcResponse.Error, "Error message in response should be empty after successfully request")
	}

	afterTest(t)
}

func TestRemoveDirectory(t *testing.T) {
	beforeTest(t)

	conn := _getConnection(t)
	defer conn.Close()

	client := agaveproto.NewSftpRelayClient(conn)

	// create a random directory name in our test dir
	tmpTestDirPath, err := _createTempDirectory("")
	if err != nil {
		assert.FailNowf(t, err.Error(), "Unable to create temp test file: %s", err.Error())
	}

	remoteTestDirPath := _resolveTestPath(tmpTestDirPath, SFTP_SHARED_TEST_DIR)

	req := &agaveproto.SrvRemoveRequest{
		SystemConfig: _createRemoteSystemConfig(),
		RemotePath:   remoteTestDirPath,
	}
	grpcResponse, err := client.Remove(context.Background(), req)
	if err != nil {
		assert.Nilf(t, err, "Error while invoking remote service: %v", err)
	} else {
		// get the test directory stat in the local shared directory
		_, err := os.Stat(_resolveTestPath(tmpTestDirPath, LocalSharedTestDir))
		assert.True(t, os.IsNotExist(err), "Directory should not be present after calling Remove")
		assert.Equal(t, "", grpcResponse.Error, "Error message in response should be empty after successfully request")
	}

	afterTest(t)
}

func TestRemoveDirectoryAndContents(t *testing.T) {
	beforeTest(t)

	conn := _getConnection(t)
	defer conn.Close()

	client := agaveproto.NewSftpRelayClient(conn)

	// create a random directory name in our test dir
	tmpTestDirPath, err := _createTempDirectory("")
	if err != nil {
		assert.FailNowf(t, err.Error(), "Unable to create temp test file: %s", err.Error())
	}
	// add a file to the temp dir
	_, err = _createTempFileInDirectory(tmpTestDirPath, "", ".bin")

	// creating a nested directory structure in the temp dir
	tmpNestedTestDirPath, err := _createTempDirectoryInDirectory(tmpTestDirPath, "")
	_, err = _createTempFileInDirectory(tmpNestedTestDirPath, "", ".bin")
	_, err = _createTempFileInDirectory(tmpNestedTestDirPath, "", ".bin")

	remoteTestDirPath := _resolveTestPath(tmpTestDirPath, SFTP_SHARED_TEST_DIR)

	req := &agaveproto.SrvRemoveRequest{
		SystemConfig: _createRemoteSystemConfig(),
		RemotePath:   remoteTestDirPath,
	}
	grpcResponse, err := client.Remove(context.Background(), req)
	if err != nil {
		assert.Nilf(t, err, "Error while invoking remote service: %v", err)
	} else {
		// get the test directory stat in the local shared directory
		_, err := os.Stat(_resolveTestPath(tmpTestDirPath, LocalSharedTestDir))
		assert.True(t, os.IsNotExist(err), "Directory should not be present after calling Remove")
		assert.Equal(t, "", grpcResponse.Error, "Error message in response should be empty after successfully request")
	}

	afterTest(t)
}

func TestPut(t *testing.T) {
	beforeTest(t)

	conn := _getConnection(t)
	defer conn.Close()

	client := agaveproto.NewSftpRelayClient(conn)

	// create a random directory name in our test dir
	tmpTestFilePath, err := _createTempFile("", ".bin")
	if err != nil {
		assert.FailNowf(t, err.Error(), "Unable to create temp test file: %s", err.Error())
	}

	resolvedLocalTestFilePath := _resolveTestPath(tmpTestFilePath, LocalSharedTestDir)
	tmpTestFileInfo, err := os.Stat(resolvedLocalTestFilePath)
	if err != nil {
		assert.FailNowf(t, err.Error(), "Unable to open temp test file: %s", err.Error())
	}

	resolvedRemoteTestFilePath := _resolveTestPath(tmpTestFilePath+".copy", SFTP_SHARED_TEST_DIR)

	req := &agaveproto.SrvPutRequest{
		SystemConfig: _createRemoteSystemConfig(),
		LocalPath:    resolvedLocalTestFilePath,
		RemotePath:   resolvedRemoteTestFilePath,
		Force:        true,
		Append:       false,
	}
	grpcResponse, err := client.Put(context.Background(), req)
	if err != nil {
		assert.Nilf(t, err, "Error while invoking remote service: %v", err)
	} else {
		// get the test directory stat in the local shared directory
		targetTestFilePath := _resolveTestPath(tmpTestFilePath+".copy", LocalSharedTestDir)
		targetTestFileInfo, err := os.Stat(targetTestFilePath)
		if err != nil {
			assert.FailNowf(t, err.Error(), "Unable to open copied file, %s, on remote host file: %s", targetTestFilePath, err.Error())
		}

		assert.Equal(t, "", grpcResponse.Error, "Error message in response should be empty after successfully request")
		assert.Equal(t, tmpTestFileInfo.Size(), targetTestFileInfo.Size(), "Put should preserve the original file size ")
		assert.Equal(t, tmpTestFileInfo.IsDir(), targetTestFileInfo.IsDir(), "Putting a file should result in a file")
		assert.Equal(t, tmpTestFileInfo.Mode().String(), targetTestFileInfo.Mode().String(), "Put should preserve file permissions")
		assert.Equal(t, tmpTestFileInfo.ModTime().Unix(), targetTestFileInfo.ModTime().Unix(), "Put should preserve last updated timestamp")
		assert.Equal(t, resolvedRemoteTestFilePath, grpcResponse.RemoteFileInfo.Path, "Returned file info path should match the path of the Put file")
		assert.Equal(t, targetTestFileInfo.Name(), grpcResponse.RemoteFileInfo.Name, "Returned file info name should match the name of the Put file")
		assert.Equal(t, targetTestFileInfo.Size(), grpcResponse.RemoteFileInfo.Size, "Returned file info size should match the size of the Put file")
		assert.Equal(t, targetTestFileInfo.ModTime().Unix(), grpcResponse.RemoteFileInfo.LastUpdated, "Returned file info last modified date should match the last modified date of the Put file")
		assert.Equal(t, targetTestFileInfo.Mode().String(), grpcResponse.RemoteFileInfo.Mode, "Returned file info mode should match the mode of the Put file")
		assert.False(t, grpcResponse.RemoteFileInfo.IsDirectory, "Returned file info name should not report as a directory")
		assert.False(t, grpcResponse.RemoteFileInfo.IsLink, "Returned file info should not report as a link")
	}

	afterTest(t)
}

func TestPutFileToDirectoryReturnsError(t *testing.T) {
	beforeTest(t)

	conn := _getConnection(t)
	defer conn.Close()

	client := agaveproto.NewSftpRelayClient(conn)

	// create a temp file to put
	tmpTestFilePath, err := _createTempFile("", ".bin")
	if err != nil {
		assert.FailNowf(t, err.Error(), "Unable to create temp test file: %s", err.Error())
	}
	resolvedLocalTestFilePath := _resolveTestPath(tmpTestFilePath, LocalSharedTestDir)

	// create a directory as the target of our put
	tmpTestDirPath, err := _createTempDirectory("")
	if err != nil {
		assert.FailNowf(t, err.Error(), "Unable to create temp test file: %s", err.Error())
	}
	resolvedRemoteTestDirPath := _resolveTestPath(tmpTestDirPath, SFTP_SHARED_TEST_DIR)

	req := &agaveproto.SrvPutRequest{
		SystemConfig: _createRemoteSystemConfig(),
		LocalPath:    resolvedLocalTestFilePath,
		RemotePath:   resolvedRemoteTestDirPath,
		Force:        true,
		Append:       false,
	}
	grpcResponse, err := client.Put(context.Background(), req)
	if err != nil {
		assert.Nilf(t, err, "Error while invoking remote service: %v", err)
	} else {
		// get the test directory stat in the local shared directory
		resolvedLocalTestDirPath := _resolveTestPath(tmpTestDirPath, LocalSharedTestDir)
		targetTestDirInfo, err := os.Stat(resolvedLocalTestDirPath)
		if err != nil {
			assert.FailNowf(t, err.Error(), "Unable to open temp directory, %s, on remote host after put: %s", resolvedLocalTestDirPath, err.Error())
		}
		assert.True(t, targetTestDirInfo.IsDir(), "Putting a file to a directory should fail and preserve the target directory")
		assert.Contains(t, grpcResponse.Error, "destination path is a directory", "Error message in response should indicate the destination path is a directory")
		assert.Nil(t, grpcResponse.RemoteFileInfo, "Returned file info should be nil on error")
	}

	afterTest(t)
}

func TestPutMissingLocalFileReturnsError(t *testing.T) {
	beforeTest(t)

	conn := _getConnection(t)
	defer conn.Close()

	client := agaveproto.NewSftpRelayClient(conn)

	// construct a missing local path
	tmpMissingTestFilePath := filepath.Join(CurrentBaseTestDirPath, uuid.New().String(), uuid.New().String())
	resolvedTmpMissingTestFilePath := _resolveTestPath(tmpMissingTestFilePath, LocalSharedTestDir)

	// create a path for the target of our put
	tmpRemoteTestDirPath := filepath.Join(CurrentBaseTestDirPath, uuid.New().String())
	resolvedRemoteTestDirPath := _resolveTestPath(tmpRemoteTestDirPath, SFTP_SHARED_TEST_DIR)

	req := &agaveproto.SrvPutRequest{
		SystemConfig: _createRemoteSystemConfig(),
		LocalPath:    resolvedTmpMissingTestFilePath,
		RemotePath:   resolvedRemoteTestDirPath,
		Force:        true,
		Append:       false,
	}
	grpcResponse, err := client.Put(context.Background(), req)
	if err != nil {
		assert.Nilf(t, err, "Error while invoking remote service: %v", err)
	} else {
		// get the test directory stat in the local shared directory
		resolvedLocalTestDirPath := _resolveTestPath(tmpRemoteTestDirPath, LocalSharedTestDir)
		_, err := os.Stat(resolvedLocalTestDirPath)
		if err == nil {
			assert.FailNowf(t, "Missing target file should not be created", "Missing target file, %s, should not be present after attempting to put a missing file", resolvedLocalTestDirPath)
		}
		assert.Contains(t, grpcResponse.Error, "no such file or directory", "Error message in response should state no such file or directory")
		assert.Nil(t, grpcResponse.RemoteFileInfo, "Returned file info should be nil on error")
	}

	afterTest(t)
}

func TestPutFileToMissingDirectoryReturnsErr(t *testing.T) {
	beforeTest(t)

	conn := _getConnection(t)
	defer conn.Close()

	client := agaveproto.NewSftpRelayClient(conn)

	// create a temp file to put
	tmpTestFilePath, err := _createTempFile("", ".bin")
	if err != nil {
		assert.FailNowf(t, err.Error(), "Unable to create temp test file: %s", err.Error())
	}
	resolvedLocalTestFilePath := _resolveTestPath(tmpTestFilePath, LocalSharedTestDir)

	// create a directory as the target of our put
	tmpTestDirPath := filepath.Join(CurrentBaseTestDirPath, uuid.New().String(), uuid.New().String())
	resolvedRemoteTestDirPath := _resolveTestPath(tmpTestDirPath, SFTP_SHARED_TEST_DIR)

	req := &agaveproto.SrvPutRequest{
		SystemConfig: _createRemoteSystemConfig(),
		LocalPath:    resolvedLocalTestFilePath,
		RemotePath:   resolvedRemoteTestDirPath,
		Force:        true,
		Append:       false,
	}
	grpcResponse, err := client.Put(context.Background(), req)
	if err != nil {
		assert.Nilf(t, err, "Error while invoking remote service: %v", err)
	} else {
		resolvedLocalTestDirPath := _resolveTestPath(tmpTestDirPath, LocalSharedTestDir)
		_, err := os.Stat(resolvedLocalTestDirPath)
		if err == nil {
			assert.FailNowf(t, "Missing target dir should not be created", "Missing target directory, %s, should not be created on the remote host after put", resolvedLocalTestDirPath)
		}

		// get the test directory stat in the local shared directory
		assert.Contains(t, grpcResponse.Error, "file does not exist", "Error message in response should state file does not exist")
		assert.Nil(t, grpcResponse.RemoteFileInfo, "Returned file info should be nil on error")
	}

	afterTest(t)
}

func TestPutFileToExistingFileWithoutForceReturnsErr(t *testing.T) {
	beforeTest(t)

	conn := _getConnection(t)
	defer conn.Close()

	client := agaveproto.NewSftpRelayClient(conn)

	// create a temp file to put
	tmpTestFilePath, err := _createTempFile("", ".bin")
	if err != nil {
		assert.FailNowf(t, err.Error(), "Unable to create temp test file: %s", err.Error())
	}
	resolvedLocalTestFilePath := _resolveTestPath(tmpTestFilePath, LocalSharedTestDir)

	tmpTestFileCopyPath, err := _createTempFile("", ".copy")
	if err != nil {
		assert.FailNowf(t, err.Error(), "Unable to create temp test file: %s", err.Error())
	}
	// get the target path of the copy file path on the server
	resolvedRemoteTestFilePath := _resolveTestPath(tmpTestFileCopyPath, SFTP_SHARED_TEST_DIR)

	req := &agaveproto.SrvPutRequest{
		SystemConfig: _createRemoteSystemConfig(),
		LocalPath:    resolvedLocalTestFilePath,
		RemotePath:   resolvedRemoteTestFilePath,
		Force:        false,
		Append:       false,
	}
	grpcResponse, err := client.Put(context.Background(), req)
	if err != nil {
		assert.Nilf(t, err, "Error while invoking remote service: %v", err)
	} else {
		// get the test directory stat in the local shared directory
		assert.Contains(t, grpcResponse.Error, "file already exists", "Error message in response should state file already exists")
		assert.Nil(t, grpcResponse.RemoteFileInfo, "Returned file info should be nil on error")
	}

	afterTest(t)
}

func TestPutFileExistingFileWithForce(t *testing.T) {
	beforeTest(t)

	conn := _getConnection(t)
	defer conn.Close()

	client := agaveproto.NewSftpRelayClient(conn)

	// create a temp file to put
	tmpTestFilePath, err := _createTempFile("", ".bin")
	if err != nil {
		assert.FailNowf(t, err.Error(), "Unable to create temp test file: %s", err.Error())
	}
	resolvedLocalTestFilePath := _resolveTestPath(tmpTestFilePath, LocalSharedTestDir)

	tmpTestFileInfo, err := os.Stat(resolvedLocalTestFilePath)
	if err != nil {
		assert.FailNowf(t, err.Error(), "Unable to open temp test file: %s", err.Error())
	}

	tmpTestFileCopyPath, err := _createTempFile("", ".copy")
	if err != nil {
		assert.FailNowf(t, err.Error(), "Unable to create temp test file: %s", err.Error())
	}
	// get the target path of the copy file path on the server
	resolvedRemoteTestFilePath := _resolveTestPath(tmpTestFileCopyPath, SFTP_SHARED_TEST_DIR)

	req := &agaveproto.SrvPutRequest{
		SystemConfig: _createRemoteSystemConfig(),
		LocalPath:    resolvedLocalTestFilePath,
		RemotePath:   resolvedRemoteTestFilePath,
		Force:        true,
		Append:       false,
	}

	grpcResponse, err := client.Put(context.Background(), req)
	if err != nil {
		assert.Nilf(t, err, "Error while invoking remote service: %v", err)
	} else {
		targetTestFilePath := _resolveTestPath(tmpTestFileCopyPath, LocalSharedTestDir)
		targetTestFileInfo, err := os.Stat(targetTestFilePath)
		if err != nil {
			assert.FailNowf(t, err.Error(), "Unable to open copied file, %s, on remote host file: %s", targetTestFilePath, err.Error())
		}

		assert.Equal(t, "", grpcResponse.Error, "Error message in response should be empty after successfully request")
		assert.Equal(t, tmpTestFileInfo.Size(), targetTestFileInfo.Size(), "Put should preserve the original file size ")
		assert.Equal(t, tmpTestFileInfo.IsDir(), targetTestFileInfo.IsDir(), "Putting a file should result in a file")
		assert.Equal(t, tmpTestFileInfo.Mode().String(), targetTestFileInfo.Mode().String(), "Put should preserve file permissions")
		assert.Equal(t, tmpTestFileInfo.ModTime().Unix(), targetTestFileInfo.ModTime().Unix(), "Put should preserve last updated timestamp")
		assert.Equal(t, resolvedRemoteTestFilePath, grpcResponse.RemoteFileInfo.Path, "Returned file info path should match the path of the Put file")
		assert.Equal(t, targetTestFileInfo.Name(), grpcResponse.RemoteFileInfo.Name, "Returned file info name should match the name of the Put file")
		assert.Equal(t, targetTestFileInfo.Size(), grpcResponse.RemoteFileInfo.Size, "Returned file info size should match the size of the Put file")
		assert.Equal(t, targetTestFileInfo.ModTime().Unix(), grpcResponse.RemoteFileInfo.LastUpdated, "Returned file info last modified date should match the last modified date of the Put file")
		assert.Equal(t, targetTestFileInfo.Mode().String(), grpcResponse.RemoteFileInfo.Mode, "Returned file info mode should match the mode of the Put file")
		assert.False(t, grpcResponse.RemoteFileInfo.IsDirectory, "Returned file info name should not report as a directory")
		assert.False(t, grpcResponse.RemoteFileInfo.IsLink, "Returned file info should not report as a link")

	}

	afterTest(t)
}

func TestGet(t *testing.T) {
	beforeTest(t)

	conn := _getConnection(t)
	defer conn.Close()

	client := agaveproto.NewSftpRelayClient(conn)

	// create a random directory name in our test dir
	tmpTestFilePath, err := _createTempFile("", ".bin")
	if err != nil {
		assert.FailNowf(t, err.Error(), "Unable to create temp test file: %s", err.Error())
	}
	// this will be the path of FileName on the Get requet
	resolvedRemoteTmpTestFilePath := _resolveTestPath(tmpTestFilePath, SFTP_SHARED_TEST_DIR)

	// calculate the local path of the temp file for comparison below
	resolvedTmpTestFilePath := _resolveTestPath(tmpTestFilePath, LocalSharedTestDir)
	tmpTestFileInfo, err := os.Stat(resolvedTmpTestFilePath)
	if err != nil {
		assert.FailNowf(t, err.Error(), "Unable to open temp test file: %s", err.Error())
	}

	// calculate the path on the relay server where the file should be downloaded
	tmpTestFileCopyPath := tmpTestFilePath + ".copy"
	resolvedTargetTestFilePath := _resolveTestPath(tmpTestFileCopyPath, LocalSharedTestDir)

	req := &agaveproto.SrvGetRequest{
		SystemConfig: _createRemoteSystemConfig(),
		LocalPath:    resolvedTargetTestFilePath,
		RemotePath:   resolvedRemoteTmpTestFilePath,
		Force:        true,
	}
	grpcResponse, err := client.Get(context.Background(), req)
	if err != nil {
		assert.Nilf(t, err, "Error while invoking remote service: %v", err)
	} else {
		// get the test directory stat in the local shared directory
		targetTestFileInfo, err := os.Stat(resolvedTargetTestFilePath)
		if err != nil {
			assert.FailNowf(t, err.Error(), "Unable to open downloaded file, %s : %s", resolvedTargetTestFilePath, err.Error())
		}

		assert.Equal(t, "", grpcResponse.Error, "Error message in response should be empty after successfully request")
		assert.Equal(t, tmpTestFileInfo.Size(), targetTestFileInfo.Size(), "Put should preserve the original file size ")
		assert.Equal(t, tmpTestFileInfo.IsDir(), targetTestFileInfo.IsDir(), "Putting a file should result in a file")
		assert.Equal(t, tmpTestFileInfo.Mode().String(), targetTestFileInfo.Mode().String(), "Put should preserve file permissions")
		assert.Equal(t, tmpTestFileInfo.ModTime().Unix(), targetTestFileInfo.ModTime().Unix(), "Put should preserve last updated timestamp")
		assert.Equal(t, resolvedTargetTestFilePath, grpcResponse.RemoteFileInfo.Path, "Returned file info path should match the path of the Get file")
		assert.Equal(t, targetTestFileInfo.Name(), grpcResponse.RemoteFileInfo.Name, "Returned file info name should match the name of the Get file")
		assert.Equal(t, targetTestFileInfo.Size(), grpcResponse.RemoteFileInfo.Size, "Returned file info size should match the size of the Get file")
		assert.Equal(t, targetTestFileInfo.ModTime().Unix(), grpcResponse.RemoteFileInfo.LastUpdated, "Returned file info last modified date should match the last modified date of the Get file")
		assert.Equal(t, targetTestFileInfo.Mode().String(), grpcResponse.RemoteFileInfo.Mode, "Returned file info mode should match the mode of the Get file")
		assert.False(t, grpcResponse.RemoteFileInfo.IsDirectory, "Returned file info name should not report as a directory")
		assert.False(t, grpcResponse.RemoteFileInfo.IsLink, "Returned file info should not report as a link")

	}

	afterTest(t)
}

func TestGetReturnsErrIfRemoteFilePathDoesNotExist(t *testing.T) {
	beforeTest(t)

	conn := _getConnection(t)
	defer conn.Close()

	client := agaveproto.NewSftpRelayClient(conn)

	// create a temp file to put
	tmpTestFileDownloadPath := filepath.Join(CurrentBaseTestDirPath, uuid.New().String())
	resolvedLocalFileDownloadPath := _resolveTestPath(tmpTestFileDownloadPath, LocalSharedTestDir)

	// create a directory as the target of our get
	missingRemoteTestDirPath := filepath.Join(CurrentBaseTestDirPath, uuid.New().String(), uuid.New().String())
	resolvedMissingRemoteTestDirPath := _resolveTestPath(missingRemoteTestDirPath, SFTP_SHARED_TEST_DIR)

	req := &agaveproto.SrvGetRequest{
		SystemConfig: _createRemoteSystemConfig(),
		RemotePath:   resolvedMissingRemoteTestDirPath,
		LocalPath:    resolvedLocalFileDownloadPath,
		Force:        true,
	}
	grpcResponse, err := client.Get(context.Background(), req)
	if err != nil {
		assert.Nilf(t, err, "Error while invoking remote service: %v", err)
	} else {
		_, err := os.Stat(resolvedLocalFileDownloadPath)
		if err == nil {
			assert.FailNowf(t, "Missing target dir should not be created", "Missing target directory, %s, should not be created on the local host after get", resolvedLocalFileDownloadPath)
		}

		// get the test directory stat in the local shared directory
		assert.Contains(t, grpcResponse.Error, "file does not exist", "Error message in response should state file does not exist")
		assert.Nil(t, grpcResponse.RemoteFileInfo, "Returned file info should be nil on error")
	}

	afterTest(t)
}

func TestGetReturnsErrIfRemotePathIsADirectory(t *testing.T) {
	beforeTest(t)

	conn := _getConnection(t)
	defer conn.Close()

	client := agaveproto.NewSftpRelayClient(conn)

	// create a random directory name in our test dir
	tmpTestFileDownloadPath := filepath.Join(CurrentBaseTestDirPath, uuid.New().String())
	resolvedLocalFileDownloadPath := _resolveTestPath(tmpTestFileDownloadPath, LocalSharedTestDir)

	// create a directory as the target of our get
	remoteTmpTestDirPath, err := _createTempDirectory("")
	if err != nil {
		assert.FailNowf(t, err.Error(), "Unable to create temp test dir: %s", err.Error())
	}
	resolvedRemoteTmpTestDirPath := _resolveTestPath(remoteTmpTestDirPath, SFTP_SHARED_TEST_DIR)

	req := &agaveproto.SrvGetRequest{
		SystemConfig: _createRemoteSystemConfig(),
		RemotePath:   resolvedRemoteTmpTestDirPath,
		LocalPath:    resolvedLocalFileDownloadPath,
		Force:        true,
	}
	grpcResponse, err := client.Get(context.Background(), req)
	if err != nil {
		assert.Nilf(t, err, "Error while invoking remote service: %v", err)
	} else {
		_, err := os.Stat(resolvedLocalFileDownloadPath)
		if err == nil {
			assert.FailNowf(t, "Missing target path should not be created", "Missing target path, %s, should not be created on the local host after failed directory get", resolvedLocalFileDownloadPath)
		}
		assert.Contains(t, grpcResponse.Error, "source path is a directory", "Error message in response should indicate the source path is a directory")
		assert.Nil(t, grpcResponse.RemoteFileInfo, "Returned file info should be nil on error")
	}

	afterTest(t)
}

func TestGetReturnsErrIfLocalFilePathDoesNotExist(t *testing.T) {
	beforeTest(t)

	conn := _getConnection(t)
	defer conn.Close()

	client := agaveproto.NewSftpRelayClient(conn)

	// create a random directory name in our test dir
	tmpTestFilePath, err := _createTempFile("", ".bin")
	if err != nil {
		assert.FailNowf(t, err.Error(), "Unable to create temp test file: %s", err.Error())
	}
	// this will be the path of FileName on the Get requet
	resolvedRemoteTmpTestFilePath := _resolveTestPath(tmpTestFilePath, SFTP_SHARED_TEST_DIR)

	// create a directory as the target of our get
	missingDownloadTestDirPath := filepath.Join(CurrentBaseTestDirPath, uuid.New().String(), uuid.New().String())
	resolvedMissingRemoteTestDirPath := _resolveTestPath(missingDownloadTestDirPath, LocalSharedTestDir)

	req := &agaveproto.SrvGetRequest{
		SystemConfig: _createRemoteSystemConfig(),
		RemotePath:   resolvedRemoteTmpTestFilePath,
		LocalPath:    resolvedMissingRemoteTestDirPath,
		Force:        false,
	}
	grpcResponse, err := client.Get(context.Background(), req)
	if err != nil {
		assert.Nilf(t, err, "Error while invoking remote service: %v", err)
	} else {
		_, err := os.Stat(resolvedMissingRemoteTestDirPath)
		if err == nil {
			assert.FailNowf(t, "Missing target dir should not be created", "Missing target directory, %s, should not be created on the local host after get", resolvedMissingRemoteTestDirPath)
		}

		// get the test directory stat in the local shared directory
		assert.Contains(t, grpcResponse.Error, "no such file or directory", "Error message in response should state no such file or directory")
		assert.Nil(t, grpcResponse.RemoteFileInfo, "Returned file info should be nil on error")
	}

	afterTest(t)
}

func TestGetReturnsErrIfLocalFilePathIsADirectory(t *testing.T) {
	beforeTest(t)

	conn := _getConnection(t)
	defer conn.Close()

	client := agaveproto.NewSftpRelayClient(conn)

	// create a random directory name in our test dir
	tmpTestFilePath, err := _createTempFile("", ".bin")
	if err != nil {
		assert.FailNowf(t, err.Error(), "Unable to create temp test file: %s", err.Error())
	}
	// this will be the path of FileName on the Get requet
	resolvedRemoteTmpTestFilePath := _resolveTestPath(tmpTestFilePath, SFTP_SHARED_TEST_DIR)

	// create a directory as the target of our get
	tmpDownloadTestDirPath, err := _createTempDirectory("")
	if err != nil {
		assert.FailNowf(t, err.Error(), "Unable to create temp test dir: %s", err.Error())
	}
	resolvedTmpDownloadTestDirPath := _resolveTestPath(tmpDownloadTestDirPath, LocalSharedTestDir)

	req := &agaveproto.SrvGetRequest{
		SystemConfig: _createRemoteSystemConfig(),
		RemotePath:   resolvedRemoteTmpTestFilePath,
		LocalPath:    resolvedTmpDownloadTestDirPath,
		Force:        true,
	}
	grpcResponse, err := client.Get(context.Background(), req)
	if err != nil {
		assert.Nilf(t, err, "Error while invoking remote service: %v", err)
	} else {
		tmpDownloadTestDirInfo, err := os.Stat(resolvedTmpDownloadTestDirPath)
		if err != nil {
			assert.FailNowf(t, err.Error(), "Unable to open temp directory, %s, on remote host after put: %v", tmpDownloadTestDirPath, err)
		}
		assert.True(t, tmpDownloadTestDirInfo.IsDir(), "Getting a file to a directory should fail and preserve the target directory")
		assert.Contains(t, grpcResponse.Error, "destination path is a directory", "Error message in response should indicate the destination path is a directory")
		assert.Nil(t, grpcResponse.RemoteFileInfo, "Returned file info should be nil on error")
	}

	afterTest(t)
}
