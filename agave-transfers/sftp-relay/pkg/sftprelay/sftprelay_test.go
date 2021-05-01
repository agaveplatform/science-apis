package sftprelay

import (
	"context"
	"entrogo.com/sshpool/pkg/clientpool"
	"fmt"
	agaveproto "github.com/agaveplatform/science-apis/agave-transfers/sftp-relay/pkg/sftpproto"
	"github.com/google/uuid"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
	"io/ioutil"
	"net"
	"os"
	"os/exec"
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

var testPool = clientpool.New(
	clientpool.WithPoolSize(10),
	clientpool.WithExpireAfter(time.Duration(30) * time.Second))

func init() {

	LocalSharedTestDir = resolveLocalSharedTestDir()

	// set the server log to debug
	log.SetLevel(logrus.DebugLevel)

	//set up for the servers
	lis = bufconn.Listen(bufSize)
	s := grpc.NewServer()

	// Init a new api server to register with the grpc server
	relaysvr := Server{
		Registry:    *prometheus.NewRegistry(),
		GrpcMetrics: *grpc_prometheus.NewServerMetrics(),
		Pool:        testPool,
	}
	// set up prometheus metrics
	relaysvr.InitMetrics()
	relaysvr.SetLogLevel(logrus.TraceLevel)
	agaveproto.RegisterSftpRelayServer(s, &relaysvr)
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

// dumps the current pool stats to stdout
func printPoolStats() {
	poolStats := testPool.PoolStats()
	for id,val := range poolStats {
		consolelog.Debugf("  - [%s]: %d", id, val)
		//consolelog.Debugf("  - [%s]: %d", id[0:strings.LastIndex(id, ":")], val)
	}
}

func beforeTest(t *testing.T) {
	consolelog.Debugf("***************************************************************************************\n")
	// create a new unique test directory for the test
	CurrentBaseTestDirPath = filepath.Join("sftprelay_test", t.Name(), uuid.New().String())

	// create the directory on the local file system within the shared folder
	sharedRandomTestDirPath := _resolveTestPath(CurrentBaseTestDirPath, LocalSharedTestDir)
	consolelog.Debugf("Creating test directory for test %s, %s", t.Name(), CurrentBaseTestDirPath)

	consolelog.Debugf("Connection pool stats before test: ")
	printPoolStats()

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

		consolelog.Debugf("Connection pool stats after test: ")
		printPoolStats()

		consolelog.Debugf("Removing test directory for test %s, %s", t.Name(), sharedRandomTestDirPath)
		consolelog.Debugf("***************************************************************************************")
		consolelog.Debugf(" ")
		consolelog.Debugf(" ")

		//err := os.RemoveAll(sharedRandomTestDirPath)
		//if err != nil {
		//	consolelog.Errorf("Failed to remove directory for test %s, %s", t.Name(), sharedRandomTestDirPath)
		//}
	}
}

// creates a temp directory in the CurrentBaseTestDirPath
func _createTempDirectory(prefix string) (string, error) {
	return _createTempDirectoryInDirectory(CurrentBaseTestDirPath, prefix)
}

// grants user 1000 recursive permissions on the local share test dir. This is
// the same uid of the testuser in the sftp container used in these tests.
func _updateLocalSharedTestDirOwnership() error {
	cmd := exec.Command("chown", "-R", "1000:1000", LocalSharedTestDir)
	return cmd.Run()
}

// creates a temp directory within the parentPath. This will be resolved relative to the
// LocalSharedTestDir, so keep all paths relative to the CurrentBaseTestDirPath for simplicity
func _createTempDirectoryInDirectory(parentPath string, prefix string) (string, error) {
	// create a new unique directory name in our current test directory
	tempDir := filepath.Join(parentPath, fmt.Sprintf("%s%s", prefix, uuid.New().String()))

	// create the directory
	err := os.MkdirAll(_resolveTestPath(tempDir, LocalSharedTestDir), os.ModePerm)
	if err == nil {
		return tempDir, err
	}

	err = _updateLocalSharedTestDirOwnership()

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
	os.Chown(resolvedTempFilePath, 1000, 1000)

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

func _createRemoteSshKeySystemConfig() *agaveproto.RemoteSystemConfig {
	return &agaveproto.RemoteSystemConfig{
		Username: "testuser",
		PrivateKey: `-----BEGIN RSA PRIVATE KEY-----
MIIEpAIBAAKCAQEAxJLvcXKxxTCzm5q4EIyKC82x4lW3Fv9OQS/VkxMVFaf/uy4T
Qjn3Qx6l1WInA+rH+FHUfKP3PBVjI7Uga4xK6qFIeCGl0XsQ+NA5IVxxFysvgGZy
wwSvxGwO16X4NUk3+7GSMLYRL+4M4Oq2yv7vuVnDhh1/hzfDMERLUvF7eoQQACV1
4LxPBmlBJf3qE0A9VTr+I23BYEtqJEEcWOewFfZSwsvRW5tlQsYVeHx/ToXjucki
W3Gqhd9j+UqX4MWOsN8HVBMTxmIEn38HbaByQaHtmqlusKGMcWuT9aFeTNPMyAa0
UKzd6qe4ZlOn8jz5P/jZw0oegL9aR6HQuouftQIDAQABAoIBAQCL6Jy9nUmDtO8Q
4CUDulOCpStnkWRX3OygnuAe5uUJ3eG5IskYSNOBFS4o2sw0EIW1auCWucj9HafL
QV5KzbaAmrxOrHwtxa7FuMYAxZ/EQrtzYvdpcEt9vP8vY3Ru0Kck4DTRsLQ47fCC
oOvrPVn4DTiJmzMqVXj6QJFv/mnJPzbeFcgAdmQs1t5YgUnOKgGRutdPuQzjasct
9jpywQIU/Lokw/9HXAxQ8wYh8LltmLkIAM70LZ219X+jW0M3pH9mPoR+qe8P8DD+
voYND904st6I9RWNpfFaC0yVo9bMaQUi1GvZ5m18uZnmR8vCz1YeiGVKVkNRg3sb
q1GQBA2dAoGBAPVGKa0XNPyXDKvKZviFecpLe4qsYGHR6pvrI1hox8QMsk213G3V
W3DLs+yN6oXAl8ueRQueNel5UShQDUuu8WVr+8bh0eMHnPUdp1W72SSR0LUbAoc2
aogZjYefGOF6l2rg9u+3u1wz9q++dwlxfV0ONOgwnvRzLNKmNetf7N07AoGBAM0r
k3aETvh0JEXY5k7AxIZCaXMh9dHc9f7GEa73qb6BbqhGNFDL57HJycTqNAqLWCEj
Hjt+gUN4DZRUi4028Jy0g5C43UGSqim4MKHtz0KIE4XqweuNGOiKLiEk9JIuIHi8
DXFT5646ZJQyB8zyXxaky+tHg8myYtEiT5uu5mfPAoGBAIz9D+fXdzXa/gWiCx7A
WwnV6eYEwEJ6kAmgWGjxkiM3ySaya0sXYcCs13ga+7x2wMri532OLB9RBT3PBlWC
8nACanAgTq3aKncb3JyDpoZG61mvdPyUYxho19olsf+qoG9ncYrKaoDNvfe33GUp
P47GI9N0X2rU6ecMc+Ig+d8RAoGAIHB1XlXJDqt+WLTUpTsBV6EEfzmtXkMred7j
SODLq91XG7AN8YBr38Zh6oqFM+2YP5UH4Kw9z+cZbox3jBpVrNE1xBoWkZmY4gPH
XLL3BDPzskbN5mpmt82xQXhQWxSD+dLx5Ss5BGkjIfNPHG5t7myb+VVTVv6ndR2R
rLHYqC0CgYAl+ZtbVyEe3BLlI/+CMv5xSz23RiHS7Gtq8W9DuDEa93c2gm1AeXS2
v+XY/WvLin8V5NyIkDMLwhOdPgWHoi27Tkr73MtQit7AIK66UxydjTF4mXEthpSF
/ZRodeQLTBOzfsczG/xqbYodaitvTym3SqbCnR0I6+xGp2HY9KUpEQ==
-----END RSA PRIVATE KEY-----
`,
		Host:     "sftp",
		Port:     10022,
	}
}

func bufDialer(string, time.Duration) (net.Conn, error) {
	return lis.Dial()
}

func TestAuthenticateWithPasswordSucceeds(t *testing.T) {
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

func TestAuthenticateWithInvalidPasswordReturnsError(t *testing.T) {
	beforeTest(t)

	conn := _getConnection(t)
	defer conn.Close()

	client := agaveproto.NewSftpRelayClient(conn)

	systemConfig := _createRemoteSystemConfig()
	systemConfig.Password = "foo"

	req := &agaveproto.AuthenticationCheckRequest{
		SystemConfig: systemConfig,
	}

	grpcResponse, err := client.AuthCheck(context.Background(), req)
	if err != nil {
		assert.Nilf(t, err, "Error while calling RPC Auth: %v", err)
	} else {
		assert.Contains(t, strings.ToLower(grpcResponse.Error), "handshake failed", "AuthCheck with invalid password should return error message stating handshake failed")
	}

	afterTest(t)
}

func TestAuthenticateWithWithInvalidRSAKeyReturnsError(t *testing.T) {
	beforeTest(t)

	conn := _getConnection(t)
	defer conn.Close()

	client := agaveproto.NewSftpRelayClient(conn)

	systemConfig := _createRemoteSshKeySystemConfig()
	systemConfig.PrivateKey = "foo"

	req := &agaveproto.AuthenticationCheckRequest{
		SystemConfig: systemConfig,
	}

	grpcResponse, err := client.AuthCheck(context.Background(), req)
	if err != nil {
		assert.Nilf(t, err, "Error while calling RPC Auth: %v", err)
	} else {
		assert.Contains(t, strings.ToLower(grpcResponse.Error), "no key found", "AuthCheck with invalid ssh key should return error stating no key found")
	}

	afterTest(t)
}

func TestAuthenticateWithRSAKeySucceeds(t *testing.T) {
	beforeTest(t)

	conn := _getConnection(t)
	defer conn.Close()

	client := agaveproto.NewSftpRelayClient(conn)

	req := &agaveproto.AuthenticationCheckRequest{
		SystemConfig: _createRemoteSshKeySystemConfig(),
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
	}
	assert.Equal(t, "", grpcResponse.Error, "Stat on existing file should return empty error")
	if grpcResponse.Error == "" {
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
	}
	assert.Equal(t, "", grpcResponse.Error, "Stat on existing file should return empty error")
	if grpcResponse.Error == "" {
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
		assert.Contains(t, strings.ToLower(grpcResponse.Error), "does not exist", err)
		assert.Nil(t, grpcResponse.RemoteFileInfo, "Returned file info should be nil when an error occurs")
	}

	afterTest(t)
}

func TestMkdir(t *testing.T) {

	beforeTest(t)

	conn := _getConnection(t)
	defer conn.Close()

		client := agaveproto.NewSftpRelayClient(conn)

	// create a random directory name in our test dir
	testDirectoryPath := fmt.Sprintf("%s/%s", CurrentBaseTestDirPath, uuid.New().String())

	err := _updateLocalSharedTestDirOwnership()
	if err != nil {
		assert.FailNowf(t, err.Error(), "Unable to change permission on temp test dir: %s", err.Error())
	}

	// resolve it to the absolute path within our shared test directory on the remote system
	remoteTestDirectoryPath := _resolveTestPath(testDirectoryPath, SFTP_SHARED_TEST_DIR)

	req := &agaveproto.SrvMkdirRequest{
		SystemConfig: _createRemoteSshKeySystemConfig(),
		RemotePath:   remoteTestDirectoryPath,
		Recursive: false,
	}
	grpcResponse, err := client.Mkdir(context.Background(), req)
	if err != nil {
		assert.Nilf(t, err, "Error while invoking remote service: %v", err)
	} else {
		// get the test directory stat in the local shared directory
		tmpTestDirInfo, err := os.Stat(_resolveTestPath(testDirectoryPath, LocalSharedTestDir))
		if os.IsNotExist(err) {
			assert.FailNowf(t, err.Error(), "Test directory was not created on remote host: %s", err.Error())
		}
		assert.Equal(t, "", grpcResponse.Error, "Mkdirs on valid remote should return empty Error")
		if grpcResponse.Error == "" {
			assert.True(t, tmpTestDirInfo.IsDir(), "Remote path should be a directory. File found instead.")
			assert.Equal(t, remoteTestDirectoryPath, grpcResponse.RemoteFileInfo.Path, "Returned file name should match the name of the new directory")
			assert.Equal(t, tmpTestDirInfo.Name(), grpcResponse.RemoteFileInfo.Name, "Returned file info name should match the name of the new directory")
			assert.Equal(t, tmpTestDirInfo.Size(), grpcResponse.RemoteFileInfo.Size, "Returned file info size should match the size of the new directory")
			assert.Equal(t, tmpTestDirInfo.ModTime().Unix(), grpcResponse.RemoteFileInfo.LastUpdated, "Returned file info last modified date should match the last modified date of the new directory")
			assert.Equal(t, tmpTestDirInfo.Mode().String(), grpcResponse.RemoteFileInfo.Mode, "Returned file info mode should match the mode of the new directory")
			assert.True(t, grpcResponse.RemoteFileInfo.IsDirectory, "Returned file info should report as a directory")
			assert.False(t, grpcResponse.RemoteFileInfo.IsLink, "Returned file info should not report as a link")
		}
	}

	afterTest(t)
}

func TestMkdirReturnsErrorWhenPathIsForbidden(t *testing.T) {

	beforeTest(t)

	conn := _getConnection(t)
	defer conn.Close()

	client := agaveproto.NewSftpRelayClient(conn)

	// create a random directory name in our test dir
	remoteTestFilePath := "/" + uuid.New().String()

	req := &agaveproto.SrvMkdirRequest{
		SystemConfig: _createRemoteSystemConfig(),
		RemotePath:   remoteTestFilePath,
		Recursive: false,
	}
	grpcResponse, err := client.Mkdir(context.Background(), req)
	if err != nil {
		assert.Nilf(t, err, "Error while invoking remote service: %v", err)
	} else {
		// get the test directory stat in the local shared directory
		assert.Contains(t, strings.ToLower(grpcResponse.Error), "permission denied", "Non-recursive Mkdirs on forbidden path should return response saying permission denied")
		assert.Nil(t, grpcResponse.RemoteFileInfo, "Returned file info should be nil when an error occurs")
	}

	afterTest(t)
}

func TestMkdirReturnsErrorWhenPathIsExistingFile(t *testing.T) {

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

	req := &agaveproto.SrvMkdirRequest{
		SystemConfig: _createRemoteSystemConfig(),
		RemotePath:   remoteTestFilePath,
		Recursive: false,
	}
	grpcResponse, err := client.Mkdir(context.Background(), req)
	if err != nil {
		assert.Nilf(t, err, "Error while invoking remote service: %v", err)
	} else {
		// get the test directory stat in the local shared directory
		tmpTestFileInfo, err := os.Stat(_resolveTestPath(tmpTestFilePath, LocalSharedTestDir))
		if err != nil {
			assert.FailNowf(t, err.Error(), "Unable to open temp test file after calling mkdir: %s", err.Error())
		}
		assert.False(t, tmpTestFileInfo.IsDir(), "Remote path should still be a file after calling mkdir on it.")
		assert.Contains(t, strings.ToLower(grpcResponse.Error), "file already exists", "Mkdirs on existing file should return response saying file already exists")
		assert.Nil(t, grpcResponse.RemoteFileInfo, "Returned file info should be nil when an error occurs")
	}

	afterTest(t)
}

func TestMkdirReturnsErrorWhenPathIsExistingDirectory(t *testing.T) {

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

	req := &agaveproto.SrvMkdirRequest{
		SystemConfig: _createRemoteSystemConfig(),
		RemotePath:   resolvedRemoteTestFilePath,
		Recursive: false,
	}
	grpcResponse, err := client.Mkdir(context.Background(), req)
	if err != nil {
		assert.Nilf(t, err, "Error while invoking remote service: %v", err)
	} else {
		// get the test directory stat in the local shared directory
		assert.Equal(t, "dir already exists", grpcResponse.Error, "Non recursive Mkdir on existing directory should return an error stating dir already exists")
		assert.Nil(t, grpcResponse.RemoteFileInfo, "Returned file info should be nil when an error occurs")

	}

	afterTest(t)
}

func TestMkdirReturnsErrorWhenParentPathDoesNotExist(t *testing.T) {

	beforeTest(t)

	conn := _getConnection(t)
	defer conn.Close()

	client := agaveproto.NewSftpRelayClient(conn)

	// create a random directory name in our test dir
	testDirectoryPath := filepath.Join(CurrentBaseTestDirPath, uuid.New().String(),uuid.New().String(),uuid.New().String())

	resolvedRemoteTestFilePath := _resolveTestPath(testDirectoryPath, SFTP_SHARED_TEST_DIR)

	req := &agaveproto.SrvMkdirRequest{
		SystemConfig: _createRemoteSystemConfig(),
		RemotePath:   resolvedRemoteTestFilePath,
		Recursive: false,
	}
	grpcResponse, err := client.Mkdir(context.Background(), req)
	if err != nil {
		assert.Nilf(t, err, "Error while invoking remote service: %v", err)
	} else {
		// get the test directory stat in the local shared directory
		_, err := os.Stat(_resolveTestPath(testDirectoryPath, LocalSharedTestDir))
		assert.Truef(t, os.IsNotExist(err), "Nested directory should not be created when parent does not exist: %s", err.Error())
		assert.Equal(t, "file does not exist", grpcResponse.Error, "Non recursive Mkdir on existing directory should return an error stating file does not exist")
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

	err := _updateLocalSharedTestDirOwnership()
	if err != nil {
		assert.FailNowf(t, err.Error(), "Unable to change permission on temp test dir: %s", err.Error())
	}

	// resolve it to the absolute path within our shared test directory on the remote system
	remoteTestDirectoryPath := _resolveTestPath(testDirectoryPath, SFTP_SHARED_TEST_DIR)

	req := &agaveproto.SrvMkdirRequest{
		SystemConfig: _createRemoteSystemConfig(),
		RemotePath:   remoteTestDirectoryPath,
		Recursive: true,
	}
	grpcResponse, err := client.Mkdir(context.Background(), req)
	if err != nil {
		assert.Nilf(t, err, "Error while invoking remote service: %v", err)
	} else {
		// get the test directory stat in the local shared directory
		tmpTestDirInfo, err := os.Stat(_resolveTestPath(testDirectoryPath, LocalSharedTestDir))
		if os.IsNotExist(err) {
			assert.FailNowf(t, err.Error(), "Recursive Mkdir should created nested test directory on remote host: %s", err.Error())
		}
		assert.Equal(t, "", grpcResponse.Error, "Recursive Mkdirs on valid remote path should return empty Error")
		if grpcResponse.Error == "" {
			assert.True(t, tmpTestDirInfo.IsDir(), "Remote path should be a directory. File found instead.")
			assert.Equal(t, remoteTestDirectoryPath, grpcResponse.RemoteFileInfo.Path, "Returned file name should match the name of the new directory")
			assert.Equal(t, tmpTestDirInfo.Name(), grpcResponse.RemoteFileInfo.Name, "Returned file info name should match the name of the new directory")
			assert.Equal(t, tmpTestDirInfo.Size(), grpcResponse.RemoteFileInfo.Size, "Returned file info size should match the size of the new directory")
			assert.Equal(t, tmpTestDirInfo.ModTime().Unix(), grpcResponse.RemoteFileInfo.LastUpdated, "Returned file info last modified date should match the last modified date of the new directory")
			assert.Equal(t, tmpTestDirInfo.Mode().String(), grpcResponse.RemoteFileInfo.Mode, "Returned file info mode should match the mode of the new directory")
			assert.True(t, grpcResponse.RemoteFileInfo.IsDirectory, "Returned file info should report as a directory")
			assert.False(t, grpcResponse.RemoteFileInfo.IsLink, "Returned file info should not report as a link")
		}
	}

	afterTest(t)
}

func TestMkdirsReturnsErrorWhenPathIsForbidden(t *testing.T) {

	beforeTest(t)

	conn := _getConnection(t)
	defer conn.Close()

	client := agaveproto.NewSftpRelayClient(conn)

	// create a random directory name in our test dir
	remoteTestFilePath := "/" + uuid.New().String()

	req := &agaveproto.SrvMkdirRequest{
		SystemConfig: _createRemoteSystemConfig(),
		RemotePath:   remoteTestFilePath,
		Recursive: true,
	}
	grpcResponse, err := client.Mkdir(context.Background(), req)
	if err != nil {
		assert.Nilf(t, err, "Error while invoking remote service: %v", err)
	} else {
		// get the test directory stat in the local shared directory
		assert.Contains(t, strings.ToLower(grpcResponse.Error), "permission denied", "Non-recursive Mkdirs on forbidden path should return response saying permission denied")
		assert.Nil(t, grpcResponse.RemoteFileInfo, "Returned file info should be nil when an error occurs")
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

	req := &agaveproto.SrvMkdirRequest{
		SystemConfig: _createRemoteSystemConfig(),
		RemotePath:   remoteTestFilePath,
		Recursive: true,
	}
	grpcResponse, err := client.Mkdir(context.Background(), req)
	if err != nil {
		assert.Nilf(t, err, "Error while invoking remote service: %v", err)
	} else {
		// get the test directory stat in the local shared directory
		tmpTestFileInfo, err := os.Stat(_resolveTestPath(tmpTestFilePath, LocalSharedTestDir))
		if err != nil {
			assert.FailNowf(t, err.Error(), "Unable to open temp test file after calling mkdir: %s", err.Error())
		} else {
			assert.False(t, tmpTestFileInfo.IsDir(), "Remote path should still be a file after calling mkdir on it.")
			assert.Contains(t, strings.ToLower(grpcResponse.Error), "not a directory", "Mkdirs on existing file should return response saying file already exists")
			assert.Nil(t, grpcResponse.RemoteFileInfo, "Returned file info should be nil when an error occurs")
		}
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

	req := &agaveproto.SrvMkdirRequest{
		SystemConfig: _createRemoteSystemConfig(),
		RemotePath:   resolvedRemoteTestFilePath,
		Recursive: true,
	}
	grpcResponse, err := client.Mkdir(context.Background(), req)
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
		if grpcResponse.Error == "" {
			assert.Equal(t, resolvedRemoteTestFilePath, grpcResponse.RemoteFileInfo.Path, "Returned file info path should match the path of the Put file")
			assert.Equal(t, tmpTestDirInfo.Name(), grpcResponse.RemoteFileInfo.Name, "Returned file info name should match the name of the Put file")
			assert.Equal(t, tmpTestDirInfo.Size(), grpcResponse.RemoteFileInfo.Size, "Returned file info size should match the size of the Put file")
			assert.Equal(t, tmpTestDirInfo.ModTime().Unix(), grpcResponse.RemoteFileInfo.LastUpdated, "Returned file info last modified date should match the last modified date of the Put file")
			assert.Equal(t, tmpTestDirInfo.Mode().String(), grpcResponse.RemoteFileInfo.Mode, "Returned file info mode should match the mode of the Put file")
			assert.True(t, grpcResponse.RemoteFileInfo.IsDirectory, "Returned file info should report as a directory")
			assert.False(t, grpcResponse.RemoteFileInfo.IsLink, "Returned file info should not report as a link")
		}

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

	err = _updateLocalSharedTestDirOwnership()
	if err != nil {
		assert.FailNowf(t, err.Error(), "Unable to change permission on temp test dir: %s", err.Error())
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

	err = _updateLocalSharedTestDirOwnership()
	if err != nil {
		assert.FailNowf(t, err.Error(), "Unable to change permission on temp test dir: %s", err.Error())
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
		absoluteTmpTestDirPath := _resolveTestPath(tmpTestDirPath, LocalSharedTestDir)
		_, err := os.Stat(absoluteTmpTestDirPath)
		assert.True(t, os.IsNotExist(err), "Directory " + absoluteTmpTestDirPath + " should not be present after calling Remove")
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

	err = _updateLocalSharedTestDirOwnership()
	if err != nil {
		assert.FailNowf(t, err.Error(), "Unable to change permission on temp test dir: %s", err.Error())
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

	err = _updateLocalSharedTestDirOwnership()
	if err != nil {
		assert.FailNowf(t, err.Error(), "Unable to change permission on temp test dir: %s", err.Error())
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
		if grpcResponse.Error == "" {
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
		assert.Contains(t, strings.ToLower(grpcResponse.Error), "destination path is a directory", "Error message in response should indicate the destination path is a directory")
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
		assert.Contains(t, strings.ToLower(grpcResponse.Error), "no such file or directory", "Error message in response should state no such file or directory")
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
		assert.Contains(t, strings.ToLower(grpcResponse.Error), "file does not exist", "Error message in response should state file does not exist")
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
		assert.Contains(t, strings.ToLower(grpcResponse.Error), "file already exists", "Error message in response should state file already exists")
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
		if grpcResponse.Error == "" {
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
		if grpcResponse.Error == "" {
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
		assert.Contains(t, strings.ToLower(grpcResponse.Error), "file does not exist", "Error message in response should state file does not exist")
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
		assert.Contains(t, strings.ToLower(grpcResponse.Error), "source path is a directory", "Error message in response should indicate the source path is a directory")
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
		assert.Contains(t, strings.ToLower(grpcResponse.Error), "no such file or directory", "Error message in response should state no such file or directory")
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
		assert.Contains(t, strings.ToLower(grpcResponse.Error), "destination path is a directory", "Error message in response should indicate the destination path is a directory")
		assert.Nil(t, grpcResponse.RemoteFileInfo, "Returned file info should be nil on error")
	}

	afterTest(t)
}

func TestListDirectory(t *testing.T) {
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
	_, err = _createTempFileInDirectory(tmpTestDirPath, "1", ".bin")
	_, err = _createTempFileInDirectory(tmpTestDirPath, "2", ".bin")
	_, err = _createTempFileInDirectory(tmpTestDirPath, "3", ".bin")

	// creating a nested directory structure in the temp dir
	tmpNestedTestDirPath, err := _createTempDirectoryInDirectory(tmpTestDirPath, "")
	_, err = _createTempFileInDirectory(tmpNestedTestDirPath, "4", ".bin")
	_, err = _createTempFileInDirectory(tmpNestedTestDirPath, "5", ".bin")

	remoteTestDirPath := _resolveTestPath(tmpTestDirPath, SFTP_SHARED_TEST_DIR)

	req := &agaveproto.SrvListRequest{
		SystemConfig: _createRemoteSystemConfig(),
		RemotePath:   remoteTestDirPath,
	}
	grpcResponse, err := client.List(context.Background(), req)
	if err != nil {
		assert.Nilf(t, err, "Error while invoking remote service: %v", err)
	} else {
		assert.Equal(t, "", grpcResponse.Error, "Error message in response should be empty after successful request")
		if grpcResponse.Error == "" {
			// get the test directory stat in the local shared directory
			resolvedtmpTestDirPath := _resolveTestPath(tmpTestDirPath, LocalSharedTestDir)
			localTmpFileList, err := ioutil.ReadDir(resolvedtmpTestDirPath)
			if err != nil {
				assert.Failf(t, "Unable to list local directory %s", resolvedtmpTestDirPath)
			}
			assert.Equal(t, len(localTmpFileList), len(grpcResponse.Listing), "Number of entries returned from List should be the same as number of test file items.")
			var found = false
			for _, fileInfo := range localTmpFileList {
				for _, remoteFileInfo := range grpcResponse.Listing {
					if remoteFileInfo.Name == fileInfo.Name() {
						found = true
						break
					}
				}

				assert.True(t, found, "Response from List should include all top level files and folders. %s was missing.", filepath.Join(resolvedtmpTestDirPath, fileInfo.Name()))
			}
		}
	}

	afterTest(t)
}

func TestRenameFile(t *testing.T) {
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

	err = _updateLocalSharedTestDirOwnership()
	if err != nil {
		assert.FailNowf(t, err.Error(), "Unable to change permission on temp test dir: %s", err.Error())
	}

	tmpRenamedFilePath := tmpTestFilePath + "-renamed"
	renamedRemoteFilePath := _resolveTestPath(tmpRenamedFilePath, SFTP_SHARED_TEST_DIR)

	remoteTestFilePath := _resolveTestPath(tmpTestFilePath, SFTP_SHARED_TEST_DIR)

	req := &agaveproto.SrvRenameRequest{
		SystemConfig: _createRemoteSystemConfig(),
		RemotePath:   remoteTestFilePath,
		NewName: renamedRemoteFilePath,
	}

	grpcResponse, err := client.Rename(context.Background(), req)
	if err != nil {
		assert.Nilf(t, err, "Error while calling RPC Stat: %v", err)
	} else {
		_, err := os.Stat(_resolveTestPath(tmpTestFilePath, LocalSharedTestDir))
		if ! os.IsNotExist(err) {
			assert.FailNowf(t, err.Error(), "Original temp file should not be present after rename operation: %s", err.Error())
		} else if grpcResponse.RemoteFileInfo == nil {
			assert.FailNow(t, "Returned file info should not be nil on successful rename")
		} else {
			assert.Equal(t, "", grpcResponse.Error, "Rename on existing file should return empty error")
			if grpcResponse.Error == "" {
				assert.Equal(t, tmpTestFileInfo.Size(), grpcResponse.RemoteFileInfo.Size, "Returned file size should match the test file size")
				assert.Equal(t, tmpTestFileInfo.Name()+"-renamed", grpcResponse.RemoteFileInfo.Name, "Returned file name should match the name of the test file")
				assert.Equal(t, renamedRemoteFilePath, grpcResponse.RemoteFileInfo.Path, "Returned file path should match the path of the test file")
				assert.Equal(t, tmpTestFileInfo.IsDir(), grpcResponse.RemoteFileInfo.IsDirectory, "Returned directory flag should match the flag of the test file")
				assert.Equal(t, tmpTestFileInfo.Mode().String(), grpcResponse.RemoteFileInfo.Mode, "Returned mode should match the mode of the test file")
				assert.Equal(t, tmpTestFileInfo.ModTime().Unix(), grpcResponse.RemoteFileInfo.LastUpdated, "Returned last updated timestamp should match the test file")
			}
		}
	}

	afterTest(t)
}

func TestRenameEmptyDirectory(t *testing.T) {
	beforeTest(t)

	conn := _getConnection(t)
	defer conn.Close()

	client := agaveproto.NewSftpRelayClient(conn)

	tmpTestDirPath, err := _createTempDirectory("")
	if err != nil {
		assert.FailNowf(t, err.Error(), "Unable to create temp test file: %s", err.Error())
	}

	tmpTestDirInfo, err := os.Stat(_resolveTestPath(tmpTestDirPath, LocalSharedTestDir))
	if err != nil {
		assert.FailNowf(t, err.Error(), "Unable to open temp test file: %s", err.Error())
	}

	err = _updateLocalSharedTestDirOwnership()
	if err != nil {
		assert.FailNowf(t, err.Error(), "Unable to change permission on temp test dir: %s", err.Error())
	}

	tmpRenamedDirPath := tmpTestDirPath + "-renamed"
	renamedRemoteDirPath := _resolveTestPath(tmpRenamedDirPath, SFTP_SHARED_TEST_DIR)

	remoteTestDirPath := _resolveTestPath(tmpTestDirPath, SFTP_SHARED_TEST_DIR)

	req := &agaveproto.SrvRenameRequest{
		SystemConfig: _createRemoteSystemConfig(),
		RemotePath:   remoteTestDirPath,
		NewName: renamedRemoteDirPath,
	}

	grpcResponse, err := client.Rename(context.Background(), req)
	if err != nil {
		assert.Nilf(t, err, "Error while calling RPC Stat: %v", err)
	} else {
		_, err := os.Stat(_resolveTestPath(tmpTestDirPath, LocalSharedTestDir))
		if ! os.IsNotExist(err) {
			assert.FailNowf(t, err.Error(), "Original temp dir should not be present after rename operation: %s", err.Error())
		} else if grpcResponse.RemoteFileInfo == nil {
			assert.FailNow(t, "Returned file info should not be nil on successful rename")
		} else {
			assert.Equal(t, "", grpcResponse.Error, "Rename on existing dir should return empty error")
			if grpcResponse.Error == "" {
				assert.Equal(t, tmpTestDirInfo.Size(), grpcResponse.RemoteFileInfo.Size, "Returned file size should match the test dir size")
				assert.Equal(t, tmpTestDirInfo.Name()+"-renamed", grpcResponse.RemoteFileInfo.Name, "Returned file name should match the name of the test dir")
				assert.Equal(t, renamedRemoteDirPath, grpcResponse.RemoteFileInfo.Path, "Returned file path should match the path of the test dir")
				assert.Equal(t, tmpTestDirInfo.IsDir(), grpcResponse.RemoteFileInfo.IsDirectory, "Returned directory flag should match the flag of the test dir")
				assert.Equal(t, tmpTestDirInfo.Mode().String(), grpcResponse.RemoteFileInfo.Mode, "Returned mode should match the mode of the test dir")
				assert.Equal(t, tmpTestDirInfo.ModTime().Unix(), grpcResponse.RemoteFileInfo.LastUpdated, "Returned last updated timestamp should match the test dir")
			}
		}
	}

	afterTest(t)
}

func TestRenameDirectoryTree(t *testing.T) {
	beforeTest(t)

	conn := _getConnection(t)
	defer conn.Close()

	client := agaveproto.NewSftpRelayClient(conn)

	tmpTestDirPath, err := _createTempDirectory("")
	if err != nil {
		assert.FailNowf(t, err.Error(), "Unable to create temp test dir: %s", err.Error())
	}

	_createTempFileInDirectory(tmpTestDirPath, "tmp1", ".txt")
	_createTempFileInDirectory(tmpTestDirPath, "tmp2", ".txt")
	_createTempFileInDirectory(tmpTestDirPath, "tmp3", ".txt")

	tmpNestedTestDirPath, err := _createTempDirectoryInDirectory(tmpTestDirPath, "")
	if err != nil {
		assert.FailNowf(t, err.Error(), "Unable to create temp test subdirectory: %s", err.Error())
	}
	_createTempFileInDirectory(tmpNestedTestDirPath, "tmp3", ".txt")
	_createTempFileInDirectory(tmpNestedTestDirPath, "tmp4", ".txt")

	tmpTestDirInfo, err := os.Stat(_resolveTestPath(tmpTestDirPath, LocalSharedTestDir))
	if err != nil {
		assert.FailNowf(t, err.Error(), "Unable to open temp test file: %s", err.Error())
	}

	err = _updateLocalSharedTestDirOwnership()
	if err != nil {
		assert.FailNowf(t, err.Error(), "Unable to change permission on temp test dir: %s", err.Error())
	}

	tmpRenamedDirPath := tmpTestDirPath + "-renamed"
	renamedRemoteDirPath := _resolveTestPath(tmpRenamedDirPath, SFTP_SHARED_TEST_DIR)

	remoteTestDirPath := _resolveTestPath(tmpTestDirPath, SFTP_SHARED_TEST_DIR)

	req := &agaveproto.SrvRenameRequest{
		SystemConfig: _createRemoteSystemConfig(),
		RemotePath:   remoteTestDirPath,
		NewName: renamedRemoteDirPath,
	}

	grpcResponse, err := client.Rename(context.Background(), req)
	if err != nil {
		assert.Nilf(t, err, "Error while calling RPC Stat: %v", err)
	} else {
		_, err := os.Stat(_resolveTestPath(tmpTestDirPath, LocalSharedTestDir))
		if ! os.IsNotExist(err) {
			assert.FailNowf(t, err.Error(), "Original temp dir should not be present after rename operation: %s", err.Error())
		} else if grpcResponse.RemoteFileInfo == nil {
			assert.FailNow(t, "Returned file info should not be nil on successful rename")
		} else {
			assert.Equal(t, "", grpcResponse.Error, "Rename on existing dir should return empty error")
			if grpcResponse.Error == "" {
				assert.Equal(t, tmpTestDirInfo.Size(), grpcResponse.RemoteFileInfo.Size, "Returned file size should match the test dir size")
				assert.Equal(t, tmpTestDirInfo.Name()+"-renamed", grpcResponse.RemoteFileInfo.Name, "Returned file name should match the name of the test dir")
				assert.Equal(t, renamedRemoteDirPath, grpcResponse.RemoteFileInfo.Path, "Returned file path should match the path of the test dir")
				assert.Equal(t, tmpTestDirInfo.IsDir(), grpcResponse.RemoteFileInfo.IsDirectory, "Returned directory flag should match the flag of the test dir")
				assert.Equal(t, tmpTestDirInfo.Mode().String(), grpcResponse.RemoteFileInfo.Mode, "Returned mode should match the mode of the test dir")
				assert.Equal(t, tmpTestDirInfo.ModTime().Unix(), grpcResponse.RemoteFileInfo.LastUpdated, "Returned last updated timestamp should match the test dir")
			}
		}
	}

	afterTest(t)
}

func TestRenameDirToExistingPthReturnsError(t *testing.T) {
	beforeTest(t)

	conn := _getConnection(t)
	defer conn.Close()

	client := agaveproto.NewSftpRelayClient(conn)

	tmpTestDirPath, err := _createTempDirectory("")
	if err != nil {
		assert.FailNowf(t, err.Error(), "Unable to create temp test file: %v", err)
	}

	forbiddenPath := "/root/" + uuid.New().String()
	_doTestRenamePathReturnsErr(t, client, tmpTestDirPath, forbiddenPath, "permission denied")

	dirExistsOutsideOriginalParentDir := "/tmp"
	_doTestRenamePathReturnsErr(t, client, tmpTestDirPath, dirExistsOutsideOriginalParentDir, "file or dir exists")

	_doTestRenamePathReturnsErr(t, client, tmpTestDirPath, _resolveTestPath(tmpTestDirPath, SFTP_SHARED_TEST_DIR), "file or dir exists")

	_doTestRenamePathReturnsErr(t, client, tmpTestDirPath, "", "file does not exist")

	fileExistsInOriginalParentDir, err := _createTempFile("","")
	if err != nil {
		assert.FailNowf(t, err.Error(), "Unable to create temp test file: %v", err)
	}
	_doTestRenamePathReturnsErr(t, client, tmpTestDirPath, _resolveTestPath(fileExistsInOriginalParentDir, SFTP_SHARED_TEST_DIR), "file or dir exists")

	dirExistsInOriginalParentDir, err := _createTempDirectory("")
	if err != nil {
		assert.FailNowf(t, err.Error(), "Unable to create temp test dir: %v", err)
	}
	_doTestRenamePathReturnsErr(t, client, tmpTestDirPath, _resolveTestPath(dirExistsInOriginalParentDir, SFTP_SHARED_TEST_DIR), "file or dir exists")

	afterTest(t)
}

func TestRenameFileToExistingPathReturnsError(t *testing.T) {
	beforeTest(t)

	conn := _getConnection(t)
	defer conn.Close()

	client := agaveproto.NewSftpRelayClient(conn)

	tmpTestFilePath, err := _createTempFile("","")
	if err != nil {
		assert.FailNowf(t, err.Error(), "Unable to create temp test file: %v", err)
	}

	forbiddenPath := "/root/" + uuid.New().String()
	_doTestRenamePathReturnsErr(t, client, tmpTestFilePath, forbiddenPath, "permission denied")

	dirExistsOutsideOriginalParentDir := "/tmp"
	_doTestRenamePathReturnsErr(t, client, tmpTestFilePath, dirExistsOutsideOriginalParentDir, "file or dir exists")

	_doTestRenamePathReturnsErr(t, client, tmpTestFilePath, _resolveTestPath(tmpTestFilePath, SFTP_SHARED_TEST_DIR), "file or dir exists")

	_doTestRenamePathReturnsErr(t, client, tmpTestFilePath, "", "file does not exist")

	fileExistsInOriginalParentDir, err := _createTempFile("","")
	if err != nil {
		assert.FailNowf(t, err.Error(), "Unable to create temp test file: %v", err)
	}
	_doTestRenamePathReturnsErr(t, client, tmpTestFilePath, _resolveTestPath(fileExistsInOriginalParentDir, SFTP_SHARED_TEST_DIR), "file or dir exists")

	dirExistsInOriginalParentDir, err := _createTempDirectory("")
	if err != nil {
		assert.FailNowf(t, err.Error(), "Unable to create temp test dir: %v", err)
	}
	_doTestRenamePathReturnsErr(t, client, tmpTestFilePath, _resolveTestPath(dirExistsInOriginalParentDir, SFTP_SHARED_TEST_DIR), "file or dir exists")

	afterTest(t)
}

func _doTestRenamePathReturnsErr(t *testing.T, client agaveproto.SftpRelayClient, tmpTestPath string, remoteRenamedPath string, expectedError string) {

	req := &agaveproto.SrvRenameRequest{
		SystemConfig: _createRemoteSystemConfig(),
		RemotePath:   _resolveTestPath(tmpTestPath, SFTP_SHARED_TEST_DIR),
		NewName: remoteRenamedPath,
	}

	grpcResponse, err := client.Rename(context.Background(), req)
	if err != nil {
		assert.Nilf(t, err, "Error while calling RPC Stat: %v", err)
	} else {
		_, err := os.Stat(_resolveTestPath(tmpTestPath, LocalSharedTestDir))
		if err != nil {
			assert.FailNowf(t, err.Error(), "Original temp path should be present after failed rename operation: %s", err.Error())
		} else {
			assert.Nil(t, grpcResponse.RemoteFileInfo, "Returned file info should be nil on error")
			assert.Contains(t, strings.ToLower(grpcResponse.Error), expectedError, "Rename to a forbidden path should return error " + expectedError)
		}
	}
}
