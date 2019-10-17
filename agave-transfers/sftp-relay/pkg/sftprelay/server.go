package sftprelay

import (
	"bufio"
	"context"
	"fmt"
	sftppb "github.com/agaveplatform/science-apis/agave-transfers/sftp-relay/pkg/sftpproto"
	"github.com/pkg/sftp"
	"github.com/sirupsen/logrus"
	"golang.org/x/crypto/ssh"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

var log = logrus.New()

type Server struct{}

func init() {
	// log to console and file
	f, err := os.OpenFile("SFTPServer.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	wrt := io.MultiWriter(os.Stdout, f)

	log.SetOutput(wrt)
	log.Info("Set up loggin for server")
}

func (*Server) CopyLocalToRemoteService(ctx context.Context, req *sftppb.CopyLocalToRemoteRequest) (*sftppb.CopyLocalToRemoteResponse, error) {
	log.Info("CopyLocalToRemoteService function was invoked with %v\n", req)
	fileName := req.Sftp.FileName
	passWord := req.Sftp.PassWord
	systemId := req.Sftp.SystemId
	username := req.Sftp.Username
	//hostKey := req.Sftp.HostKey
	hostPort := req.Sftp.HostPort

	if username == "" {
		log.Println("username is null")
	}
	if passWord == "" {
		log.Println("pwd is null")
	}
	// get host public key
	//HostKey := getHostKey(systemId)
	config := ssh.ClientConfig{
		User: username,
		Auth: []ssh.AuthMethod{
			ssh.Password(passWord),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		//HostKeyCallback: ssh.FixedHostKey(HostKey),
	}

	result := ""
	// connect
	conn, err := ssh.Dial("tcp", systemId+hostPort, &config)
	if err != nil {
		log.Info("Error Dialing the server: %f", err)
		log.Error(err)
		result = err.Error()
	}
	defer conn.Close()

	// create new SFTP client
	client, err := sftp.NewClient(conn)
	if err != nil {
		log.Info("Error creating new client: %f", err)
		result = err.Error()
	}
	defer client.Close()

	// create destination file
	dstFile, err := client.Create(fileName)
	if err != nil {
		log.Info("Error creating destFile: %f", err)
		result = err.Error()
	}
	defer dstFile.Close()

	// open source file
	srcFile, err := os.Open(fileName)
	if err != nil {
		log.Info("Error opening source file: %f", err)
		result = err.Error()
	}

	// copy with the WriteTo function
	bytesWritten, err := dstFile.ReadFrom(srcFile)
	if err != nil {
		log.Println(err)
	}
	log.Info("%d bytes copied\n", bytesWritten)

	// copy source file to destination file
	//bytes, err := io.Copy(dstFile, srcFile)
	//if err != nil {
	//	log.Println(err)
	//	result = err.Error()
	//}

	log.Info("%d bytes copied\n", bytesWritten)
	log.Info(result)
	res := &sftppb.CopyLocalToRemoteResponse{
		Result: strconv.FormatInt(bytesWritten, 10),
	}
	log.Info(res.String())
	return res, nil
}

func (*Server) CopyFromRemoteService(ctx context.Context, req *sftppb.CopyFromRemoteRequest) (*sftppb.CopyFromRemoteResponse, error) {
	log.Info("CopyFromRemoteService function was invoked with %v\n", req)
	fileName := req.Sftp.FileName
	passWord := req.Sftp.PassWord
	systemId := req.Sftp.SystemId
	username := req.Sftp.Username
	//hostKey := req.Sftp.HostKey
	hostPort := req.Sftp.HostPort

	if username == "" {
		log.Println("username is null")
	}
	if passWord == "" {
		log.Println("pwd is null")
	}
	// get host public key
	//HostKey := getHostKey(systemId)

	config := ssh.ClientConfig{
		User: username,
		Auth: []ssh.AuthMethod{
			ssh.Password(passWord),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		//HostKeyCallback: ssh.FixedHostKey(HostKey),
	}

	result := ""
	// connect
	conn, err := ssh.Dial("tcp", systemId+hostPort, &config)
	if err != nil {
		log.Info("Error dialing system: ", err)
		result = err.Error()
	}
	defer conn.Close()

	// create new SFTP client
	client, err := sftp.NewClient(conn)
	if err != nil {
		log.Info("Error creating new client", err)
		result = err.Error()
	}
	defer client.Close()

	// create destination file
	dstFile, err := os.Create(fileName)
	if err != nil {
		log.Info("Error creating dest file", err)
		result = err.Error()
	}
	defer dstFile.Close()

	// open source file
	srcFile, err := client.Open(fileName)
	if err != nil {
		log.Info("Opening source file: ", err)
		result = err.Error()
	}

	// copy with the WriteTo function
	bytesWritten, err := srcFile.WriteTo(dstFile)
	if err != nil {
		log.Info("Error writing to file: ", err)
	}
	// copy source file to destination file
	//bytes, err := io.Copy(dstFile, srcFile)
	//if err != nil {
	//	log.Println(err)
	//	result = err.Error()
	//}
	log.Info("%d bytes copied\n", bytesWritten)
	log.Info(result)
	res := &sftppb.CopyFromRemoteResponse{
		Result: strconv.FormatInt(bytesWritten, 10),
	}
	log.Println(res.String())
	return res, nil
}

//func (*Server) GetDirListing(ctx context.Context, req *sftppb.GetDirRequest) (*sftppb.GetDirResponse, error) {
//	log.Printf("CopyFromRemoteService function was invoked with %v\n", req)
//	fileName := req.List.FileName
//	passWord := req.List.PassWord
//	systemId := req.List.SystemId
//	username := req.List.UserName
//	//hostKey := req.Sftp.HostKey
//	hostPort := req.List.HostPort
//
//	if username == "" {
//		log.Println("username is null")
//	}
//	if passWord == "" {
//		log.Println("pwd is null")
//	}
//	// get host public key
//	//HostKey := getHostKey(systemId)
//
//	config := ssh.ClientConfig{
//		User: username,
//		Auth: []ssh.AuthMethod{
//			ssh.Password(passWord),
//		},
//		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
//		//HostKeyCallback: ssh.FixedHostKey(HostKey),
//	}
//
//	result := ""
//	// connect
//	conn, err := ssh.Dial("tcp", systemId+hostPort, &config)
//	if err != nil {
//		log.Printf("Error dialing system: ", err)
//		result = err.Error()
//	}
//	defer conn.Close()
//
//	// create new SFTP client
//	client, err := sftp.NewClient(conn)
//	if err != nil {
//		log.Printf("Error creating new client", err)
//		result = err.Error()
//	}
//	defer client.Close()
//
//
//	// open source file
//	fileList, err := client.ReadDir(fileName);
//	if err != nil {
//		log.Printf("Error reading directory contents: ", err)
//		result = err.Error()
//	}
//	fileList
//	log.Printf()
//	log.Printf("%d bytes copied\n", bytesWritten)
//	log.Println(result)
//	res := &sftppb.CopyFromRemoteResponse{
//		Result: strconv.FormatInt(bytesWritten, 10),
//	}
//	log.Println(res.String())
//	return res, nil
//}

func getHostKey(host string) ssh.PublicKey {
	// parse OpenSSH known_hosts file
	// ssh or use ssh-keyscan to get initial key
	fmt.Println(host)
	file, err := os.Open(filepath.Join(os.Getenv("HOME"), ".ssh", "known_hosts"))
	if err != nil {
		log.Println(err)
	}
	fmt.Println(file)
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var hostKey ssh.PublicKey
	fmt.Println(hostKey)
	for scanner.Scan() {
		fields := strings.Split(scanner.Text(), " ")
		fmt.Println(fields)
		if len(fields) != 3 {
			continue
		}
		if strings.Contains(fields[0], host) {

			var err error
			hostKey, _, _, _, err = ssh.ParseAuthorizedKey(scanner.Bytes())
			if err != nil {
				log.Printf("error parsing %q: %v", fields[2], err)
			}
			break
		}
	}

	if hostKey == nil {
		log.Println("no hostkey found for " + host)
	}

	return hostKey
}
