package sftprelay

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"golang.org/x/crypto/ssh"
)

// Implements the callback function to create a new ssh.Client from the SSHConfig
func NewSSHClientCallback(ctx context.Context, id string) (*ssh.Client, error) {
	protoSystemConfig, err := ProtobufFromJSON(id)
	if err != nil {
		return nil, err
	}
	sshConfig, err := NewSSHConfig(protoSystemConfig)
	if err != nil {
		return nil, err
	}

	log.Debugf("Dialing %v for new ssh client", sshConfig.String())

	addr := fmt.Sprintf("%s:%d", sshConfig.Host, sshConfig.Port)
	cli, err := ssh.Dial("tcp", addr, &ssh.ClientConfig{
		User:            sshConfig.User,
		Auth:            sshConfig.Auth,
		Timeout:         sshConfig.Timeout,
		HostKeyCallback: sshConfig.HostKeyCallback,
	})
	if err != nil {
		return nil, errors.Wrap(err, "pool create client")
	}
	return cli, nil
}

