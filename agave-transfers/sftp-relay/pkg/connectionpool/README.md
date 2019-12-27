# ssh_sftp_connection_pool
ssh sftp connection pool

This is a fork of 0xef53/go-sshpool

The documentation is located at https://godoc.org/github.com/0xef53/go-sshpool

The fork was done to open the client variable in the SSHConn struct.  The change was made from "client" to "Client".



go-sshpool
GoDoc

Package sshpool provides an SSH connection pool implementation for the Go language.

Installation
go get github.com/0xef53/go-sshpool
Example
agentSocket, ok := os.LookupEnv("SSH_AUTH_SOCK")
if !ok {
	log.Fatalln("Could not connect to SSH_AUTH_SOCK. Is ssh-agent running?")
}

poolCfg := &sshpool.PoolConfig{
	GCInterval: 5 * time.Second,
	MaxConns:   5,
}

p := sshpool.NewPool(poolCfg)

sshCfg := &sshpool.SSHConfig{
	User:        "root",
	Host:        "localhost",
	Port:        22,
	AgentSocket: agentSocket,
	Timeout:     30 * time.Second,
	HostKeyCallback: ssh.InsecureIgnoreHostKey(),
}

output, err := p.CombinedOutput(sshCfg, "uname -a ; sleep 3", nil, nil)
if err != nil {
	log.Fatalf("%s: %s\n", err, output)
}

fmt.Println(string(output))

fmt.Println("Active connections:", p.ActiveConns())
Documentation
Use Godoc documentation for reference and usage.
