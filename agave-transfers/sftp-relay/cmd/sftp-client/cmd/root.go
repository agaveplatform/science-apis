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
	"fmt"
	homedir "github.com/mitchellh/go-homedir"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"io"
	"os"
)

var log = logrus.New()

const (
	DefaultUsername = "testuser"
	DefaultHost     = "0.0.0.0"
	DefaultKey      = ""
	DefaultPassword = "testuser"
	DefaultPort     = 10022
	//DefaultSrc      = "/etc/hosts"
	DefaultSrc  = "/tmp/10MB.txt"
	DefaultDest = "/tmp/10MB.txt"
	GrpcService = "[::1]:50051"
)

var (
	cfgFile     string
	username    string
	host        string
	key         string
	passwd      string
	port        int
	src         string
	dest        string
	grpcservice string
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{

	Use:   "sftp-client",
	Short: "CLI interface to the sftp-server",
	Long:  `CLI client written in Go for the Agave sftp-server application.`,
	// Uncomment the following line if your bare application
	// has an action associated with it:
	Run: func(cmd *cobra.Command, args []string) {},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)

	// log to console and file
	f, err := os.OpenFile("SFTPClient.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	wrt := io.MultiWriter(os.Stdout, f)
	log.SetOutput(wrt)

	// Here you will define your flags and configuration settings.
	// Cobra supports persistent flags, which, if defined here,
	// will be global for your application.
	pflags := rootCmd.PersistentFlags()

	pflags.StringVar(&cfgFile, "config", "", "Location of configuration file, if wanted instead of flags. (default is $HOME/.sftp-client.yaml)")
	pflags.StringVarP(&username, "username", "u", DefaultUsername, "Username to use to connect to the remote host. (default is "+DefaultUsername+")")
	pflags.StringVarP(&passwd, "passwd", "p", DefaultPassword, "Password to use to connect to the remote host.")
	pflags.StringVarP(&key, "key", "i", "", "Private key to use to connect to the remote host.")
	pflags.StringVarP(&host, "host", "H", DefaultHost, "Hostname or ip of the remote host.")
	pflags.IntVarP(&port, "port", "P", DefaultPort, "Port of the remote host.")
	pflags.StringVarP(&grpcservice, "grpcservice", "g", GrpcService, "Address of the grpc server. (default is [::1]:50051")

	pflags.StringVarP(&dest, "dest", "d", DefaultDest, "Dest file.  Defaults to /tmp/100K.txt")
	pflags.StringVarP(&src, "src", "s", DefaultSrc, "Source file.  Defaults to /tmp/100K.txt")

	viper.BindPFlag("username", pflags.Lookup("username"))
	viper.BindPFlag("password", pflags.Lookup("password"))
	viper.BindPFlag("key", pflags.Lookup("key"))
	viper.BindPFlag("host", pflags.Lookup("host"))
	viper.BindPFlag("port", pflags.Lookup("port"))
	viper.BindPFlag("grpcservice", pflags.Lookup("grpcservice"))
	// Cobra also supports local flags, which will only run
	// when this action is called directly.
	//rootCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := homedir.Dir()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		// Search config in home directory with name ".client" (without extension).
		viper.AddConfigPath(home)
		viper.SetConfigName(".sftp-client")
	}

	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	}
}
