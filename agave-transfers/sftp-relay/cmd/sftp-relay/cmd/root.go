package cmd

import (
	"entrogo.com/sshpool/pkg/clientpool"
	"fmt"
	agaveproto "github.com/agaveplatform/science-apis/agave-transfers/sftp-relay/pkg/sftpproto"
	"github.com/agaveplatform/science-apis/agave-transfers/sftp-relay/pkg/sftprelay"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/mitchellh/go-homedir"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	hpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"time"
)

var (
	cfgFile     string
	listen      string
	verbose     bool
	metricsPort int
	poolSize    int
	idleTimeout int

	metricsRegistry = prometheus.NewRegistry()           // prometheus metrics registry
	grpcMetrics     = grpc_prometheus.NewServerMetrics() // grpc metrics server

)

var rootCmd = &cobra.Command{
	Use:   "sftp-relay",
	Short: "A grpc sftp proxy server",
	Long: `This is a grpc microservice providing basic managed SFTP operations. It provides connection 
pooling and a convenient protobuf wrapper for interacting with one or more remote systems.`,
	Run: func(cmd *cobra.Command, args []string) {

		//// log to console and file
		//f, err := os.OpenFile("sftprelay.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		//if err != nil {
		//	log.Fatalf("Error opening file: %v", err)
		//}
		//wrt := io.MultiWriter(os.Stdout, f)
		//log.SetOutput(wrt)

		// set up the protobuf server
		lis, err := net.Listen("tcp", listen)
		if err != nil {
			log.Fatalf("Failed to listen: %v", err)
		}
		defer lis.Close()

		log.Println("Initializing grpc server")
		grpcServer := grpc.NewServer(
			grpc.StreamInterceptor(grpcMetrics.StreamServerInterceptor()),
			grpc.UnaryInterceptor(grpcMetrics.UnaryServerInterceptor()),
			grpc.KeepaliveParams(keepalive.ServerParameters{
				MaxConnectionIdle: 20 * time.Minute,
				Time:              (time.Duration(10) * time.Second),
				Timeout:           (time.Duration(10) * time.Second),
			}))

		cp := clientpool.New(clientpool.WithPoolSize(poolSize), clientpool.WithExpireAfter(time.Duration(idleTimeout)*time.Second))
		defer cp.Close()

		// Init a new api server to register with the grpc server
		server := sftprelay.Server{
			Registry:    *metricsRegistry,
			GrpcMetrics: *grpcMetrics,
			Pool:        cp,
		}
		// set up prometheus metrics
		server.InitMetrics()

		agaveproto.RegisterSftpRelayServer(grpcServer, &server)
		hpb.RegisterHealthServer(grpcServer, health.NewServer())

		// Create a HTTP server for prometheus.
		httpServer := &http.Server{
			Handler: promhttp.HandlerFor(metricsRegistry, promhttp.HandlerOpts{}),
			Addr:    fmt.Sprintf("0.0.0.0:%d", metricsPort),
		}

		// start the http server in a goroutine
		go func() {
			log.Printf("sftp-relay metrics listening -> 0.0.0.0:%d", metricsPort)
			if err := httpServer.ListenAndServe(); err != nil {
				log.Fatal("Unable to start a http server.")
			}
		}()

		// start the grpc server
		log.Printf("sftp-relay grpc listening -> %s", listen)

		if err = grpcServer.Serve(lis); err != nil {
			log.Fatalf("Ca c'est le temps: %v", err)
		}

		//// grpcServer is a *grpc.Server
		//service.RegisterChannelzServiceToServer(grpcServer)
	},
}

func Main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

// Execute adds all child commands to the root command and sets flags.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		log.Fatalf("Error executing root command: %v", err)
	}
}

func init() {
	cobra.OnInitialize(initConfig)

	// log to console and file
	f, err := os.OpenFile("sftp-relay.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("Error opening file: %v", err)
	}
	wrt := io.MultiWriter(os.Stdout, f)
	log.SetOutput(wrt)

	// Here you will define your flags and configuration settings.
	// Cobra supports persistent flags, which, if defined here,
	// will be global for your application.
	pflags := rootCmd.PersistentFlags()

	pflags.StringVar(&cfgFile, "config", "", "Location of configuration file, if wanted instead of flags. (default is $HOME/.sftp-client.yaml)")
	pflags.StringVar(&listen, "listen", ":50051", "Address on which to listen for gRPC requests.")
	pflags.BoolVarP(&verbose, "verbose", "V", false, "Verbose logging.")
	pflags.IntVar(&metricsPort, "metrics_port", 9092, "Port for Prometheus metrics service")
	pflags.IntVarP(&poolSize, "pool_size", "s", 10, "Maximum pool size")
	pflags.IntVarP(&idleTimeout, "idle_timeout", "i", 300, "Amount of time, in seconds, that an idle connection will be kept around before reaping.")

	viper.BindPFlag("listen", pflags.Lookup("listen"))
	viper.BindPFlag("verbose", pflags.Lookup("verbose"))
	viper.BindPFlag("metricsPort", pflags.Lookup("metricsPort"))
	viper.BindPFlag("poolSize", pflags.Lookup("poolSize"))
	viper.BindPFlag("idleTimeout", pflags.Lookup("idleTimeout"))
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

		// Search config in home directory with name ".sftp-relay" (without extension).
		viper.AddConfigPath(home)
		viper.SetConfigName(".sftp-relay")
	}

	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	}
}
