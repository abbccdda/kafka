package cmd

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/confluentinc/ce-kafka/cc-kafka-init/listener"
	"github.com/spf13/cobra"
)

// listenerCmd represents the listener command
var listenerCmd = &cobra.Command{
	Use:   "listener",
	Short: "Wait for Kafka's external listener to become available",
	Long: `Parses a Kafka 'server.properties' file to find the port corresponding to the provided LISTENER. This 
port will be used by a TCP client to validate external network availability.`,
	Run: func(cmd *cobra.Command, args []string) {
		ctx := context.Background()
		ctx, cancel := withInterruptHandling(ctx)
		defer cancel()
		logger := log.New(os.Stdout, "listener: ", 0)
		listenerName, err := cmd.Flags().GetString("listener")
		if err != nil {
			logger.Println("listener not specified")
			os.Exit(1)
		}
		serverPropertiesPath, err := cmd.Flags().GetString("server-properties")
		if err != nil {
			logger.Println("server-properties not specified")
			os.Exit(1)
		}

		internalPort, err := cmd.Flags().GetInt("internal-port")
		if err != nil {
			logger.Println("server-properties not specified")
			os.Exit(1)
		}

		config := listener.Config{
			ServerPropertiesPath: serverPropertiesPath,
			Listener:             listenerName,
			InternalPort:         internalPort,
			ReadTimeout:          30 * time.Second,
		}
		srv := listener.NewTcpNonceRoundTripper(logger, config)
		err = srv.Run(ctx)
		if err != nil {
			logger.Fatalf("error: %v\n", err)
		} else {
			os.Exit(0)
		}
	},
}

func init() {
	rootCmd.AddCommand(listenerCmd)
	listenerCmd.Flags().String("listener", "EXTERNAL", "specify a listener to search for and bind to in server.properties")
	listenerCmd.Flags().String("server-properties", "/mnt/config/kafka.properties", "path to kafka server.properties")
	listenerCmd.Flags().Int("internal-port", 9092, "internal port which Kafka binds to")
}
