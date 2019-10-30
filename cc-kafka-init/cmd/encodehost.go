package cmd

import (
	"fmt"
	"github.com/confluentinc/ce-kafka/cc-kafka-init/encodehost"
	"github.com/spf13/cobra"
	"log"
	"os"
)

const HostIpEnvVarName string = "HOST_IP"

// This logic is intended to be moved to our common init container, once it's ready
// https://confluentinc.atlassian.net/wiki/spaces/~roger/pages/937957745/Init+Container+Plan
// https://github.com/confluentinc/confluent-platform-images/tree/master/components/init-container
var encodeHostCmd = &cobra.Command{
	Use:   "encode-host",
	Short: "Encode host IP",
	Long:  `Encodes host IP for use in direct endpoint addresses. Requires HOST_IP env var`,
	Run: func(cmd *cobra.Command, args []string) {
		logger := log.New(os.Stderr, "encoder: ", 0)
		hostIp, ok := os.LookupEnv(HostIpEnvVarName)
		if !ok {
			logger.Println(fmt.Sprintf("Env var \"%s\" is not set", HostIpEnvVarName))
			os.Exit(1)
		}
		encoded, err := encodehost.Encode(hostIp)
		if err != nil {
			logger.Println(err)
			os.Exit(1)
		}
		fmt.Println(encoded)
	},
}

func init() {
	rootCmd.AddCommand(encodeHostCmd)
}
