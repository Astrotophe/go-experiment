/*
* Copyright (c) 2017-2020. Canal+ Group
* All rights reserved
 */
package cmd

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/mitchellh/go-homedir"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"os"
	"os/signal"
	"rocket-storemanager/internal/kafka"
	"syscall"
)

var cfgFile string

// Sarama configuration options
var (
	brokers  = "localhost:9092"
	version  = "2.1.1"
	group    = "rocket-storemanager"
	topics   = "asset-events"
	assignor = "range"
	oldest   = true
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "rocket-storemanager",
	Short: "Rocket Store Manager executes filesystem commands and stornext commands",
	Long: `A longer description that spans multiple lines and likely contains
examples and usage of using your application. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,

	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := context.WithCancel(context.Background())
		readChan := make(chan string, 30)
		defer close(readChan)
		doneChan := make(chan struct{})
		defer close(doneChan)

		consumer := kafka.NewConsumer(kafkaConfig(), []string{brokers}, readChan)

		for i := 0; i < 3; i++ {
			go consumer.Consume(ctx, group, []string{topics}, doneChan)
		}

		// Listen system signal to stop goroutines by context
		sigc := make(chan os.Signal, 1)
		signal.Notify(sigc,
			syscall.SIGHUP,
			syscall.SIGINT,
			syscall.SIGTERM,
			syscall.SIGQUIT,
		)
		defer signal.Stop(sigc)
		select {
		case <-sigc:
			log.Info("Signal caught canceling commands contexts")
			cancel()
			for i := 0; i < 3; i++ {
				log.Print("ok")
				<-doneChan
			}
			return
		}
	},
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

	// Here you will define your flags and configuration settings.
	// Cobra supports persistent flags, which, if defined here,
	// will be global for your application.

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.rocket-storemanager.yaml)")

	// Cobra also supports local flags, which will only run
	// when this action is called directly.
	rootCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if cfgFile != "" {
		log.Debug("Loading configuration file from flag")
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := homedir.Dir()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		// Search config in home directory with name ".rocket-storemanager" (without extension).
		viper.AddConfigPath(home)
		viper.SetConfigName(".rocket-storemanager")
	}

	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	}
}

func kafkaConfig() *sarama.Config {
	sarama.Logger = log.New()

	version, err := sarama.ParseKafkaVersion(version)
	if err != nil {
		log.Panicf("Error parsing Kafka version: %v", err)
	}

	config := sarama.NewConfig()
	config.Version = version

	switch assignor {
	case "sticky":
		config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategySticky
	case "roundrobin":
		config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	case "range":
		config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange
	default:
		log.Panicf("Unrecognized consumer group partition assignor: %s", assignor)
	}

	if oldest {
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	}

	return config
}
