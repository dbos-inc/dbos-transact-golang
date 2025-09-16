package main

import (
	"log/slog"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	rootCmd = &cobra.Command{
		Use:          "dbos",
		Short:        "DBOS CLI",
		Long:         `DBOS CLI is a command-line interface for managing DBOS workflows`,
		SilenceUsage: true,
	}

	// Global flags
	dbURL      string
	configFile string
	verbose    bool

	// Global config
	config *Config
	logger *slog.Logger
)

func init() {
	cobra.OnInitialize(initConfig)

	// Global flags available to all commands
	rootCmd.PersistentFlags().StringVarP(&dbURL, "db-url", "D", "", "Your DBOS system database URL")
	rootCmd.PersistentFlags().StringVar(&configFile, "config", "", "Config file (default is dbos-config.yaml)")
	rootCmd.PersistentFlags().BoolVar(&verbose, "verbose", false, "Enable verbose mode (DEBUG level logging)")

	// Add all subcommands
	rootCmd.AddCommand(versionCmd)
	rootCmd.AddCommand(startCmd)
	rootCmd.AddCommand(migrateCmd)
	rootCmd.AddCommand(resetCmd)
	rootCmd.AddCommand(initCmd)
	rootCmd.AddCommand(postgresCmd)
	rootCmd.AddCommand(workflowCmd)
}

func initConfig() {
	// Initialize global logger
	logger = initLogger(slog.LevelInfo)

	if configFile != "" {
		viper.SetConfigFile(configFile)
	} else {
		viper.SetConfigName("dbos-config")
		viper.SetConfigType("yaml")
		viper.AddConfigPath(".")
	}

	// If a config file is found, read it in and parse it
	if err := viper.ReadInConfig(); err == nil {
		// Expand environment variables in all string values
		expandEnvVarsInConfig()

		var cfg Config
		if err := viper.Unmarshal(&cfg); err == nil {
			config = &cfg
		}
	}
}

func initLogger(logLevel slog.Level) *slog.Logger {
	if verbose {
		logLevel = slog.LevelDebug
	}
	return slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		Level: logLevel,
	}))
}

// expandEnvVarsInConfig recursively expands environment variables in all string values
func expandEnvVarsInConfig() {
	for _, key := range viper.AllKeys() {
		value := viper.Get(key)
		if strValue, ok := value.(string); ok {
			expandedValue := os.ExpandEnv(strValue)
			viper.Set(key, expandedValue)
		}
	}
}
