package main

import (
	"github.com/spf13/cobra"
)

// Version will be set at build time using ldflags
var Version = "dev"

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Show the version and exit",
	RunE: func(cmd *cobra.Command, args []string) error {
		if jsonOutput {
			return outputJSON(map[string]string{"version": Version})
		} else {
			logger.Info("DBOS CLI version", "version", Version)
		}
		return nil
	},
}