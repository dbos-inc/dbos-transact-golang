package main

import (
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"
)

// Version will be set at build time using ldflags
var Version = "dev"

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Show the version and exit",
	RunE: func(cmd *cobra.Command, args []string) error {
		if jsonOutput {
			output := map[string]string{"version": Version}
			data, err := json.Marshal(output)
			if err != nil {
				return err
			}
			fmt.Println(string(data))
		} else {
			fmt.Printf("DBOS CLI version: %s\n", Version)
		}
		return nil
	},
}