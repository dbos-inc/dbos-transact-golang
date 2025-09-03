package main

import (
	"runtime/debug"

	"github.com/spf13/cobra"
)

var (
	Version = "dev" // overridden by -ldflags in CI releases
	Commit  = ""
	BuiltAt = ""
)

func init() {
	if info, ok := debug.ReadBuildInfo(); ok {
		// If built via `go install module/cmd@vX.Y.Z`, use the module version.
		if Version == "dev" && info.Main.Version != "" && info.Main.Version != "(devel)" {
			Version = info.Main.Version
		}
		for _, s := range info.Settings {
			switch s.Key {
			case "vcs.revision":
				if Commit == "" {
					if len(s.Value) >= 7 {
						Commit = s.Value[:7]
					} else {
						Commit = s.Value
					}
				}
			case "vcs.time":
				if BuiltAt == "" {
					BuiltAt = s.Value
				}
			case "vcs.modified":
				if s.Value == "true" && Commit != "" {
					Commit += "-dirty"
				}
			}
		}
	}
}

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Show the version and exit",
	RunE: func(cmd *cobra.Command, args []string) error {
		if jsonOutput {
			return outputJSON(map[string]string{"version": Version})
		}
		logger.Info("DBOS CLI version", "version", Version, "commit", Commit, "built", BuiltAt)
		return nil
	},
}
