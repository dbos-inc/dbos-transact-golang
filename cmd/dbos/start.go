package main

import (
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/spf13/cobra"
)

var startCmd = &cobra.Command{
	Use:   "start",
	Short: "Start your DBOS application using the start commands in 'dbos-config.yaml'",
	RunE:  runStart,
}

func runStart(cmd *cobra.Command, args []string) error {
	// Check if config is loaded
	if config == nil {
		return fmt.Errorf("no config provided")
	}

	if len(config.RuntimeConfig.Start) == 0 {
		return fmt.Errorf("no start commands found in config file")
	}

	logger.Info("Executing start commands from config file")

	for _, command := range config.RuntimeConfig.Start {
		logger.Info("Executing command", "command", command)

		// Create the command
		var process *exec.Cmd
		if runtime.GOOS == "windows" {
			process = exec.Command("cmd", "/C", command)
		} else {
			process = exec.Command("sh", "-c", command)
		}

		// Set up the command
		process.Stdout = os.Stdout
		process.Stderr = os.Stderr
		process.Stdin = os.Stdin

		// On Unix-like systems, set process group
		if runtime.GOOS != "windows" {
			process.SysProcAttr = &syscall.SysProcAttr{
				Setpgid: true,
			}
		}

		// Set up signal handling
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

		// Start the process
		if err := process.Start(); err != nil {
			return fmt.Errorf("failed to start command: %w", err)
		}

		// Wait for the process or signal
		done := make(chan error, 1)
		go func() {
			done <- process.Wait()
		}()

		select {
		case err := <-done:
			if err != nil {
				return fmt.Errorf("command failed: %w", err)
			}
		case sig := <-sigChan:
			logger.Info("Received signal, stopping...", "signal", sig.String())

			// Kill the process group on Unix-like systems
			if runtime.GOOS != "windows" {
				syscall.Kill(-process.Process.Pid, syscall.SIGTERM)
			} else {
				process.Process.Kill()
			}

			// Wait a bit for graceful shutdown
			select {
			case <-done:
			case <-sigChan:
				// Force kill if we get another signal
				if runtime.GOOS != "windows" {
					syscall.Kill(-process.Process.Pid, syscall.SIGKILL)
				}
			case <-time.After(10 * time.Second):
				// Force kill after timeout
				if runtime.GOOS != "windows" {
					syscall.Kill(-process.Process.Pid, syscall.SIGKILL)
				}
			}

			os.Exit(0)
		}
	}

	return nil
}
