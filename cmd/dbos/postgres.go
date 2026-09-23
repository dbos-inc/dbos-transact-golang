package main

import (
	"bytes"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/spf13/cobra"
)

const (
	containerName        = "dbos-db"
	imageName            = "pgvector/pgvector:pg16"
	pgData               = "/var/lib/postgresql/data"
	hostPgDataVolumeName = "pgdata"
)

var postgresCmd = &cobra.Command{
	Use:   "postgres",
	Short: "Manage local Postgres database with Docker",
}

var postgresStartCmd = &cobra.Command{
	Use:   "start",
	Short: "Start a local Postgres database",
	RunE:  runPostgresStart,
}

var postgresStopCmd = &cobra.Command{
	Use:   "stop",
	Short: "Stop the local Postgres database",
	RunE:  runPostgresStop,
}

func init() {
	postgresCmd.AddCommand(postgresStartCmd)
	postgresCmd.AddCommand(postgresStopCmd)
}

func runPostgresStart(cmd *cobra.Command, args []string) error {
	return startDockerPostgres()
}

func runPostgresStop(cmd *cobra.Command, args []string) error {
	return stopDockerPostgres()
}

func runDocker(args ...string) (string, error) {
	var stdout, stderr bytes.Buffer
	cmd := exec.Command("docker", args...)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg == "" {
			return "", fmt.Errorf("docker %s: %w", args[0], err)
		}
		return "", fmt.Errorf("docker %s: %w: %s", args[0], err, msg)
	}
	return strings.TrimSpace(stdout.String()), nil
}

func checkDockerInstalled() bool {
	_, err := runDocker("version", "--format", "{{.Server.Version}}")
	return err == nil
}

// containerState returns "" if the container does not exist.
func containerState() (string, error) {
	out, err := runDocker("container", "ls", "-a", "--filter", "name=^/"+containerName+"$", "--format", "{{.State}}")
	if err != nil {
		return "", err
	}
	return out, nil
}

func startDockerPostgres() error {
	logger.Info("Attempting to create a Docker Postgres container...")

	if !checkDockerInstalled() {
		return fmt.Errorf("Docker not detected locally. Please install Docker to use this feature")
	}

	state, err := containerState()
	if err != nil {
		return fmt.Errorf("failed to list containers: %w", err)
	}

	switch state {
	case "":
	case "running":
		logger.Info("Container is already running", "container", containerName)
		return nil
	case "exited", "created":
		// With --rm the daemon may have already begun removal. Fall through to
		// the create path after waiting for the removal to finish.
		_, err := runDocker("start", containerName)
		if err == nil {
			logger.Info("Container was stopped and has been restarted", "container", containerName)
			return waitForPostgres()
		}
		if !isMarkedForRemovalErr(err) {
			return fmt.Errorf("failed to start existing container: %w", err)
		}
		logger.Info("Existing container is being removed; waiting before recreating", "container", containerName)
		if err := waitForContainerRemoved(); err != nil {
			return fmt.Errorf("failed waiting for container removal: %w", err)
		}
	case "removing", "dead":
		logger.Info("Existing container is being removed; waiting before recreating", "container", containerName, "state", state)
		if err := waitForContainerRemoved(); err != nil {
			return fmt.Errorf("failed waiting for container removal: %w", err)
		}
	default:
		return fmt.Errorf("container %s is in unexpected state %q", containerName, state)
	}

	if _, err := runDocker("image", "inspect", imageName); err != nil {
		logger.Info("Pulling Docker image", "image", imageName)
		if _, err := runDocker("pull", imageName); err != nil {
			return fmt.Errorf("failed to pull image: %w", err)
		}
	}

	password := os.Getenv("PGPASSWORD")
	if password == "" {
		password = "dbos"
	}

	if _, err := runDocker("volume", "create", hostPgDataVolumeName); err != nil {
		return fmt.Errorf("failed to create volume %s for Postgres: %w", hostPgDataVolumeName, err)
	}

	id, err := runDocker("run", "-d", "--rm",
		"--name", containerName,
		"-e", "POSTGRES_PASSWORD="+password,
		"-e", "PGDATA="+pgData,
		"-p", "0.0.0.0:5432:5432",
		"-v", hostPgDataVolumeName+":"+pgData,
		imageName,
	)
	if err != nil {
		return fmt.Errorf("failed to create container: %w", err)
	}

	if len(id) > 12 {
		id = id[:12]
	}
	logger.Info("Created container", "id", id)

	if err := waitForPostgres(); err != nil {
		return err
	}

	logger.Info("Postgres available", "url", fmt.Sprintf("postgres://postgres:%s@localhost:5432", url.QueryEscape(password)))
	return nil
}

func stopDockerPostgres() error {
	logger.Info("Stopping Docker Postgres container", "container", containerName)

	state, err := containerState()
	if err != nil {
		return fmt.Errorf("failed to list containers: %w", err)
	}

	switch state {
	case "":
		logger.Info("Container does not exist", "container", containerName)
		return nil
	case "running":
		if _, err := runDocker("stop", containerName); err != nil {
			return fmt.Errorf("failed to stop container: %w", err)
		}
		// With --rm, wait for the daemon to finish removing the container so
		// that a subsequent start sees a clean slate.
		autoRemove, err := runDocker("inspect", "--format", "{{.HostConfig.AutoRemove}}", containerName)
		if err == nil && autoRemove == "true" {
			if err := waitForContainerRemoved(); err != nil {
				return fmt.Errorf("failed waiting for container removal: %w", err)
			}
		}
		logger.Info("Successfully stopped Docker Postgres container", "container", containerName)
		return nil
	case "removing", "dead":
		if err := waitForContainerRemoved(); err != nil {
			return fmt.Errorf("failed waiting for container removal: %w", err)
		}
	}
	logger.Info("Container exists but is not running", "container", containerName)
	return nil
}

func waitForContainerRemoved() error {
	deadline := time.Now().Add(30 * time.Second)
	for {
		state, err := containerState()
		if err != nil {
			return err
		}
		if state == "" {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("container %s still present (state %q) after 30s", containerName, state)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func isMarkedForRemovalErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), "marked for removal")
}

func waitForPostgres() error {
	logger.Info("Waiting for Postgres Docker container to start...")

	password := os.Getenv("PGPASSWORD")
	if password == "" {
		password = "dbos"
	}

	connStr := fmt.Sprintf("postgres://postgres:%s@localhost:5432/postgres?connect_timeout=2&sslmode=disable", url.QueryEscape(password))

	// Try for up to 30 seconds
	for i := 0; i < 30; i++ {
		if i%5 == 0 && i > 0 {
			logger.Info("Still waiting for Postgres Docker container to start...")
		}

		db, err := sql.Open("pgx", connStr)
		if err == nil {
			err = db.Ping()
			db.Close()
			if err == nil {
				return nil
			}
		}

		time.Sleep(time.Second)
	}

	return fmt.Errorf("failed to start Docker container: Container %s did not start in time", containerName)
}
