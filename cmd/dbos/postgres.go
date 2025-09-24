package main

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"net/url"
	"os"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/spf13/cobra"
)

const (
	containerName = "dbos-db"
	imageName     = "pgvector/pgvector:pg16"
	pgData        = "/var/lib/postgresql/data"
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

func checkDockerInstalled() bool {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return false
	}
	defer cli.Close()

	_, err = cli.Ping(context.Background())
	return err == nil
}

func startDockerPostgres() error {
	logger.Info("Attempting to create a Docker Postgres container...")

	if !checkDockerInstalled() {
		return fmt.Errorf("Docker not detected locally. Please install Docker to use this feature")
	}

	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return fmt.Errorf("failed to create Docker client: %w", err)
	}
	defer cli.Close()

	ctx := context.Background()

	// Check if container already exists
	containers, err := cli.ContainerList(ctx, container.ListOptions{All: true})
	if err != nil {
		return fmt.Errorf("failed to list containers: %w", err)
	}

	for _, c := range containers {
		for _, name := range c.Names {
			if name == "/"+containerName {
				if c.State == "running" {
					logger.Info("Container is already running", "container", containerName)
					return nil
				} else if c.State == "exited" {
					// Start the existing container
					if err := cli.ContainerStart(ctx, c.ID, container.StartOptions{}); err != nil {
						return fmt.Errorf("failed to start existing container: %w", err)
					}
					logger.Info("Container was stopped and has been restarted", "container", containerName)
					return waitForPostgres()
				}
			}
		}
	}

	// Pull image if it doesn't exist
	images, err := cli.ImageList(ctx, image.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list images: %w", err)
	}

	imageExists := false
	for _, img := range images {
		for _, tag := range img.RepoTags {
			if tag == imageName {
				imageExists = true
				break
			}
		}
	}

	if !imageExists {
		logger.Info("Pulling Docker image", "image", imageName)
		reader, err := cli.ImagePull(ctx, imageName, image.PullOptions{})
		if err != nil {
			return fmt.Errorf("failed to pull image: %w", err)
		}
		defer reader.Close()
		io.Copy(io.Discard, reader) // Wait for pull to complete
	}

	// Get password from environment or use default
	password := os.Getenv("PGPASSWORD")
	if password == "" {
		password = "dbos"
	}

	// Create and start container
	config := &container.Config{
		Image: imageName,
		Env: []string{
			fmt.Sprintf("POSTGRES_PASSWORD=%s", password),
			fmt.Sprintf("PGDATA=%s", pgData),
		},
		ExposedPorts: nat.PortSet{
			"5432/tcp": {},
		},
	}

	hostConfig := &container.HostConfig{
		PortBindings: nat.PortMap{
			"5432/tcp": []nat.PortBinding{
				{
					HostIP:   "0.0.0.0",
					HostPort: "5432",
				},
			},
		},
		AutoRemove: true,
	}

	resp, err := cli.ContainerCreate(ctx, config, hostConfig, nil, nil, containerName)
	if err != nil {
		return fmt.Errorf("failed to create container: %w", err)
	}

	if err := cli.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		return fmt.Errorf("failed to start container: %w", err)
	}

	logger.Info("Created container", "id", resp.ID[:12])

	// Wait for PostgreSQL to be ready
	if err := waitForPostgres(); err != nil {
		return err
	}

	logger.Info("Postgres available", "url", fmt.Sprintf("postgres://postgres:%s@localhost:5432", url.QueryEscape(password)))
	return nil
}

func stopDockerPostgres() error {
	logger.Info("Stopping Docker Postgres container", "container", containerName)

	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return fmt.Errorf("failed to create Docker client: %w", err)
	}
	defer cli.Close()

	ctx := context.Background()

	// Find the container
	containers, err := cli.ContainerList(ctx, container.ListOptions{All: true})
	if err != nil {
		return fmt.Errorf("failed to list containers: %w", err)
	}

	for _, c := range containers {
		for _, name := range c.Names {
			if name == "/"+containerName {
				if c.State == "running" {
					// Stop the container
					if err := cli.ContainerStop(ctx, c.ID, container.StopOptions{}); err != nil {
						return fmt.Errorf("failed to stop container: %w", err)
					}
					logger.Info("Successfully stopped Docker Postgres container", "container", containerName)
					return nil
				} else {
					logger.Info("Container exists but is not running", "container", containerName)
					return nil
				}
			}
		}
	}

	logger.Info("Container does not exist", "container", containerName)
	return nil
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
