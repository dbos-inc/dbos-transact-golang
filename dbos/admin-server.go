package dbos

import (
	"context"
	"fmt"
	"net/http"
	"time"
)

type AdminServer struct {
	server *http.Server
}

func NewAdminServer(port int) *AdminServer {
	mux := http.NewServeMux()

	// Health endpoint
	mux.HandleFunc("/dbos-healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"healthy"}`))
	})

	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}

	return &AdminServer{
		server: server,
	}
}

func (as *AdminServer) Start() error {
	getLogger().Info("Starting admin server", "port", 3001)

	go func() {
		if err := as.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			getLogger().Error("Admin server error", "error", err)
		}
	}()

	return nil
}

func (as *AdminServer) Shutdown() error {
	getLogger().Info("Shutting down admin server")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := as.server.Shutdown(ctx); err != nil {
		getLogger().Error("Admin server shutdown error", "error", err)
		return fmt.Errorf("failed to shutdown admin server: %w", err)
	}

	getLogger().Info("Admin server shutdown complete")
	return nil
}
