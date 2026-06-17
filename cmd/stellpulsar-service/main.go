package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/stellhub/stellpulsar-service/internal/limiter"
	"github.com/stellhub/stellpulsar-service/internal/server"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	httpServer := &http.Server{
		Addr:              getenv("STELLPULSAR_HTTP_ADDR", ":8080"),
		Handler:           server.NewHandler(limiter.NewMemoryLimiter(time.Now)),
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		logger.Info("starting StellPulsar service", "address", httpServer.Addr)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("StellPulsar service stopped unexpectedly", "error", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("failed to stop StellPulsar service", "error", err)
		os.Exit(1)
	}
	logger.Info("StellPulsar service stopped")
}

func getenv(key string, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}
