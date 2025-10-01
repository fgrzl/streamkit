package main

import (
	"log/slog"
	"net/http"
	"os"

	"github.com/fgrzl/mux"
	"github.com/fgrzl/streamkit/pkg/node"
	"github.com/fgrzl/streamkit/pkg/storage/azurekit"
	"github.com/fgrzl/streamkit/pkg/transport/wskit"
)

func main() {
	addr := getEnv("LISTEN_ADDR", ":9444")
	endpoint := mustEnv("AZURE_TABLES_ENDPOINT")

	// Build Azure Tables StoreFactory using DefaultAzureCredential only.
	factory, err := azurekit.NewStoreFactory(&azurekit.AzureStoreOptions{
		Prefix:                    "streams-",
		Endpoint:                  endpoint,
		UseDefaultAzureCredential: true,
	})
	if err != nil {
		slog.Error("failed to initialize azure store factory", slog.String("err", err.Error()))
		os.Exit(1)
	}

	// Wire NodeManager with the Azure factory
	nm := node.NewNodeManager(node.WithStoreFactory(factory))

	// Router and endpoints
	router := mux.NewRouter()
	// Basic health endpoint
	router.Healthz().AllowAnonymous()
	// WebSocket endpoint
	wskit.ConfigureWebSocketServer(router, nm)

	slog.Info("Starting HTTP server (with listener)", slog.String("addr", addr))
	if err := http.ListenAndServe(addr, router); err != nil {
		slog.Error("http server exited", slog.String("err", err.Error()))
		os.Exit(1)
	}
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		slog.Error("missing required environment variable", slog.String("key", key))
		os.Exit(1)
	}
	return v
}
