package server

import (
	dbservice "achillesdb/pkgs/db_service"
	wt "achillesdb/pkgs/wiredtiger"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/valyala/fasthttp"
)

var wtService = wt.WiredTiger()

// getEnvOrDefault returns the environment variable value or a default
func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// Data directories - configurable via environment variables for Docker
var (
	WIREDTIGER_DIR = getEnvOrDefault("WT_HOME", "volumes/WT_HOME")
	VECTORS_DIR    = getEnvOrDefault("VECTORS_HOME", "volumes/vectors")
)

func StartServer() {

	if _, err := os.Stat(WIREDTIGER_DIR); os.IsNotExist(err) {
		if mkErr := os.MkdirAll(WIREDTIGER_DIR, 0755); mkErr != nil {
			fmt.Printf("failed to create %s: %v\n", WIREDTIGER_DIR, mkErr)
			os.Exit(1)
		}
	}

	if _, err := os.Stat(VECTORS_DIR); os.IsNotExist(err) {
		if mkErr := os.MkdirAll(VECTORS_DIR, 0755); mkErr != nil {
			fmt.Printf("failed to create %s: %v\n", VECTORS_DIR, mkErr)
			os.Exit(1)
		}
	}

	if err := wtService.Open(WIREDTIGER_DIR, "create,cache_size=4GB,eviction_trigger=95,eviction_dirty_target=5,eviction_dirty_trigger=15,eviction=(threads_max=8),checkpoint=(wait=300)"); err != nil {
		fmt.Printf("Error opening WiredTiger: %v\n", err)
		os.Exit(1)
	}

	err := dbservice.InitTablesHelper(wtService)

	if err != nil {
		fmt.Printf("Error initializing tables: %v\n", err)
		os.Exit(1)
	}

	// Signal handling for graceful shutdown
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		fmt.Println("\nShutting down server...")
		if err := wtService.Close(); err != nil {
			fmt.Printf("Error closing WiredTiger: %v\n", err)
		}
		os.Exit(0)
	}()

	r := Router()

	fmt.Println("Server running on http://localhost:8180")
	if err := fasthttp.ListenAndServe(":8180", r.Handler); err != nil {
		fmt.Printf("Server error: %v\n", err)
		os.Exit(1)
	}
}
