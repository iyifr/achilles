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
var WIREDTIGER_DIR = "volumes/WT_HOME"

func StartServer() {

	if _, err := os.Stat("volumes/WT_HOME"); os.IsNotExist(err) {
		if mkErr := os.MkdirAll("volumes/WT_HOME", 0755); mkErr != nil {
			fmt.Printf("failed to create volumes/db_files: %v\n", mkErr)
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

	// Set up signal handling for graceful shutdown
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

	fmt.Println("Server running on http://localhost:8080")
	fasthttp.ListenAndServe(":8080", r.Handler)
}
