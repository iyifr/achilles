package server

import (
	dbservice "achillesdb/pkgs/db_service"
	logger "achillesdb/pkgs/logger"
	wt "achillesdb/pkgs/wiredtiger"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/valyala/fasthttp"
	"go.uber.org/zap"
)

var wtService = wt.WiredTiger()

var (
	WIREDTIGER_DIR = getEnvOrDefault("WT_HOME", "volumes/WT_HOME")
	VECTORS_DIR    = getEnvOrDefault("VECTORS_HOME", "volumes/vectors")
)

// initializeLogger sets up the application logger
func initializeLogger() (*zap.Logger, *zap.SugaredLogger) {
	log, sugaredLog := logger.InitLogger()
	return log, sugaredLog
}

// Graceful shutdown handler
func setupSignalHandlers(sugaredLog *zap.SugaredLogger, wtService wt.WTService) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		sugaredLog.Info("Shutting down server...")
		if err := wtService.Close(); err != nil {
			sugaredLog.Errorw("Error closing WiredTiger", "error", err)
		}
		os.Exit(0)
	}()
}

// Init Wiredtiger.
func openWiredTiger() error {
	cacheSizeMB := getOptimalCacheSizeMB()
	wtConfig := fmt.Sprintf("create,cache_size=%dMB,eviction=(threads_min=1,threads_max=2),checkpoint=(wait=600)", cacheSizeMB)
	return wtService.Open(WIREDTIGER_DIR, wtConfig)
}

func StartServer() {
	// Initialize directories and logger in parallel
	var log *zap.Logger
	var sugaredLog *zap.SugaredLogger

	errChan := make(chan error, 2)
	logChan := make(chan struct {
		log        *zap.Logger
		sugaredLog *zap.SugaredLogger
	}, 1)

	go func() {
		errChan <- initializeDirectories()
	}()

	go func() {
		l, sl := initializeLogger()
		logChan <- struct {
			log        *zap.Logger
			sugaredLog *zap.SugaredLogger
		}{l, sl}
	}()

	// Wait for directory initialization
	if err := <-errChan; err != nil {
		fmt.Printf("Failed to initialize directories: %v\n", err)
		os.Exit(1)
	}

	// Wait for logger initialization
	logResult := <-logChan
	log = logResult.log
	sugaredLog = logResult.sugaredLog
	defer log.Sync()

	if err := openWiredTiger(); err != nil {
		fmt.Printf("Error opening WiredTiger: %v\n", err)
		os.Exit(1)
	}

	// Initialize database tables
	if err := dbservice.InitTablesHelper(wtService); err != nil {
		fmt.Printf("Error initializing tables: %v\n", err)
		os.Exit(1)
	}

	// Setup signal handlers for graceful shutdown
	setupSignalHandlers(sugaredLog, wtService)

	// Setup router
	r := Router()

	// Configure and start server
	server := &fasthttp.Server{
		Handler:            r.Handler,
		ReadBufferSize:     16384,
		WriteBufferSize:    16384,
		MaxRequestBodySize: 20 * 1024 * 1024,
	}

	sugaredLog.Infow("Server started", "port", 8180)
	sugaredLog.Info("API docs available at http://localhost:8180/docs")

	if err := server.ListenAndServe(":8180"); err != nil {
		sugaredLog.Errorw("Server error", "error", err)
		os.Exit(1)
	}
}
