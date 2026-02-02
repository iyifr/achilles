package server

import (
	dbservice "achillesdb/pkgs/db_service"
	logger "achillesdb/pkgs/logger"
	wt "achillesdb/pkgs/wiredtiger"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"syscall"

	"github.com/valyala/fasthttp"
	"go.uber.org/zap"
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

// getOptimalCacheSizeMB returns optimal WiredTiger cache size in MB
// Can be overridden with WT_CACHE_SIZE env var (in MB)
func getOptimalCacheSizeMB() int {
	if envCache := os.Getenv("WT_CACHE_SIZE"); envCache != "" {
		if size, err := strconv.Atoi(envCache); err == nil && size > 0 {
			return size
		}
	}

	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	systemRAM := m.Sys / (1024 * 1024)
	optimalCache := int(float64(systemRAM) * 0.10)

	if optimalCache < 64 {
		optimalCache = 64
	}
	if optimalCache > 1024 {
		optimalCache = 1024
	}

	return optimalCache
}

// ensureDirectory creates a directory if it doesn't exist
func ensureDirectory(path string) error {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return os.MkdirAll(path, 0755)
	}
	return nil
}

// initializeDirectories creates required directories in parallel
func initializeDirectories() error {
	errChan := make(chan error, 2)

	go func() {
		errChan <- ensureDirectory(WIREDTIGER_DIR)
	}()

	go func() {
		errChan <- ensureDirectory(VECTORS_DIR)
	}()

	for i := 0; i < 2; i++ {
		if err := <-errChan; err != nil {
			return err
		}
	}

	return nil
}

// initializeLogger sets up the application logger
func initializeLogger() (*zap.Logger, *zap.SugaredLogger) {
	log, sugaredLog := logger.InitLogger()
	return log, sugaredLog
}

// setupSignalHandlers configures graceful shutdown handlers
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

// openWiredTiger initializes WiredTiger with optimized configuration
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

	// Open WiredTiger
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
		MaxRequestBodySize: 100 * 1024 * 1024,
	}

	sugaredLog.Infow("Server started", "port", 8180)
	sugaredLog.Info("API docs available at http://localhost:8180/docs")

	if err := server.ListenAndServe(":8180"); err != nil {
		sugaredLog.Errorw("Server error", "error", err)
		os.Exit(1)
	}
}
