package server

import (
	"os"
	"strconv"
)

func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// Can be overridden with WT_CACHE_SIZE env var (in MB)
func getOptimalCacheSizeMB() int {
	if envCache := os.Getenv("WT_CACHE_SIZE"); envCache != "" {
		if size, err := strconv.Atoi(envCache); err == nil && size > 0 {
			return size
		}
	}

	totalMB := int(totalSystemMemory() / (1024 * 1024))
	optimalCache := totalMB / 10 // 10% of system RAM

	if optimalCache < 64 {
		optimalCache = 64
	}
	if optimalCache > 1024 {
		optimalCache = 1024
	}

	return optimalCache
}

func ensureDirectory(path string) error {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return os.MkdirAll(path, 0755)
	}
	return nil
}

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
