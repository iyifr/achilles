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

	return 1228
}

func ensureDirectory(path string) error {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return os.MkdirAll(path, 0755)
	}
	return nil
}

func createDataVolumes() error {
	if err := ensureDirectory(WIREDTIGER_DIR); err != nil {
		return err
	}
	if err := ensureDirectory(VECTORS_DIR); err != nil {
		return err
	}
	return nil
}
