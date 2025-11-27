package main

import (
	"fmt"
	"log"
	"os"
	"runtime"
	"time"

	"achillesdb/pkgs/wiredtiger"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// CacheOptimizationDemo demonstrates the performance difference
func CacheOptimizationDemo() {
	fmt.Println("=== Cache Optimization Performance Test ===")

	// Setup WiredTiger
	if err := os.MkdirAll("WT_HOME_CACHE_DEMO", 0755); err != nil {
		log.Fatalf("Failed to create WT_HOME_CACHE_DEMO: %v", err)
	}
	defer os.RemoveAll("WT_HOME_CACHE_DEMO")

	wtService := wiredtiger.WiredTiger()
	if err := wtService.Open("WT_HOME_CACHE_DEMO", "create"); err != nil {
		log.Fatalf("Failed to open connection: %v", err)
	}
	defer wtService.Close()

	uri := "table:cache_demo"
	if err := wtService.CreateTable(uri, "key_format=u,value_format=u"); err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	numRecords := 50000
	fmt.Printf("Inserting %d records...\n", numRecords)

	insertStart := time.Now()
	for i := 0; i < numRecords; i++ {
		key := primitive.NewObjectID()
		value := bson.M{
			"id":    i,
			"name":  fmt.Sprintf("User%d", i),
			"email": fmt.Sprintf("user%d@example.com", i),
			"data":  make([]byte, 200), // 200 bytes of data per record
		}
		bsonData, _ := bson.Marshal(value)
		if err := wtService.PutBinary(uri, key[:], bsonData); err != nil {
			log.Fatalf("Failed to put binary data: %v", err)
		}
	}
	insertDuration := time.Since(insertStart)
	fmt.Printf("Inserted %d records in %v (%.2f records/sec)\n",
		numRecords, insertDuration, float64(numRecords)/insertDuration.Seconds())

	// Test different batch sizes
	batchSizes := map[string]int{
		"Old (2MB)":  2 * 1024 * 1024,
		"L1 Optimal": 24 * 1024,
		"L2 Optimal": 192 * 1024,
		"L3 Optimal": 6 * 1024 * 1024,
	}

	fmt.Println("\n=== Performance Comparison ===")
	for name, batchSize := range batchSizes {
		scanStart := time.Now()

		cursor, err := wtService.ScanRangeBinary(uri, nil, nil)
		if err != nil {
			log.Fatalf("Failed to create cursor: %v", err)
		}

		// Set the batch size using the interface method
		cursor.SetBatchSize(batchSize)

		count := 0
		var totalBytes int64
		for cursor.Next() {
			key, value, err := cursor.Current()
			if err != nil {
				log.Fatalf("Failed to get current: %v", err)
			}
			count++
			totalBytes += int64(len(key) + len(value))
		}

		if err := cursor.Err(); err != nil {
			log.Fatalf("Cursor error: %v", err)
		}
		cursor.Close()

		scanDuration := time.Since(scanStart)

		fmt.Printf("%s (%dKB): %v, %.2f records/sec, %.2f MB/sec\n",
			name, batchSize/1024, scanDuration,
			float64(count)/scanDuration.Seconds(),
			float64(totalBytes)/(1024*1024)/scanDuration.Seconds())
	}

	fmt.Println("\n=== Memory Statistics ===")
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fmt.Printf("Alloc: %d KB, TotalAlloc: %d KB, Sys: %d KB\n",
		m.Alloc/1024, m.TotalAlloc/1024, m.Sys/1024)
	fmt.Printf("NumGC: %d, PauseTotal: %v\n", m.NumGC, time.Duration(m.PauseTotalNs))
}
