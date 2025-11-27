package main

import (
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"achillesdb/pkgs/wiredtiger"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// PerformanceStats holds performance metrics
type PerformanceStats struct {
	TotalRecords     int64
	TotalDuration    time.Duration
	RecordsPerSecond float64
	BytesRead        int64
	BytesPerSecond   float64
}

// User represents a demo BSON user document for parallel examples.
type BUser struct {
	ID        primitive.ObjectID     `bson:"_id"`
	Name      string                 `bson:"name"`
	Email     string                 `bson:"email"`
	CreatedAt time.Time              `bson:"created_at"`
	Age       int                    `bson:"age"`
	Score     float64                `bson:"score"`
	Active    bool                   `bson:"active"`
	Nickname  string                 `bson:"nickname"`
	Meta      map[string]interface{} `bson:"meta"`
}

// computeBatchRanges returns a slice of [start, end) ranges for numGoroutines over numItems.
func computeBatchRanges(numItems, numGoroutines int) [][2]int {
	if numItems <= 0 || numGoroutines <= 0 {
		return nil
	}

	if numGoroutines > numItems {
		numGoroutines = numItems
	}

	batches := make([][2]int, numGoroutines)
	batchSize := (numItems + numGoroutines - 1) / numGoroutines // Ceiling division

	for i := 0; i < numGoroutines; i++ {
		start := i * batchSize
		end := start + batchSize
		if end > numItems {
			end = numItems
		}
		if start >= numItems {
			start = numItems
			end = numItems
		}
		batches[i] = [2]int{start, end}
	}

	return batches
}

// incrementBytesRead increments an atomic int64 by value
func incrementBytesRead(total *int64, delta int64) {
	atomic.AddInt64(total, delta)
}

// incrementRecordsRead increments an atomic int64 by value
func incrementRecordsRead(total *int64, delta int64) {
	atomic.AddInt64(total, delta)
}

func RunParallelBSONExample() {
	fmt.Println("=== BSON Parallel Scanning Example ===")

	// Setup WiredTiger
	if err := os.MkdirAll("WT_HOME_BSON_PARALLEL", 0755); err != nil {
		log.Fatalf("Failed to create WT_HOME_BSON_PARALLEL: %v", err)
	}
	defer os.RemoveAll("WT_HOME_BSON_PARALLEL")

	wtService := wiredtiger.WiredTiger()
	if err := wtService.Open("WT_HOME_BSON_PARALLEL", "create"); err != nil {
		log.Fatalf("Failed to open connection: %v", err)
	}
	defer wtService.Close()

	uri := "table:bson_users"
	if err := wtService.CreateTable(uri, "key_format=u,value_format=u"); err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}
	fmt.Println("\n--- Generating and Storing 1,000,000 BSON user data ---")
	numUsers := 10000
	users := make([]BUser, numUsers)

	// Time the data generation
	genStart := time.Now()
	letters := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	randomString := func(n int) string {
		b := make([]rune, n)
		for i := range b {
			b[i] = letters[rand.Intn(len(letters))]
		}
		return string(b)
	}
	for i := 0; i < numUsers; i++ {
		users[i] = BUser{
			ID:        primitive.NewObjectID(),
			Name:      fmt.Sprintf("User%d", i+1),
			Email:     fmt.Sprintf("user%d@example.com", i+1),
			CreatedAt: time.Now().Add(time.Duration(i) * time.Second),
			// Added random data fields to the struct
			Age:      18 + rand.Intn(50),   // random age 18-67
			Score:    rand.Float64() * 100, // random float between 0 and 100
			Active:   rand.Intn(2) == 1,    // random bool
			Nickname: randomString(8),      // random string of length 8
			Meta: map[string]interface{}{ // random-ish metadata
				"city":      randomString(6),
				"verified":  rand.Intn(2) == 1,
				"favorites": []string{randomString(5), randomString(5)},
			},
		}
	}
	fmt.Printf(" User size: %d\n", len(users))
	genDuration := time.Since(genStart)
	fmt.Printf("  Data generation took: %v\n", genDuration)

	// Time the data insertion
	insertStart := time.Now()
	var totalBytesWritten int64
	for _, user := range users {
		key := user.ID[:]
		value, err := bson.Marshal(user)
		if err != nil {
			log.Fatalf("Failed to marshal user BSON: %v", err)
		}
		atomic.AddInt64(&totalBytesWritten, int64(len(key)+len(value)))
		if err := wtService.PutBinary(uri, key, value); err != nil {
			log.Fatalf("Failed to put binary data: %v", err)
		}
	}
	insertDuration := time.Since(insertStart)
	fmt.Printf("  Stored %d users in %v\n", numUsers, insertDuration)
	fmt.Printf("  Write performance: %.2f records/sec, %.2f MB/sec\n",
		float64(numUsers)/insertDuration.Seconds(),
		float64(totalBytesWritten)/(1024*1024)/insertDuration.Seconds())

	// 2. Split into batches for parallel scanning with performance measurement
	fmt.Println("\n--- Parallel Scanning with 8 Goroutines (Performance Test) ---")
	numGoroutines := 10

	var wg sync.WaitGroup
	var totalRecordsRead int64
	var totalBytesRead int64

	scanStart := time.Now()

	batchRanges := computeBatchRanges(numUsers, numGoroutines)
	for i, rng := range batchRanges {
		wg.Add(1)

		startIdx := rng[0]
		endIdx := rng[1]

		fmt.Printf("Batch %d: range [%d, %d)\n", i, startIdx, endIdx)

		if startIdx >= endIdx {
			wg.Done()
			continue
		}

		startKey := users[startIdx].ID[:]
		var endKey []byte
		if endIdx == len(users) {
			endKey = bytes.Repeat([]byte{0xFF}, 12)
		} else {
			endKey = users[endIdx].ID[:]
		}

		fmt.Printf("Batch %d: StartKey: %s (users[%d]), EndKey: %s (users[%d])\n",
			i,
			primitive.ObjectID(startKey).Hex(), startIdx,
			primitive.ObjectID(endKey).Hex(),
			func() int {
				if endIdx == len(users) {
					return endIdx - 1
				} else {
					return endIdx
				}
			}(),
		)

		go func(uri string, startKey, endKey []byte, batchStart, batchEnd, goroutineID int) {
			defer wg.Done()

			goroutineStart := time.Now()
			cursor, err := wtService.ScanRangeBinary(uri, startKey, endKey)
			if err != nil {
				log.Printf("Goroutine %d: Failed to create binary range cursor: %v", goroutineID, err)
				return
			}
			defer cursor.Close()

			cursor.SetBatchSize(192 * (1024 * 1024))

			fmt.Printf("Goroutine %d: Starting scan from %s (idx=%d) to %s (idx=%d)\n",
				goroutineID, primitive.ObjectID(startKey).Hex(), batchStart, primitive.ObjectID(endKey).Hex(), batchEnd-1)

			count := 0
			var bytesRead int64
			for cursor.Next() {
				key, value, err := cursor.Current()
				if err != nil {
					log.Printf("Goroutine %d: Failed to get current item: %v", goroutineID, err)
					continue
				}

				bytesRead += int64(len(key) + len(value))
				count++

				// Unmarshal the data back into a User struct
				var user BUser
				if err := bson.Unmarshal(value, &user); err != nil {
					log.Printf("Goroutine %d: Failed to unmarshal user: %v", goroutineID, err)
					continue
				}

				var objectID [12]byte
				copy(objectID[:], key)

				if count <= 3 {
					fmt.Printf("Goroutine %d: -> Found User: %s, Email: %s, ID: %s\n", goroutineID, user.Name, user.Email, primitive.ObjectID(objectID).Hex())
				}

			}

			if err := cursor.Err(); err != nil {
				log.Printf("Goroutine %d: Cursor encountered an error: %v", goroutineID, err)
			}

			goroutineDuration := time.Since(goroutineStart)
			fmt.Printf("Goroutine %d: Found %d users in %v (%.2f records/sec, %.2f MB/sec)\n",
				goroutineID, count, goroutineDuration,
				float64(count)/goroutineDuration.Seconds(),
				float64(bytesRead)/(1024*1024)/goroutineDuration.Seconds())
			incrementRecordsRead(&totalRecordsRead, int64(count))
			incrementBytesRead(&totalBytesRead, bytesRead)
		}(uri, startKey, endKey, startIdx, endIdx, i)
	}

	wg.Wait()
	scanDuration := time.Since(scanStart)

	// Calculate and display performance statistics
	stats := PerformanceStats{
		TotalRecords:     totalRecordsRead,
		TotalDuration:    scanDuration,
		RecordsPerSecond: float64(totalRecordsRead) / scanDuration.Seconds(),
		BytesRead:        totalBytesRead,
		BytesPerSecond:   float64(totalBytesRead) / scanDuration.Seconds(),
	}

	fmt.Println("\n=== PERFORMANCE SUMMARY ===")
	fmt.Printf("Total records read: %d\n", stats.TotalRecords)
	fmt.Printf("Total scan duration: %v\n", stats.TotalDuration)
	fmt.Printf("Records per second: %.2f\n", stats.RecordsPerSecond)
	fmt.Printf("Total bytes read: %.2f MB\n", float64(stats.BytesRead)/(1024*1024))
	fmt.Printf("Throughput: %.2f MB/sec\n", stats.BytesPerSecond/(1024*1024))
	fmt.Printf("Average record size: %.2f bytes\n", float64(stats.BytesRead)/float64(stats.TotalRecords))

	fmt.Println("\n=== BSON Parallel Scanning Example Completed ===")
}
