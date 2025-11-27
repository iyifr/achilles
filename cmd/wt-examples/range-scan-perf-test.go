package main

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"runtime"
	"runtime/debug"
	"time"

	wt "achillesdb/pkgs/wiredtiger"
)

// TestResult holds metrics for a test run
type TestResult struct {
	Name            string
	Duration        time.Duration
	RecordsFound    int
	MemoryAllocated int64
	MemoryReleased  int64
}

// MemorySnapshot captures memory state at a point in time
func captureMemory() runtime.MemStats {
	debug.FreeOSMemory() // Force release to OS
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m
}

func RunDetailedRangeScanPerformanceTest() {
	fmt.Println("=== Detailed Range Scan Performance Test ===")

	// Create directory
	if err := os.MkdirAll("WT_PERF_TEST", 0755); err != nil {
		log.Fatal("Failed to create WT_PERF_TEST:", err)
	}
	defer os.RemoveAll("WT_PERF_TEST")

	// Initialize service
	wtService := wt.WiredTigerService()

	// Open connection with optimized config
	if err := wtService.Open("WT_PERF_TEST", "create,cache_size=500M"); err != nil {
		log.Fatal("Failed to open connection:", err)
	}
	defer wtService.Close()

	uri := "table:perf_test"

	// Create table
	if err := wtService.CreateTable(uri, "key_format=S,value_format=S"); err != nil {
		log.Fatal("Failed to create table:", err)
	}

	// Generate test data
	fmt.Println("=== Phase 1: Data Generation ===")
	numRecords := 500000
	fmt.Printf("Generating %d records...\n", numRecords)

	startTime := time.Now()
	for i := 0; i < numRecords; i++ {
		key := fmt.Sprintf("user_%08d", i)
		value := fmt.Sprintf("data_%d_with_some_content", i)
		if err := wtService.PutString(uri, key, value); err != nil {
			log.Fatal("Failed to put data:", err)
		}
	}
	insertTime := time.Since(startTime)
	fmt.Printf("✓ Inserted %d records in %v (%.0f records/sec)\n\n", numRecords, insertTime, float64(numRecords)/insertTime.Seconds())

	// Force garbage collection
	runtime.GC()
	runtime.GC()
	time.Sleep(100 * time.Millisecond)

	// Test Suite 1: Equivalent Range Comparisons
	fmt.Println("=== Phase 2: Equivalent Range Comparisons ===")
	fmt.Println("(Comparing operations on same result set size)")

	testRanges := []struct {
		name     string
		start    string
		end      string
		expected int
	}{
		{"Small (1k records)", "user_00000000", "user_00001000", 1000},
		{"Medium (10k records)", "user_00000000", "user_00010000", 10000},
		{"Large (50k records)", "user_00000000", "user_00050000", 50000},
	}

	fullScanResults := make([]TestResult, len(testRanges))
	rangeScanResults := make([]TestResult, len(testRanges))

	for idx, tr := range testRanges {
		fmt.Printf("Test: %s\n", tr.name)

		// Test A: Full scan with limit
		fmt.Print("  → Full scan with limit... ")
		m1 := captureMemory()
		startTime := time.Now()

		pairs, err := wtService.Scan(uri, tr.expected)
		if err != nil {
			log.Fatal("Failed to scan:", err)
		}

		duration := time.Since(startTime)
		m2 := captureMemory()

		fullScanResults[idx] = TestResult{
			Name:            tr.name + " (Full Scan)",
			Duration:        duration,
			RecordsFound:    len(pairs),
			MemoryAllocated: int64(m2.Alloc - m1.Alloc),
		}

		fmt.Printf("%v (%d records, %dKB)\n", duration, len(pairs), fullScanResults[idx].MemoryAllocated/1024)

		// Verify correctness
		if len(pairs) != tr.expected {
			fmt.Printf("  ⚠️  WARNING: Expected %d records, got %d\n", tr.expected, len(pairs))
		}

		// Verify key ranges
		if len(pairs) > 0 {
			if pairs[0].Key < tr.start {
				fmt.Printf("  ⚠️  WARNING: First key %s is before start %s\n", pairs[0].Key, tr.start)
			}
			if pairs[len(pairs)-1].Key >= tr.end {
				fmt.Printf("  ⚠️  WARNING: Last key %s is not before end %s\n", pairs[len(pairs)-1].Key, tr.end)
			}
		}

		time.Sleep(100 * time.Millisecond)
		runtime.GC()

		// Test B: Range scan (if available)
		fmt.Print("  → Range scan... ")

		m1 = captureMemory()
		startTime = time.Now()

		cursor, err := wtService.ScanRange(uri, tr.start, tr.end)
		if err != nil {
			fmt.Printf("SKIPPED (not implemented yet)\n\n")
			continue
		}

		recordCount := 0
		lastKey := ""
		firstKey := ""
		outOfBounds := false

		for cursor.Next() {
			key, _, err := cursor.CurrentString()
			if err != nil {
				fmt.Printf("ERROR: %v\n", err)
				break
			}

			if firstKey == "" {
				firstKey = key
			}

			// Bounds checking
			if key < tr.start {
				fmt.Printf("  ⚠️  Out of bounds (below): %s < %s\n", key, tr.start)
				outOfBounds = true
			}
			if key >= tr.end {
				fmt.Printf("  ⚠️  Out of bounds (above): %s >= %s\n", key, tr.end)
				outOfBounds = true
			}

			lastKey = key
			recordCount++
		}
		cursor.Close()

		duration = time.Since(startTime)
		m2 = captureMemory()

		rangeScanResults[idx] = TestResult{
			Name:            tr.name + " (Range Scan)",
			Duration:        duration,
			RecordsFound:    recordCount,
			MemoryAllocated: int64(m2.Alloc - m1.Alloc),
		}

		fmt.Printf("%v (%d records, %dKB)\n", duration, recordCount, rangeScanResults[idx].MemoryAllocated/1024)

		// Verify correctness
		if recordCount != tr.expected {
			fmt.Printf("  ⚠️  ERROR: Expected %d records, got %d\n", tr.expected, recordCount)
		}
		if firstKey < tr.start {
			fmt.Printf("  ⚠️  ERROR: First key %s is before start %s\n", firstKey, tr.start)
		}
		if lastKey >= tr.end {
			fmt.Printf("  ⚠️  ERROR: Last key %s is not before end %s\n", lastKey, tr.end)
		}
		if outOfBounds {
			fmt.Println("  ⚠️  CRITICAL: Bounds enforcement failed!")
		}

		fmt.Println()
		time.Sleep(100 * time.Millisecond)
	}

	// Comparison Summary
	fmt.Println("=== Performance Comparison ===")
	fmt.Printf("%-30s | %-12s | %-10s | %-10s\n", "Operation", "Duration", "Records", "Memory")
	fmt.Println(string(bytes.Repeat([]byte("-"), 80)))

	for idx := range testRanges {
		fmt.Printf("%-30s | %12v | %10d | %10dKB\n",
			fullScanResults[idx].Name,
			fullScanResults[idx].Duration,
			fullScanResults[idx].RecordsFound,
			fullScanResults[idx].MemoryAllocated/1024)
		fmt.Printf("%-30s | %12v | %10d | %10dKB\n",
			rangeScanResults[idx].Name,
			rangeScanResults[idx].Duration,
			rangeScanResults[idx].RecordsFound,
			rangeScanResults[idx].MemoryAllocated/1024)
		fmt.Println(string(bytes.Repeat([]byte("-"), 80)))
	}

	// Phase 3: Memory Efficiency Test
	fmt.Println("\n=== Phase 3: Memory Efficiency (Constant Memory Test) ===")
	fmt.Println("(Scanning increasing ranges, memory should remain constant)")

	memorySamples := make([]int64, 5)
	recordSamples := make([]int, 5)

	testSizes := []struct {
		name      string
		endRecord int
	}{
		{"10k records", 10000},
		{"50k records", 50000},
		{"100k records", 100000},
		{"250k records", 250000},
		{"500k records", 500000},
	}

	for idx, ts := range testSizes {
		fmt.Printf("Scanning %s... ", ts.name)

		runtime.GC()
		m1 := captureMemory()

		endKey := fmt.Sprintf("user_%08d", ts.endRecord)
		cursor, err := wtService.ScanRange(uri, "user_00000000", endKey)
		if err != nil {
			fmt.Printf("SKIPPED\n")
			continue
		}

		count := 0
		for cursor.Next() {
			count++
		}
		cursor.Close()

		m2 := captureMemory()

		memorySamples[idx] = int64(m2.Alloc - m1.Alloc)
		recordSamples[idx] = count

		fmt.Printf("%d records, %dKB allocated\n", count, memorySamples[idx]/1024)
	}

	// Analyze memory consistency
	if len(memorySamples) > 1 {
		avgMem := int64(0)
		for _, m := range memorySamples {
			avgMem += m
		}
		avgMem /= int64(len(memorySamples))

		fmt.Println("\nMemory Consistency Analysis:")
		maxDeviation := int64(0)
		for idx, m := range memorySamples {
			if m > 0 {
				deviation := (m - avgMem) * 100 / avgMem
				fmt.Printf("  Sample %d: %+d%% from average\n", idx+1, deviation)
				if deviation > maxDeviation {
					maxDeviation = deviation
				}
			}
		}

		if maxDeviation > 50 {
			fmt.Printf("⚠️  WARNING: High memory variance (%d%%) - memory not constant!\n", maxDeviation)
		} else if maxDeviation > 20 {
			fmt.Printf("⚠️  CAUTION: Moderate memory variance (%d%%)\n", maxDeviation)
		} else {
			fmt.Printf("✓ Memory usage is consistent (variance: %d%%)\n", maxDeviation)
		}
	}

	// Phase 4: Correctness Verification
	fmt.Println("\n=== Phase 4: Correctness Verification ===")

	testCases := []struct {
		name          string
		start         string
		end           string
		shouldInclude []string
		shouldExclude []string
	}{
		{
			"Exact boundaries",
			"user_00000100",
			"user_00000200",
			[]string{"user_00000100", "user_00000199"},
			[]string{"user_00000099", "user_00000200"},
		},
		{
			"Empty range",
			"user_00500000",
			"user_00500001",
			[]string{},
			[]string{},
		},
		{
			"Single record",
			"user_00000500",
			"user_00000501",
			[]string{"user_00000500"},
			[]string{"user_00000499", "user_00000501"},
		},
	}

	for _, tc := range testCases {
		fmt.Printf("Test: %s\n", tc.name)
		fmt.Printf("  Range: [%s, %s)\n", tc.start, tc.end)

		cursor, err := wtService.ScanRange(uri, tc.start, tc.end)
		if err != nil {
			fmt.Printf("  SKIPPED (not implemented): %v\n\n", err)
			continue
		}

		results := make(map[string]bool)
		for cursor.Next() {
			key, _, _ := cursor.CurrentString()
			results[key] = true
		}
		cursor.Close()

		// Verify included keys
		errors := 0
		for _, key := range tc.shouldInclude {
			if !results[key] {
				fmt.Printf("  ✗ Missing expected key: %s\n", key)
				errors++
			}
		}

		// Verify excluded keys
		for _, key := range tc.shouldExclude {
			if results[key] {
				fmt.Printf("  ✗ Unexpected key included: %s\n", key)
				errors++
			}
		}

		if errors == 0 {
			fmt.Printf("  ✓ All assertions passed (%d results)\n", len(results))
		} else {
			fmt.Printf("  ✗ %d assertions failed\n", errors)
		}
		fmt.Println()
	}
	cursor, _ := wtService.ScanRange(uri, "user_00000500", "user_00000510")
	defer cursor.Close()

	// First iteration - should return user_00000500
	cursor.Next()
	key, _, _ := cursor.CurrentString()
	fmt.Printf("First key: %s (expected user_00000500)\n", key)

	// This will show if it's returning 00000501 (off by one)
}
