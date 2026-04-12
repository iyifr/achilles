package main

import (
	"fmt"
	"log"
	"os"

	wt "achillesdb/pkgs/wiredtiger"
)

// RunRangeScanExample runs different range scan examples, including
// simple range queries and cursor-based pagination.
func RunRangeScanExample() {
	fmt.Println("=== Range Scan Example ===")

	// Create WT_HOME directory
	if err := os.MkdirAll("WT_HOME", 0755); err != nil {
		log.Fatal("Failed to create WT_HOME:", err)
	}

	// Initialize WiredTiger service
	wtService := wt.WiredTiger()

	// Open connection to WT_HOME
	if err := wtService.Open("WT_HOME", "create"); err != nil {
		log.Fatal("Failed to open connection:", err)
	}
	defer func() {
		if err := wtService.Close(); err != nil {
			fmt.Printf("Warning: failed to close connection: %v\n", err)
		}
	}()

	uri := "table:range_example"

	// Create the test table
	if err := wtService.CreateTable(uri, "key_format=S,value_format=S"); err != nil {
		log.Fatal("Failed to create table:", err)
	}

	// Insert test data with alphabetical keys
	fmt.Println("\nInserting test data...")
	testData := map[string]string{
		"apple":      "fruit",
		"banana":     "fruit",
		"cherry":     "fruit",
		"date":       "fruit",
		"elderberry": "fruit",
		"fig":        "fruit",
		"grape":      "fruit",
		"honeydew":   "melon",
		"kiwi":       "fruit",
		"lemon":      "citrus",
		"mango":      "fruit",
		"orange":     "citrus",
		"papaya":     "fruit",
		"quince":     "fruit",
		"raspberry":  "berry",
		"strawberry": "berry",
		"tangerine":  "citrus",
		"watermelon": "melon",
	}

	for k, v := range testData {
		if err := wtService.PutString(uri, k, v); err != nil {
			log.Fatal("Failed to put data:", err)
		}
	}

	// === Example 1: Simple Range Query ===
	fmt.Println("\n=== Example 1: Simple Range Query ===")
	fmt.Println("Finding all fruits from 'a' to 'c':")
	func() {
		cursor, err := wtService.ScanRange(uri, "a", "c")
		if err != nil {
			log.Fatal("Failed to create range cursor:", err)
		}
		defer cursor.Close()
		count := 0
		for cursor.Next() {
			key, value, err := cursor.CurrentString()
			if err != nil {
				log.Fatal("Failed to get current:", err)
			}
			fmt.Printf("  %s -> %s\n", key, value)
			count++
		}
		fmt.Printf("Found %d items in range\n", count)
	}()

	// === Example 2: Cursor-Based Pagination ===
	fmt.Println("\n=== Example 2: Cursor-Based Pagination ===")
	pageSize := 5
	var lastKey *string // Pointer to distinguish unset vs empty string
	pageNum := 1

	for {
		endKey := "~" // Sentinel for highest lexicographical key

		// Set startKey for ScanRange: WiredTiger does not allow empty keys
		// Use a minimal key (e.g., "a") for the first page, or lastKey for subsequent pages
		var startKey string
		if lastKey == nil {
			startKey = "a"
		} else {
			startKey = *lastKey
		}

		cursor, err := wtService.ScanRange(uri, startKey, endKey)
		if err != nil {
			log.Fatal("Failed to create pagination cursor:", err)
		}

		firstInPage := true
		pageItems := 0
		var nextLastKey string

		fmt.Printf("\nPage %d:\n", pageNum)
		for cursor.Next() && pageItems < pageSize {
			// Only call CurrentString if cursor is valid
			key, value, err := cursor.CurrentString()
			if err != nil {
				cursor.Close()
				log.Fatal("Failed to get current:", err)
			}
			if lastKey != nil && firstInPage {
				// For subsequent pages, skip the first result if it's the previous page's last key
				if key == *lastKey {
					firstInPage = false
					continue
				}
			}
			fmt.Printf("  %s -> %s\n", key, value)
			nextLastKey = key
			pageItems++
			firstInPage = false
		}

		if pageItems == 0 {
			break // No more items
		}

		lastKey = &nextLastKey
		pageNum++

		if pageItems < pageSize {
			break // Last page
		}
	}

	// === Example 3: Count items in range ===
	fmt.Println("\n=== Example 3: Count Items in Range ===")
	fmt.Println("Counting fruits from 'a' to 'z':")
	func() {
		cursor, err := wtService.ScanRange(uri, "a", "z")
		if err != nil {
			log.Fatal("Failed to create count cursor:", err)
		}
		defer cursor.Close()
		totalCount := 0
		for cursor.Next() {
			totalCount++
		}
		fmt.Printf("Total items in range: %d\n", totalCount)
	}()

	fmt.Println("\n=== Range Scan Example Completed ===")
}
