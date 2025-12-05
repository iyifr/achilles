package main

import (
	"fmt"
	"log"
	"os"

	wt "achillesdb/pkgs/wiredtiger"
)

func RunRangeScanExample() {
	fmt.Println("=== Range Scan Example ===")

	// Create directory
	if err := os.MkdirAll("WT_HOME", 0755); err != nil {
		log.Fatal("Failed to create WT_HOME:", err)
	}

	// Initialize service
	wtService := wt.WiredTiger()

	// Open connection
	if err := wtService.Open("WT_HOME", "create"); err != nil {
		log.Fatal("Failed to open connection:", err)
	}
	defer func() {
		if err := wtService.Close(); err != nil {
			fmt.Printf("Warning: failed to close connection: %v\n", err)
		}
	}()

	uri := "table:range_example"

	// Create table
	if err := wtService.CreateTable(uri, "key_format=S,value_format=S"); err != nil {
		log.Fatal("Failed to create table:", err)
	}

	// Insert test data with alphabetical keys
	fmt.Println("\nInserting test data...")
	testData := map[string]string{
		"db:xyz:apple": "fruit",
		"banana":       "fruit",
		"cherry":       "fruit",
		"date":         "fruit",
		"elderberry":   "fruit",
		"fig":          "fruit",
		"grape":        "fruit",
		"honeydew":     "melon",
		"kiwi":         "fruit",
		"lemon":        "citrus",
		"mango":        "fruit",
		"orange":       "citrus",
		"papaya":       "fruit",
		"quince":       "fruit",
		"raspberry":    "berry",
		"strawberry":   "berry",
		"tangerine":    "citrus",
		"watermelon":   "melon",
	}

	for k, v := range testData {
		if err := wtService.PutString(uri, k, v); err != nil {
			log.Fatal("Failed to put data:", err)
		}
		// fmt.Printf("  %s -> %s\n", k, v)
	}

	// Example 1: Simple range query
	fmt.Println("\n=== Example 1: Simple Range Query ===")
	fmt.Println("Finding all fruits from 'a' to 'c':")

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

	// // Example 2: Pagination simulation
	// fmt.Println("\n=== Example 2: Pagination Simulation ===")
	// fmt.Println("Simulating pagination with page size 5:")

	// pageSize := 5
	// lastKey := ""
	// pageNum := 1

	// for {
	// 	var endKey string
	// 	if lastKey == "" {
	// 		endKey = "~" // High sentinel for first page
	// 	} else {
	// 		endKey = "~"
	// 	}

	// 	cursor, err := wtService.ScanRange(uri, lastKey, endKey)
	// 	if err != nil {
	// 		log.Fatal("Failed to create pagination cursor:", err)
	// 	}

	// 	fmt.Printf("\nPage %d:\n", pageNum)
	// 	pageItems := 0
	// 	var nextLastKey string

	// 	for cursor.Next() && pageItems < pageSize {
	// 		key, value, err := cursor.CurrentString()
	// 		if err != nil {
	// 			log.Fatal("Failed to get current:", err)
	// 		}
	// 		fmt.Printf("  %s -> %s\n", key, value)
	// 		nextLastKey = key
	// 		pageItems++
	// 	}
	// 	cursor.Close()

	// 	if pageItems == 0 {
	// 		break // No more items
	// 	}

	// 	lastKey = nextLastKey
	// 	pageNum++

	// 	if pageItems < pageSize {
	// 		break // Last page
	// 	}
	// }

	// Example 3: Count items in range
	// fmt.Println("\n=== Example 4: Count Items in Range ===")
	// fmt.Println("Counting fruits from 'a' to 'z':")

	// cursor, err = wtService.ScanRange(uri, "a", "z")
	// if err != nil {
	// 	log.Fatal("Failed to create count cursor:", err)
	// }
	// defer cursor.Close()

	// totalCount := 0
	// for cursor.Next() {
	// 	totalCount++
	// }
	// fmt.Printf("Total items in range: %d\n", totalCount)

	fmt.Println("\n=== Range Scan Example Completed ===")
}
