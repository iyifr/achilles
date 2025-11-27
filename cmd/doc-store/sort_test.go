package main

import (
	"achillesdb/pkgs/wiredtiger"
	"fmt"
	"log"
	"os"
	"reflect"
	"testing"
)

func TestByteSorting(t *testing.T) {

	if err := os.MkdirAll("WT_HOME", 0755); err != nil {
		log.Fatal("Failed to create WT_HOME:", err)
	}

	wt := wiredtiger.WiredTiger()
	defer wt.Close()

	// Open connection
	if err := wt.Open("WT_HOME", "create"); err != nil {
		log.Fatal("Failed to open connection:", err)
	}
	defer func() {
		if err := wt.Close(); err != nil {
			fmt.Printf("Warning: failed to close connection: %v\n", err)
		}
	}()

	uri := "table:test"

	// Create table
	if err := wt.CreateTable(uri, "key_format=S,value_format=S"); err != nil {
		log.Fatal("Failed to create table:", err)
	}

	// Insert keys in random order
	keys := []string{"zebra", "alpha", "beta", "charlie", "delta"}
	for _, key := range keys {
		wt.PutString(uri, key, "value")
	}

	// Scan and collect results
	cursor, _ := wt.ScanRange(uri, " ", "~")
	var results []string
	for cursor.Next() {
		key, _, _ := cursor.CurrentString()
		results = append(results, key)
	}

	// Should be sorted: ["alpha", "beta", "charlie", "delta", "zebra"]
	expected := []string{"alpha", "beta", "charlie", "delta", "zebra"}
	if !reflect.DeepEqual(results, expected) {
		t.Errorf("Expected %v, got %v", expected, results)
	}
}
