package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"achillesdb/pkgs/wiredtiger"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// User represents a user record.
type User struct {
	ID        primitive.ObjectID `bson:"_id"`
	Name      string             `bson:"name"`
	Email     string             `bson:"email"`
	CreatedAt time.Time          `bson:"created_at"`
}

func RunBSONExample() {
	fmt.Println("=== BSON Binary Range Scan Example ===")

	// Setup WiredTiger
	if err := os.MkdirAll("WT_HOME_BSON", 0755); err != nil {
		log.Fatalf("Failed to create WT_HOME_BSON: %v", err)
	}
	defer os.RemoveAll("WT_HOME_BSON")

	wtService := wiredtiger.WiredTiger()
	if err := wtService.Open("WT_HOME_BSON", "create"); err != nil {
		log.Fatalf("Failed to open connection: %v", err)
	}
	defer wtService.Close()

	uri := "table:bson_users"
	if err := wtService.CreateTable(uri, "key_format=u,value_format=u"); err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}

	// 1. Generate and store BSON data
	fmt.Println("\n--- Storing BSON user data ---")
	users := []User{
		{ID: primitive.NewObjectID(), Name: "Alice", Email: "alice@example.com", CreatedAt: time.Now()},
		{ID: primitive.NewObjectID(), Name: "Bob", Email: "bob@example.com", CreatedAt: time.Now().Add(1 * time.Second)},
		{ID: primitive.NewObjectID(), Name: "Charlie", Email: "charlie@example.com", CreatedAt: time.Now().Add(2 * time.Second)},
		{ID: primitive.NewObjectID(), Name: "David", Email: "david@example.com", CreatedAt: time.Now().Add(3 * time.Second)},
		{ID: primitive.NewObjectID(), Name: "Eve", Email: "eve@example.com", CreatedAt: time.Now().Add(4 * time.Second)},
	}

	for _, user := range users {
		// Keys are BSON ObjectIDs
		key := user.ID[:]

		// Values are marshaled BSON documents
		value, err := bson.Marshal(user)
		if err != nil {
			log.Fatalf("Failed to marshal user BSON: %v", err)
		}

		if err := wtService.PutBinary(uri, key, value); err != nil {
			log.Fatalf("Failed to put binary data: %v", err)
		}
		fmt.Printf("  Stored User: %s (ID: %s)\n", user.Name, user.ID.Hex())
	}

	// 2. Fetch and print all users in batches of 10
	fmt.Println("\n--- Scanning all users in batches of 10 ---")

	startKey := users[0].ID[:] // Use the first user's ObjectID as the lower bound
	endKey := users[3].ID[:]   // ObjectID upper-bound

	cursor, err := wtService.ScanRangeBinary(uri, startKey, endKey)
	if err != nil {
		log.Fatalf("Failed to create binary range cursor: %v", err)
	}
	defer cursor.Close()

	batchCount := 0
	total := 0
	for {
		countInBatch := 0
		for countInBatch < 2 && cursor.Next() {
			key, value, err := cursor.Current()
			if err != nil {
				log.Fatalf("Failed to get current item: %v", err)
			}

			// Unmarshal the data back into a User struct
			var user User
			if err := bson.Unmarshal(value, &user); err != nil {
				log.Fatalf("Failed to unmarshal user: %v", err)
			}

			var objectID [12]byte
			copy(objectID[:], key)
			fmt.Printf("  -> Found User: %s, Email: %s, ID: %s\n", user.Name, user.Email, primitive.ObjectID(objectID).Hex())
			countInBatch++
			total++
		}
		if countInBatch == 0 {
			break // No more users
		}
		batchCount++
		fmt.Printf("Batch %d: %d users\n\n", batchCount, countInBatch)
	}

	if err := cursor.Err(); err != nil {
		log.Fatalf("Cursor encountered an error: %v", err)
	}

	fmt.Printf("Found %d users in total.\n", total)
	fmt.Println("\n=== BSON Example Completed ===")
}
