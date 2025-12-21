package wiredtiger

// Service provides a minimal API for interacting with WiredTiger.
// This abstracts the underlying cgo implementation to allow testing and !cgo builds.
type WTService interface {
	Open(home string, config string) error
	Close() error
	CreateTable(name string, config string) error
	DeleteTable(name string) error
	TableExists(name string) (bool, error)
	PutString(table string, key string, value string) error
	GetString(table string, key string) (string, bool, error)
	DeleteString(table string, key string) error
	Exists(table string, key string) (bool, error)
	Scan(table string, threshold ...int) ([]KeyValuePair, error)
	SearchNear(table string, probeKey string) (string, string, int, bool, error)
	PutBinary(table string, key []byte, value []byte) error
	GetBinary(table string, key []byte) ([]byte, bool, error)
	DeleteBinary(table string, key []byte) error
	ExistsBinary(table string, key []byte) (bool, error)
	ScanBinary(table string) ([]BinaryKeyValuePair, error)
	SearchNearBinary(table string, probeKey []byte) ([]byte, []byte, int, bool, error)
	PutBinaryWithStringKey(table string, stringKey string, value []byte) error
	GetBinaryWithStringKey(table string, stringKey string) ([]byte, bool, error)
	DeleteBinaryWithStringKey(table string, stringKey string) error
	ScanRange(table string, startKey string, endKey string) (StringRangeCursor, error)
	ScanRangeBinary(table string, startKey, endKey []byte) (BinaryRangeCursor, error)
}

func WiredTiger() WTService {
	return WiredTigerService()
}

// KeyValuePair represents a string key/value row.
type KeyValuePair struct {
	Key   string
	Value string
}
type BinaryKeyValuePair struct {
	Key   []byte
	Value []byte
}

// StringRangeCursor provides cursor-based range iteration for string keys
type StringRangeCursor interface {
	Next() bool
	// TODO: Add Prev() for backwards iteration                            // Iterate forward
	CurrentString() (string, string, error) // Get key, value
	Err() error
	Close() error
	Valid() bool
}

// BinaryRangeCursor provides cursor-based range iteration for binary keys
type BinaryRangeCursor interface {
	Next() bool // Iterate forward
	// TODO: Add Prev() method for backwards iteration
	Current() ([]byte, []byte, error) // Get key, value
	Err() error
	Close() error
	Valid() bool
	SetBatchSize(size int) // Configure batch size
	GetBatchSize() int     // Get current batch size
}
