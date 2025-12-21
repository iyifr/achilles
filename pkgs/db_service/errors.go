package dbservice

import (
	wt "achillesdb/pkgs/wiredtiger"
	"errors"
	"fmt"
)

// ErrorCode represents standardized error types
type ErrorCode int

const (
	ErrCodeOK            ErrorCode = 0
	ErrCodeInvalidInput  ErrorCode = 1
	ErrCodeNotFound      ErrorCode = 2
	ErrCodeAlreadyExists ErrorCode = 3
	ErrCodeInternal      ErrorCode = 4
	ErrCodeSerialization ErrorCode = 5
	ErrCodeStorage       ErrorCode = 6
)

// Sentinel errors for common scenarios
var (
	ErrEmptyName          = errors.New("name cannot be empty")
	ErrEmptyDocuments     = errors.New("documents slice cannot be empty")
	ErrDatabaseNotFound   = errors.New("database not found")
	ErrCollectionNotFound = errors.New("collection not found")
	ErrDatabaseExists     = errors.New("database already exists")
	ErrCollectionExists   = errors.New("collection already exists")
	ErrDocumentNotFound   = errors.New("document not found")
)

// DBError represents a database operation error
type DBError struct {
	Code ErrorCode
	Err  error
}

func (e *DBError) Error() string {
	if e.Err != nil {
		return e.Err.Error()
	}
	return "unknown error"
}

func (e *DBError) Unwrap() error {
	return e.Err
}

// Is enables error comparison
func (e *DBError) Is(target error) bool {
	if target == nil {
		return false
	}
	if t, ok := target.(*DBError); ok {
		return e.Code == t.Code
	}
	return errors.Is(e.Err, target)
}

// HTTP status code mapping
func (e *DBError) HTTPStatus() int {
	switch e.Code {
	case ErrCodeInvalidInput:
		return 400 // Bad Request
	case ErrCodeNotFound:
		return 404 // Not Found
	case ErrCodeAlreadyExists:
		return 409 // Conflict
	case ErrCodeInternal, ErrCodeSerialization, ErrCodeStorage:
		return 500 // Internal Server Error
	default:
		return 500
	}
}

// Error constructors - clean and concise
func InvalidInput_Err(err error) *DBError {
	return &DBError{Code: ErrCodeInvalidInput, Err: err}
}

func NotFound_Err(err error) *DBError {
	return &DBError{Code: ErrCodeNotFound, Err: err}
}

func AlreadyExists_Err(err error) *DBError {
	return &DBError{Code: ErrCodeAlreadyExists, Err: err}
}

func Internal_Err(err error) *DBError {
	return &DBError{Code: ErrCodeInternal, Err: err}
}

func Serialization_Err(err error) *DBError {
	return &DBError{Code: ErrCodeSerialization, Err: err}
}

func Storage_Err(err error) *DBError {
	return &DBError{Code: ErrCodeStorage, Err: err}
}

// Wrap wraps a standard error with additional context
func Wrap_Err(err error, format string, args ...interface{}) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %w", fmt.Sprintf(format, args...), err)
}

// IsNotFoundError checks if the error is a WiredTiger "not found" error
func IsNotFoundError(err error) bool {
	return errors.Is(err, wt.ErrNotFound)
}

// IsBusyError checks if the error is a WiredTiger "busy" error (table has open handles)
func IsBusyError(err error) bool {
	return errors.Is(err, wt.ErrBusy)
}
