# FAISS Service

This package provides a Go binding for the FAISS (Facebook AI Similarity Search) library.

## Usage

```go
package main

import (
    "fmt"
    "achillesdb/pkgs/faiss"
)

func main() {
    service := faiss.FAISS()
    version, err := service.GetVersion()
    if err != nil {
        fmt.Printf("Error: %v\n", err)
        return
    }
    fmt.Printf("FAISS version: %s\n", version)
}
```

## Building

### With CGO (requires FAISS library)

```bash
go build -tags cgo ./cmd/faiss-examples
```

### Without CGO

```bash
go build -tags !cgo ./cmd/faiss-examples
```

## Dependencies

When building with CGO, you need to have FAISS installed:

- macOS: `brew install faiss`
- Linux: Install FAISS from source or package manager

The CGO build expects FAISS headers in `/usr/local/include` and libraries in `/usr/local/lib`.
