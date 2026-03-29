package server

import (
	"syscall"
	"unsafe"
)

func totalSystemMemory() uint64 {
	val, err := syscall.Sysctl("hw.memsize")
	if err != nil || len(val) < 8 {
		return 0
	}
	return *(*uint64)(unsafe.Pointer(&[]byte(val)[0]))
}
