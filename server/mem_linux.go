package server

import "syscall"

func totalSystemMemory() uint64 {
	var info syscall.Sysinfo_t
	if err := syscall.Sysinfo(&info); err != nil {
		return 0
	}
	return info.Totalram * uint64(info.Unit)
}
