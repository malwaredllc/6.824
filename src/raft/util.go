package raft

import "log"

// Debugging
const DebugMode = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if DebugMode {
		log.Printf(format, a...)
	}
	return
}
