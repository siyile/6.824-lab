package raft

import "log"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)

	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}
