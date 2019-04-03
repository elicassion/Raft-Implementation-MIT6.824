package raft

import (
	"fmt"
)

//import "log"

// Debugging
const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		fmt.Printf(format, a...)
		//log.Printf(format, a...)
	}
	return
}
