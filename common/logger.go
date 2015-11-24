package common

import (
	"log"
)

//DebugLevel set to true will output extra information out workers flow.
var DebugLevel bool

func Info(args ...interface{}) {
	if DebugLevel {
		Log(args...)
	}
}

func Log(args ...interface{}) {
	log.Println(args...)
}
