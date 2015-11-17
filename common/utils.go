package common

import (
	"log"
)

var DebugLevel bool

func Info(args ...interface{}) {
	if DebugLevel {
		Log(args...)
	}
}

func Log(args ...interface{}) {
	log.Println(args...)
}
