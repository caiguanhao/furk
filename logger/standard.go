package logger

import "log"

type standardLogger int

var StandardLogger standardLogger

func (_ standardLogger) Debug(args ...interface{}) {
	log.Println(args...)
}

func (_ standardLogger) Info(args ...interface{}) {
	log.Println(args...)
}

func (_ standardLogger) Notice(args ...interface{}) {
	log.Println(args...)
}

func (_ standardLogger) Warning(args ...interface{}) {
	log.Println(args...)
}

func (_ standardLogger) Error(args ...interface{}) {
	log.Println(args...)
}

func (_ standardLogger) Critical(args ...interface{}) {
	log.Println(args...)
}

func (_ standardLogger) Fatal(args ...interface{}) {
	log.Fatalln(args...)
}
