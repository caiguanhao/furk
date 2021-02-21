package logger

type noopLogger int

var NoopLogger noopLogger

func (_ noopLogger) Debug(args ...interface{}) {
}

func (_ noopLogger) Info(args ...interface{}) {
}

func (_ noopLogger) Notice(args ...interface{}) {
}

func (_ noopLogger) Warning(args ...interface{}) {
}

func (_ noopLogger) Error(args ...interface{}) {
}

func (_ noopLogger) Critical(args ...interface{}) {
}

func (_ noopLogger) Fatal(args ...interface{}) {
}
