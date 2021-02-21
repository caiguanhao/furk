package logger

type (
	Logger interface {
		Debug(args ...interface{})
		Info(args ...interface{})
		Notice(args ...interface{})
		Warning(args ...interface{})
		Error(args ...interface{})
		Critical(args ...interface{})
		Fatal(args ...interface{})
	}
)
