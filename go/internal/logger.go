package internal

import (
	"os"

	"github.com/pterm/pterm"
)

const (
	LogLevelDisabled = pterm.LogLevelDisabled
	LogLevelTrace    = pterm.LogLevelTrace
	LogLevelDebug    = pterm.LogLevelDebug
	LogLevelInfo     = pterm.LogLevelInfo
	LogLevelWarn     = pterm.LogLevelWarn
	LogLevelError    = pterm.LogLevelError
	LogLevelFatal    = pterm.LogLevelFatal
	LogLevelPrint    = pterm.LogLevelPrint
)

type Logger = pterm.Logger

var logger *Logger

func DefaultLogger() *Logger {
	if logger == nil {
		logger = pterm.DefaultLogger.WithLevel(LogLevelInfo).WithWriter(os.Stderr)
	}
	return logger
}
