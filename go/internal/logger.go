package internal

import (
	"os"
	"sync"

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

var (
	logger     *Logger
	initLogger sync.Mutex
)

func DefaultLogger() *Logger {
	if logger == nil {
		initLogger.Lock()
		logger = pterm.DefaultLogger.WithLevel(LogLevelInfo).WithWriter(os.Stderr)
		initLogger.Unlock()
	}
	return logger
}
