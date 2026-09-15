// Package logger provides a minimal leveled logger with optional file
// output. It replaces the reference gateway's logger package with just the
// surface the VK client and handler need: a *Logger with *Logf methods plus
// a package-level DebugToFile used by the VK API client.
package logger

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Logger writes leveled messages. INFO/WARN go to the configured log file
// when present (stderr otherwise); ERROR always goes to stderr as well.
type Logger struct {
	debug    bool
	file     *log.Logger
	filePath string
	mu       sync.Mutex
}

// package-level state shared by DebugToFile
var (
	globalMu   sync.Mutex
	globalFile *log.Logger
	globalDbg  bool
)

// New creates a Logger. When debug is true, DEBUG-level messages are written
// to stderr and to file (created if missing).
func New(name string, debug bool, file string) *Logger {
	_ = name
	l := &Logger{debug: debug}
	globalMu.Lock()
	globalDbg = debug
	if file != "" {
		if dir := filepath.Dir(file); dir != "" {
			_ = os.MkdirAll(dir, 0755)
		}
		if f, err := os.OpenFile(file, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644); err == nil {
			globalFile = log.New(f, "", 0)
			l.file = globalFile
			l.filePath = file
		}
	}
	globalMu.Unlock()
	return l
}

func (l *Logger) write(level, format string, args ...interface{}) {
	line := fmt.Sprintf("%s %-5s %s", time.Now().Format("2006-01-02T15:04:05"), level, fmt.Sprintf(format, args...))
	l.mu.Lock()
	defer l.mu.Unlock()

	switch level {
	case "DEBUG":
		if !l.debug {
			return
		}
		if l.file != nil {
			l.file.Println(line)
		}
		fmt.Fprintln(os.Stderr, line)
	default:
		if l.file != nil {
			l.file.Println(line)
			if level == "ERROR" {
				fmt.Fprintln(os.Stderr, line)
			}
		} else {
			fmt.Fprintln(os.Stderr, line)
		}
	}
}

func (l *Logger) DebugLogf(format string, args ...interface{}) { l.write("DEBUG", format, args...) }
func (l *Logger) InfoLogf(format string, args ...interface{})  { l.write("INFO", format, args...) }
func (l *Logger) WarnLogf(format string, args ...interface{})  { l.write("WARN", format, args...) }
func (l *Logger) ErrorLogf(format string, args ...interface{}) { l.write("ERROR", format, args...) }
func (l *Logger) InfoLog(msg string)                           { l.write("INFO", "%s", msg) }
func (l *Logger) WarnLog(msg string)                           { l.write("WARN", "%s", msg) }

// LogFilePath reports the configured log file path (for the /log command).
func (l *Logger) LogFilePath() string {
	return l.filePath
}

// DebugToFile writes a debug-level line using the package-level debug flag.
// No-op when debug logging is disabled.
func DebugToFile(format string, args ...interface{}) {
	globalMu.Lock()
	dbg := globalDbg
	f := globalFile
	globalMu.Unlock()
	if !dbg {
		return
	}
	line := fmt.Sprintf("%s [debug] %s", time.Now().Format("2006-01-02T15:04:05"), fmt.Sprintf(format, args...))
	if f != nil {
		f.Println(line)
	}
	fmt.Fprintln(os.Stderr, line)
}
