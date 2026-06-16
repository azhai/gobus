package log

import (
	"bytes"
	"fmt"
	"go/format"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
)

// ParseLevel converts a string log level to slog.Level.
// Supported values (case-insensitive): "debug", "info", "warn"/"warning", "error".
// Returns slog.LevelInfo for unknown or invalid values.
func ParseLevel(logLevel string) slog.Level {
	switch strings.ToUpper(logLevel) {
	case "DEBUG":
		return slog.LevelDebug
	case "", "INFO", "NOTICE":
		return slog.LevelInfo
	case "WARN", "WARNING":
		return slog.LevelWarn
	case "ERR", "ERROR", "FATAL":
		return slog.LevelError
	default:
		return slog.LevelInfo // default to Info for unknown values
	}
}

// SetDefault sets the default logger to the one created by CreateRotateLogger with daily rotation.
// Parameters:
//   - logFile: path to the log file (or "stdout", "stderr", "/dev/null", "")
//   - logLevel: minimum log level to output (string)
//     Supported values: "debug", "info", "warn", "error" (case-insensitive)
//
// This is a convenience wrapper that calls SetDefaultByFile with maxBackups=7.
func SetDefault(logFile, logLevel string) {
	SetDefaultByFile(CycleDaily, logFile, logLevel, 7)
}

// SetDefaultByFile sets the default logger to the one created by CreateRotateLogger with custom rotation cycle.
// Parameters:
//   - cycle: rotation cycle (CycleMonthly, CycleWeekly, CycleDaily, CycleHourly, or CycleMinutely)
//   - logFile: path to the log file (or "stdout", "stderr", "/dev/null", "")
//   - logLevel: minimum log level to output (string)
//     Supported values: "debug", "info", "warn", "error" (case-insensitive)
//   - maxBackups: maximum number of old log files to retain (0 = no limit)
func SetDefaultByFile(cycle RotateCycle, logFile, logLevel string, maxBackups int) {
	logger, err := NewRotateLogger(cycle, logFile, maxBackups, true)
	if err != nil {
		panic(err)
	}
	slog.SetLogLoggerLevel(ParseLevel(logLevel))
	slog.SetDefault(logger)
}

// NewDailyLogger creates an slog.Logger with daily log rotation.
// This is a convenience wrapper around CreateRotateLogger with CycleDaily and compression enabled.
// Deprecated: Use CreateRotateLogger with CycleDaily instead for more flexibility.
// Parameters:
//   - logFile: path to the log file (or "stdout", "stderr", "/dev/null", "")
//   - maxBackups: maximum number of old log files to retain (0 = no limit)
func NewDailyLogger(logFile string, maxBackups int) (*slog.Logger, error) {
	return NewRotateLogger(CycleDaily, logFile, maxBackups, true)
}

// NewRotateLogger creates an slog.Logger with log rotation based on the specified cycle.
// Supported cycles: CycleMonthly, CycleWeekly, CycleDaily (default), CycleHourly, CycleMinutely.
//
// Parameters:
//   - cycle: rotation cycle (CycleMonthly, CycleWeekly, CycleDaily, CycleHourly, or CycleMinutely)
//   - logFile: path to the log file (or "stdout", "stderr", "/dev/null", "")
//   - maxBackups: maximum number of old log files to retain (0 = no limit)
//   - compress: whether to gzip-compress rotated log files (default: true)
func NewRotateLogger(cycle RotateCycle, logFile string, maxBackups int, compress bool) (*slog.Logger, error) {
	// Special values that don't need rotation: delegate to NewFileLogger
	switch strings.ToLower(logFile) {
	case "", "/dev/null", "stdout", "stderr":
		return NewFileLogger(logFile, true, false)
	}
	writer := NewRotateWriter(cycle, logFile, maxBackups, 0, compress)
	logger := NewLoggerByWriter(writer, true, false)
	return logger, nil
}

// NewFileLogger creates a logger by log file
// If log file directory doesn't exist, it creates it
// Returns the created logger or an error
func NewFileLogger(logFile string, isJSON, addSource bool) (*slog.Logger, error) {
	var wt io.Writer
	switch strings.ToLower(logFile) {
	case "", "/dev/null":
		wt = nil
	case "stdout":
		wt = os.Stdout
	case "stderr":
		wt = os.Stderr
	default:
		var err error
		if wt, err = NewFileWriter(logFile); err != nil {
			return nil, err
		}
	}
	logger := NewLoggerByWriter(wt, isJSON, addSource)
	return logger, nil
}

// NewLoggerByWriter creates a logger by writer
// isJSON: whether to use JSON format (default: true)
// addSource: whether to add source information (default: false)
// Returns the created logger or an error
func NewLoggerByWriter(writer io.Writer, isJSON, addSource bool) *slog.Logger {
	if writer == nil {
		return slog.New(slog.DiscardHandler)
	}
	opts := &slog.HandlerOptions{AddSource: addSource}
	if isJSON {
		return slog.New(slog.NewJSONHandler(writer, opts))
	} else {
		return slog.New(slog.NewTextHandler(writer, opts))
	}
}

// NewFileWriter creates a file handler for logging
// It ensures the directory exists and opens the file for writing
// Returns the file handler or an error
func NewFileWriter(logFile string) (*os.File, error) {
	if err := MakeDirForFile(logFile); err != nil {
		return nil, err
	}
	mode := os.O_RDWR | os.O_CREATE | os.O_APPEND
	return os.OpenFile(logFile, mode, 0666)
}

// MakeDirForFile creates directory for file if it doesn't exist
// It creates all necessary parent directories
// Returns an error if directory creation fails
func MakeDirForFile(filename string) (err error) {
	dir := filepath.Dir(filename)
	if _, err = os.Stat(dir); os.IsNotExist(err) {
		return os.MkdirAll(dir, 0755)
	}
	return err
}

// WriteToFile writes the formatted content to a file.
func WriteToFile(buf *bytes.Buffer, outputPath string) error {
	formatted, err := format.Source(buf.Bytes())
	if err != nil {
		return fmt.Errorf("error formatting output: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(outputPath), 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}
	if err := os.WriteFile(outputPath, formatted, 0644); err != nil {
		return fmt.Errorf("error writing file: %w", err)
	}
	return nil
}
