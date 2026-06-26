package log

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Manager manages log files under a base directory.
// It provides read, clear, and purge operations for service logs.
type Manager struct {
	baseDir string
}

// NewManager creates a Manager rooted at baseDir.
func NewManager(baseDir string) *Manager {
	return &Manager{baseDir: baseDir}
}

// CreateWriter opens (creating directories as needed) a log file for append writes.
// Returns nil, nil when path is empty.
func (m *Manager) CreateWriter(serviceName string, path string) (*os.File, error) {
	if path == "" {
		return nil, nil
	}

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create log directory: %w", err)
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open log file: %w", err)
	}

	return f, nil
}

// ReadLogs reads the last `lines` lines of <baseDir>/<serviceName>.log.
// Returns all lines when lines <= 0 or lines >= total.
func (m *Manager) ReadLogs(serviceName string, lines int) ([]string, error) {
	logPath := filepath.Join(m.baseDir, serviceName+".log")

	data, err := os.ReadFile(logPath)
	if err != nil {
		if os.IsNotExist(err) {
			return []string{}, nil
		}
		return nil, fmt.Errorf("failed to read log file: %w", err)
	}

	allLines := strings.Split(string(data), "\n")
	if lines <= 0 || lines >= len(allLines) {
		return allLines, nil
	}

	start := len(allLines) - lines
	if start < 0 {
		start = 0
	}

	return allLines[start:], nil
}

// ReadFileLines reads the last `lines` lines of the given file path.
// Returns all lines when lines <= 0 or lines >= total.
func (m *Manager) ReadFileLines(path string, lines int) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return []string{}, nil
		}
		return nil, err
	}
	defer f.Close()

	var result []string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		result = append(result, scanner.Text())
	}

	if lines <= 0 || lines >= len(result) {
		return result, nil
	}

	return result[len(result)-lines:], nil
}

// Purge removes the log file for the given service.
func (m *Manager) Purge(serviceName string) error {
	logPath := filepath.Join(m.baseDir, serviceName+".log")
	return os.Remove(logPath)
}

// ClearFile truncates the file at path to zero length.
func (m *Manager) ClearFile(path string) error {
	if path == "" {
		return fmt.Errorf("log path is empty")
	}
	return os.Truncate(path, 0)
}

// PurgeAll removes every *.log file under baseDir.
func (m *Manager) PurgeAll() error {
	entries, err := os.ReadDir(m.baseDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".log") {
			os.Remove(filepath.Join(m.baseDir, entry.Name()))
		}
	}

	return nil
}
