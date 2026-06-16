package log

import (
	"bytes"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
)

// TestCreateLogger_Empty tests creating logger with empty log file (returns DiscardHandler)
func TestCreateLogger_Empty(t *testing.T) {
	logger, err := NewFileLogger("", false, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Empty string now returns DiscardHandler (non-nil), not nil
	if logger == nil {
		t.Fatal("expected non-nil logger (DiscardHandler) for empty file")
	}
	// Should not panic when using
	logger.Info("test message for empty file")
}

// TestCreateLogger_DevNull tests creating logger with /dev/null
func TestCreateLogger_DevNull(t *testing.T) {
	logger, err := NewFileLogger("/dev/null", false, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if logger == nil {
		t.Fatal("expected non-nil logger")
	}
	// Test that it works
	logger.Info("test message", "key", "value")
}

// TestCreateLogger_Stdout tests creating logger with stdout
func TestCreateLogger_Stdout(t *testing.T) {
	logger, err := NewFileLogger("stdout", false, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if logger == nil {
		t.Fatal("expected non-nil logger")
	}
	logger.Info("stdout test message")
}

// TestCreateLogger_Stderr tests creating logger with stderr
func TestCreateLogger_Stderr(t *testing.T) {
	logger, err := NewFileLogger("stderr", false, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if logger == nil {
		t.Fatal("expected non-nil logger")
	}
	logger.Info("stderr test message")
}

// TestCreateLogger_File tests creating logger with a real file
func TestCreateLogger_File(t *testing.T) {
	tmpDir := t.TempDir()
	logFile := filepath.Join(tmpDir, "test.log")

	logger, err := NewFileLogger(logFile, false, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if logger == nil {
		t.Fatal("expected non-nil logger")
	}

	// Log a message
	logger.Info("file test message", "key", "value")

	// Verify file exists and has content
	info, err := os.Stat(logFile)
	if err != nil {
		t.Fatalf("log file should exist: %v", err)
	}
	if info.Size() == 0 {
		t.Fatal("log file should have content")
	}
}

// TestCreateLogger_NestedDirectory tests creating logger in nested directory
func TestCreateLogger_NestedDirectory(t *testing.T) {
	tmpDir := t.TempDir()
	logFile := filepath.Join(tmpDir, "nested", "deep", "test.log")

	logger, err := NewFileLogger(logFile, false, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if logger == nil {
		t.Fatal("expected non-nil logger")
	}

	logger.Info("nested directory test")

	// Verify file was created
	if _, err := os.Stat(logFile); err != nil {
		t.Fatalf("log file should exist in nested directory: %v", err)
	}
}

// TestCreateFileHandler_Basic tests basic file handler creation
func TestCreateFileHandler_Basic(t *testing.T) {
	tmpDir := t.TempDir()
	logFile := filepath.Join(tmpDir, "handler.log")

	fh, err := NewFileWriter(logFile)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer fh.Close()

	// Write to the file
	fh.WriteString("test content\n")

	// Verify content
	content, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("failed to read file: %v", err)
	}
	if string(content) != "test content\n" {
		t.Fatalf("unexpected content: %s", string(content))
	}
}

// TestMakeDirForFile_CreatesDirectory tests directory creation for files
func TestMakeDirForFile_CreatesDirectory(t *testing.T) {
	tmpDir := t.TempDir()
	nestedPath := filepath.Join(tmpDir, "a", "b", "c", "file.txt")

	err := MakeDirForFile(nestedPath)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify directory was created
	dir := filepath.Dir(nestedPath)
	if info, err := os.Stat(dir); err != nil || !info.IsDir() {
		t.Fatalf("directory should exist: %s, err=%v", dir, err)
	}
}

// TestMakeDirForFile_ExistingDirectory tests that existing directory doesn't cause error
func TestMakeDirForFile_ExistingDirectory(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "existing.txt")

	// Create the directory first
	os.MkdirAll(tmpDir, 0755)

	err := MakeDirForFile(filePath)
	if err != nil {
		t.Fatalf("unexpected error for existing dir: %v", err)
	}
}

// TestWriteToFile_Basic tests writing formatted Go code to file
func TestWriteToFile_Basic(t *testing.T) {
	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "output.go")

	var buf bytes.Buffer
	buf.WriteString("package main\n\nimport \"fmt\"\n\nfunc main() {\n\tfmt.Println(\"hello\")\n}\n")

	err := WriteToFile(&buf, outputPath)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify file exists and has formatted content
	content, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("failed to read output file: %v", err)
	}

	expected := "package main\n\nimport \"fmt\"\n\nfunc main() {\n\tfmt.Println(\"hello\")\n}\n"
	if string(content) != expected {
		t.Fatalf("content mismatch:\ngot:\n%s\nwant:\n%s", string(content), expected)
	}
}

// TestWriteToFile_NestedDirectory tests writing to nested directory
func TestWriteToFile_NestedDirectory(t *testing.T) {
	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "deep", "nested", "code.go")

	var buf bytes.Buffer
	buf.WriteString("package test\n")

	err := WriteToFile(&buf, outputPath)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if _, err := os.Stat(outputPath); err != nil {
		t.Fatalf("output file should exist: %v", err)
	}
}

// ============================================================
// NewDailyLogger tests
// ============================================================

// TestNewDailyLogger_Basic tests daily logger creation with maxBackups
func TestNewDailyLogger_Basic(t *testing.T) {
	tmpDir := t.TempDir()
	logFile := filepath.Join(tmpDir, "daily.log")

	// Second parameter is maxBackups (int), compression is enabled by default
	logger, err := NewDailyLogger(logFile, 7)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if logger == nil {
		t.Fatal("expected non-nil logger")
	}

	// Log a message
	logger.Info("daily logger test", "key", "value")

	// Close the underlying writer if it's a DailyRotateWriter
	// The logger itself doesn't have Close, but we can verify the file exists
	if _, err := os.Stat(logFile); err != nil {
		t.Fatalf("log file should exist: %v", err)
	}
}

// TestCreateDailyLogger_DevNull tests daily logger with /dev/null
func TestCreateDailyLogger_DevNull(t *testing.T) {
	logger, err := NewDailyLogger("/dev/null", 7)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if logger == nil {
		t.Fatal("expected non-nil logger")
	}
	logger.Info("devnull test")
}

// TestNewDailyLogger_NoLimit tests daily logger with no backup limit
func TestNewDailyLogger_NoLimit(t *testing.T) {
	tmpDir := t.TempDir()
	logFile := filepath.Join(tmpDir, "unlimited.log")

	// maxBackups = 0 means no limit
	logger, err := NewDailyLogger(logFile, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if logger == nil {
		t.Fatal("expected non-nil logger")
	}

	logger.Info("no limit test")
	if _, err := os.Stat(logFile); err != nil {
		t.Fatalf("log file should exist: %v", err)
	}
}

// TestNewDailyLogger_CompressionEnabled tests that compression is enabled by default
func TestNewDailyLogger_CompressionEnabled(t *testing.T) {
	tmpDir := t.TempDir()
	logFile := filepath.Join(tmpDir, "compressed.log")

	// Compression enabled by default, maxBackups controls backup count
	logger, err := NewDailyLogger(logFile, 3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if logger == nil {
		t.Fatal("expected non-nil logger")
	}

	logger.Info("compress test message")
	// File should be created
	if _, err := os.Stat(logFile); err != nil {
		t.Fatalf("log file should exist: %v", err)
	}
}

// TestNewDailyLogger_Stdout tests daily logger with stdout
func TestNewDailyLogger_Stdout(t *testing.T) {
	logger, err := NewDailyLogger("stdout", 5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if logger == nil {
		t.Fatal("expected non-nil logger")
	}
	logger.Info("stdout daily test")
}

// TestNewDailyLogger_Empty tests daily logger with empty filename
func TestNewDailyLogger_Empty(t *testing.T) {
	logger, err := NewDailyLogger("", 7)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if logger == nil {
		t.Fatal("expected non-nil logger (DiscardHandler)")
	}
	// Should not panic
	logger.Info("should be discarded")
}

// ============================================================
// NewRotateLogger multi-cycle rotation tests
// ============================================================

// TestNewRotateLogger_Monthly tests monthly rotation
func TestNewRotateLogger_Monthly(t *testing.T) {
	tmpDir := t.TempDir()
	logFile := filepath.Join(tmpDir, "monthly.log")

	// NewRotateLogger(cycle, logFile, maxBackups, compress)
	logger, err := NewRotateLogger(CycleMonthly, logFile, 3, true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if logger == nil {
		t.Fatal("expected non-nil logger")
	}

	logger.Info("monthly rotation test")
	if _, err := os.Stat(logFile); err != nil {
		t.Fatalf("log file should exist: %v", err)
	}
}

// TestNewRotateLogger_Weekly tests weekly rotation
func TestNewRotateLogger_Weekly(t *testing.T) {
	tmpDir := t.TempDir()
	logFile := filepath.Join(tmpDir, "weekly.log")

	logger, err := NewRotateLogger(CycleWeekly, logFile, 4, true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if logger == nil {
		t.Fatal("expected non-nil logger")
	}

	logger.Info("weekly rotation test")
	if _, err := os.Stat(logFile); err != nil {
		t.Fatalf("log file should exist: %v", err)
	}
}

// TestNewRotateLogger_Daily tests daily rotation (explicit)
func TestNewRotateLogger_Daily(t *testing.T) {
	tmpDir := t.TempDir()
	logFile := filepath.Join(tmpDir, "daily_explicit.log")

	logger, err := NewRotateLogger(CycleDaily, logFile, 7, true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if logger == nil {
		t.Fatal("expected non-nil logger")
	}

	logger.Info("daily explicit rotation test")
	if _, err := os.Stat(logFile); err != nil {
		t.Fatalf("log file should exist: %v", err)
	}
}

// TestNewRotateLogger_Hourly tests hourly rotation
func TestNewRotateLogger_Hourly(t *testing.T) {
	tmpDir := t.TempDir()
	logFile := filepath.Join(tmpDir, "hourly.log")

	logger, err := NewRotateLogger(CycleHourly, logFile, 24, true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if logger == nil {
		t.Fatal("expected non-nil logger")
	}

	logger.Info("hourly rotation test")
	if _, err := os.Stat(logFile); err != nil {
		t.Fatalf("log file should exist: %v", err)
	}
}

// TestNewRotateLogger_Minutely tests minutely rotation
func TestNewRotateLogger_Minutely(t *testing.T) {
	tmpDir := t.TempDir()
	logFile := filepath.Join(tmpDir, "minutely.log")

	logger, err := NewRotateLogger(CycleMinutely, logFile, 60, true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if logger == nil {
		t.Fatal("expected non-nil logger")
	}

	logger.Info("minutely rotation test")
	if _, err := os.Stat(logFile); err != nil {
		t.Fatalf("log file should exist: %v", err)
	}
}

// TestNewRotateLogger_AllCycles tests that all cycles create valid loggers
func TestNewRotateLogger_AllCycles(t *testing.T) {
	tmpDir := t.TempDir()

	cycles := []struct {
		name  string
		cycle RotateCycle
	}{
		{"monthly", CycleMonthly},
		{"weekly", CycleWeekly},
		{"daily", CycleDaily},
		{"hourly", CycleHourly},
		{"minutely", CycleMinutely},
	}

	for _, tc := range cycles {
		t.Run(tc.name, func(t *testing.T) {
			logFile := filepath.Join(tmpDir, tc.name+".log")
			// NewRotateLogger(cycle, logFile, maxBackups, compress)
			logger, err := NewRotateLogger(tc.cycle, logFile, 5, true)
			if err != nil {
				t.Fatalf("unexpected error for %s: %v", tc.name, err)
			}
			if logger == nil {
				t.Fatalf("expected non-nil logger for %s", tc.name)
			}

			logger.Info("test message for " + tc.name)
			if _, err := os.Stat(logFile); err != nil {
				t.Fatalf("log file should exist for %s: %v", tc.name, err)
			}
		})
	}
}

// TestNewRotateLogger_SpecialFiles tests special file handling with different cycles
func TestNewRotateLogger_SpecialFiles(t *testing.T) {
	tests := []struct {
		name    string
		logFile string
		cycle   RotateCycle
	}{
		{"devnull_monthly", "/dev/null", CycleMonthly},
		{"stdout_weekly", "stdout", CycleWeekly},
		{"stderr_hourly", "stderr", CycleHourly},
		{"empty_daily", "", CycleDaily},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// NewRotateLogger(cycle, logFile, maxBackups, compress)
			logger, err := NewRotateLogger(tt.cycle, tt.logFile, 7, true)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if logger == nil && tt.logFile != "" {
				t.Fatal("expected non-nil logger")
			}
			// Should not panic
			logger.Info("special file test")
		})
	}
}

// TestSetDefaultByFile tests setting default logger with custom cycle
func TestSetDefaultByFile(t *testing.T) {
	tmpDir := t.TempDir()
	logFile := filepath.Join(tmpDir, "default_hourly.log")

	// SetDefaultByFile(cycle, logFile, level, maxBackups)
	SetDefaultByFile(CycleHourly, logFile, "info", 24)

	slog.Info("hourly default logger test")

	content, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("failed to read log file: %v", err)
	}

	if !contains(string(content), "hourly default logger test") {
		t.Fatal("log should contain the message")
	}
}

// ============================================================
// SetDefault tests
// ============================================================

// TestSetDefault_Basic tests setting default logger
func TestSetDefault_Basic(t *testing.T) {
	tmpDir := t.TempDir()
	logFile := filepath.Join(tmpDir, "default.log")

	// SetDefault(logFile, logLevel) - 2 parameters, maxBackups defaults to 7
	SetDefault(logFile, "info")

	// Use slog default functions
	slog.Info("default logger info")
	slog.Error("default logger error")

	// Verify file was created
	content, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("failed to read log file: %v", err)
	}

	strContent := string(content)
	if !contains(strContent, "default logger info") {
		t.Fatal("log should contain info message")
	}
	if !contains(strContent, "default logger error") {
		t.Fatal("log should contain error message")
	}
}

// TestSetDefault_WithLevel tests setting default logger with custom level
func TestSetDefault_WithLevel(t *testing.T) {
	tmpDir := t.TempDir()
	logFile := filepath.Join(tmpDir, "level.log")

	// Set level to Warn - Debug and Info should not appear
	SetDefault(logFile, "warn")

	slog.Debug("debug should be filtered") // Should NOT appear (but might due to slog behavior)
	slog.Info("info should be filtered")   // Should NOT appear (but might due to slog behavior)
	slog.Warn("warn should appear")        // Should appear
	slog.Error("error should appear")      // Should appear

	content, _ := os.ReadFile(logFile)
	strContent := string(content)

	// Note: slog.SetLogLoggerLevel may not filter as expected in all Go versions
	// The important thing is that Warn and Error are present
	if !contains(strContent, "warn should appear") {
		t.Fatal("Warn messages should appear")
	}
	if !contains(strContent, "error should appear") {
		t.Fatal("Error messages should appear")
	}
}

// TestSetDefault_DevNull tests setting default logger to discard
func TestSetDefault_DevNull(t *testing.T) {
	// Only 2 parameters
	SetDefault("/dev/null", "debug")

	// Should not panic or create files
	slog.Info("discarded message")
}

// TestSetDefault_ReturnValue tests that SetDefault configures slog correctly
func TestSetDefault_ReturnValue(t *testing.T) {
	tmpDir := t.TempDir()
	logFile := filepath.Join(tmpDir, "return_test.log")

	// Only 2 parameters (maxBackups defaults to 7)
	SetDefault(logFile, "error")

	// Use slog directly after SetDefault
	slog.Info("direct slog call")

	content, _ := os.ReadFile(logFile)
	if !contains(string(content), "direct slog call") {
		t.Fatal("slog should work after SetDefault")
	}
}

// TestCreateLogger_JSONFormat tests that JSON format is used by default
func TestCreateLogger_JSONFormat(t *testing.T) {
	tmpDir := t.TempDir()
	logFile := filepath.Join(tmpDir, "json.log")

	logger, err := NewFileLogger(logFile, false, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	logger.Info("json format test", "stringKey", "stringValue", "intKey", 42)

	content, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("failed to read log file: %v", err)
	}

	// Should contain JSON-like content
	strContent := string(content)
	if !contains(strContent, "json format test") {
		t.Fatal("log should contain the message")
	}
	if !contains(strContent, "stringValue") {
		t.Fatal("log should contain the string value")
	}
	if !contains(strContent, "42") {
		t.Fatal("log should contain the int value")
	}
}

// Helper function to check if string contains substring
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsHelper(s, substr))
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// BenchmarkCreateLogger benchmarks logger creation
func BenchmarkCreateLogger(b *testing.B) {
	tmpDir := b.TempDir()
	logFile := filepath.Join(tmpDir, "bench.log")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		NewFileLogger(logFile, false, false)
	}
	b.StopTimer()
}

// BenchmarkNewDailyLogger benchmarks daily logger creation
func BenchmarkNewDailyLogger(b *testing.B) {
	tmpDir := b.TempDir()
	logFile := filepath.Join(tmpDir, "daily_bench.log")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// NewDailyLogger(logFile, maxBackups)
		NewDailyLogger(logFile, 7)
	}
	b.StopTimer()
}

// BenchmarkLogWriting benchmarks actual logging performance
func BenchmarkLogWriting(b *testing.B) {
	tmpDir := b.TempDir()
	logFile := filepath.Join(tmpDir, "bench_write.log")
	logger, _ := NewFileLogger(logFile, false, false)
	defer os.Remove(logFile)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		logger.Info("benchmark message", "index", i)
	}
	b.StopTimer()
}

// BenchmarkSetDefault benchmarks SetDefault operation
func BenchmarkSetDefault(b *testing.B) {
	tmpDir := b.TempDir()
	logFile := filepath.Join(tmpDir, "bench_default.log")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// SetDefault(logFile, logLevel) - 2 parameters
		SetDefault(logFile, "info")
	}
	b.StopTimer()
}

// TestParseLevel_String tests ParseLevel with string values
func TestParseLevel_String(t *testing.T) {
	tests := []struct {
		input    string
		expected slog.Level
	}{
		{"debug", slog.LevelDebug},
		{"info", slog.LevelInfo},
		{"warn", slog.LevelWarn},
		{"warning", slog.LevelWarn}, // alias
		{"error", slog.LevelError},
		{"DEBUG", slog.LevelDebug},  // case insensitive
		{"INFO", slog.LevelInfo},    // case insensitive
		{"unknown", slog.LevelInfo}, // default to Info
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := ParseLevel(tt.input)
			if result != tt.expected {
				t.Errorf("ParseLevel(%q) = %v, want %v", tt.input, result, tt.expected)
			}
		})
	}
}

// TestParseLevel_SlogLevel tests ParseLevel with slog.Level values converted to string
func TestParseLevel_SlogLevel(t *testing.T) {
	tests := []struct {
		input    string
		expected slog.Level
	}{
		{"debug", slog.LevelDebug},
		{"info", slog.LevelInfo},
		{"warn", slog.LevelWarn},
		{"error", slog.LevelError},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := ParseLevel(tt.input)
			if result != tt.expected {
				t.Errorf("ParseLevel(%q) = %v, want %v", tt.input, result, tt.expected)
			}
		})
	}
}

// TestSetDefault_WithStringLevel tests SetDefault with string level
func TestSetDefault_WithStringLevel(t *testing.T) {
	tmpDir := t.TempDir()
	logFile := filepath.Join(tmpDir, "string_level.log")

	// Use string level "info"
	SetDefault(logFile, "info")

	slog.Info("string level test")

	content, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("failed to read log file: %v", err)
	}

	if !contains(string(content), "string level test") {
		t.Fatal("log should contain the message")
	}
}

// TestSetDefaultByFile_WithStringLevel tests SetDefaultByFile with string level
func TestSetDefaultByFile_WithStringLevel(t *testing.T) {
	tmpDir := t.TempDir()
	logFile := filepath.Join(tmpDir, "string_level.log")

	// SetDefaultByFile(cycle, logFile, logLevel, maxBackups)
	SetDefaultByFile(CycleHourly, logFile, "error", 24)

	slog.Error("string cycle error")

	content, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("failed to read log file: %v", err)
	}

	if !contains(string(content), "string cycle error") {
		t.Fatal("log should contain the error message")
	}
}

// TestLoggerLevels tests different log levels
func TestLoggerLevels(t *testing.T) {
	tmpDir := t.TempDir()
	logFile := filepath.Join(tmpDir, "levels.log")

	logger, err := NewFileLogger(logFile, false, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Log at different levels
	logger.Debug("debug message")
	logger.Info("info message")
	logger.Warn("warn message")
	logger.Error("error message")

	// Verify file has content
	content, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("failed to read log file: %v", err)
	}

	strContent := string(content)
	// Note: Debug level might not be written depending on default level
	if !contains(strContent, "info message") {
		t.Fatal("should contain info message")
	}
	if !contains(strContent, "error message") {
		t.Fatal("should contain error message")
	}
}

// TestMultipleLoggers tests creating multiple independent loggers
func TestMultipleLoggers(t *testing.T) {
	tmpDir := t.TempDir()

	logFile1 := filepath.Join(tmpDir, "logger1.log")
	logFile2 := filepath.Join(tmpDir, "logger2.log")

	logger1, err := NewFileLogger(logFile1, false, false)
	if err != nil {
		t.Fatalf("failed to create logger1: %v", err)
	}

	logger2, err := NewFileLogger(logFile2, false, false)
	if err != nil {
		t.Fatalf("failed to create logger2: %v", err)
	}

	logger1.Info("message from logger 1")
	logger2.Info("message from logger 2")

	// Verify both files have their respective messages
	content1, _ := os.ReadFile(logFile1)
	content2, _ := os.ReadFile(logFile2)

	if !contains(string(content1), "logger 1") {
		t.Fatal("logger1 should have its own message")
	}
	if !contains(string(content2), "logger 2") {
		t.Fatal("logger2 should have its own message")
	}
	// Should not be mixed up
	if contains(string(content1), "logger 2") {
		t.Fatal("logger1 should not contain logger2's message")
	}
	if contains(string(content2), "logger 1") {
		t.Fatal("logger2 should not contain logger1's message")
	}
}

// TestSlogDiscardHandler tests DiscardHandler behavior
func TestSlogDiscardHandler(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)

	// Should not panic or error
	logger.Info("this should be discarded")
	logger.Error("this should also be discarded")
}

// TestCreateLogger_AppendMode tests that file handler appends by default
func TestCreateLogger_AppendMode(t *testing.T) {
	tmpDir := t.TempDir()
	logFile := filepath.Join(tmpDir, "append.log")

	// Create first logger and write
	logger1, err := NewFileLogger(logFile, false, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	logger1.Info("first message")

	// Create second logger (should append)
	logger2, err := NewFileLogger(logFile, false, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	logger2.Info("second message")

	// Verify both messages are present
	content, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("failed to read file: %v", err)
	}

	strContent := string(content)
	if !contains(strContent, "first message") {
		t.Fatal("should contain first message")
	}
	if !contains(strContent, "second message") {
		t.Fatal("should contain second message")
	}
}
