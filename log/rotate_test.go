package log

import (
	"compress/gzip"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

// TestRotateCycle_Layout tests that each cycle returns the correct time layout
func TestRotateCycle_Layout(t *testing.T) {
	tests := []struct {
		cycle  RotateCycle
		layout string
	}{
		{CycleMonthly, "200601"},
		{CycleWeekly, "20060102"},
		{CycleDaily, "20060102"},
		{CycleHourly, "2006010215"},
		{CycleMinutely, "20060102-1504"},
	}

	for _, tt := range tests {
		if got := tt.cycle.layout(); got != tt.layout {
			t.Errorf("RotateCycle(%d).layout() = %v, want %v", tt.cycle, got, tt.layout)
		}
	}
}

// TestRotateCycle_PeriodLabel tests period label generation
func TestRotateCycle_PeriodLabel(t *testing.T) {
	now := time.Date(2026, 1, 15, 14, 30, 45, 0, time.UTC)

	tests := []struct {
		cycle     RotateCycle
		wantLabel string
	}{
		{CycleMonthly, "202601"},
		{CycleWeekly, "20260115"},
		{CycleDaily, "20260115"},
		{CycleHourly, "2026011514"},
		{CycleMinutely, "20260115-1430"},
	}

	for _, tt := range tests {
		if got := tt.cycle.periodLabel(now); got != tt.wantLabel {
			t.Errorf("RotateCycle(%d).periodLabel() = %v, want %v", tt.cycle, got, tt.wantLabel)
		}
	}
}

// TestNewDailyRotateWriter tests creating a daily rotate writer
func TestNewDailyRotateWriter(t *testing.T) {
	tmpDir := t.TempDir()
	logFile := filepath.Join(tmpDir, "test.log")

	w := NewDailyRotateWriter(logFile, 3, 7, true) // maxBackups=3, maxAge=7
	defer w.Close()

	if w.Filename != logFile {
		t.Errorf("Filename = %v, want %v", w.Filename, logFile)
	}
	if w.Cycle != CycleDaily {
		t.Errorf("Cycle = %v, want %v", w.Cycle, CycleDaily)
	}
	if w.MaxBackups != 3 {
		t.Errorf("MaxBackups = %v, want %v", w.MaxBackups, 3)
	}
	if w.MaxAge != 7 {
		t.Errorf("MaxAge = %v, want %v", w.MaxAge, 7)
	}
	if !w.Compress {
		t.Error("Compress should be true")
	}
}

// TestNewPresetRotateWriter tests preset rotate writer with defaults
func TestNewPresetRotateWriter(t *testing.T) {
	tmpDir := t.TempDir()
	logFile := filepath.Join(tmpDir, "preset.log")

	w := NewDailyRotateWriter(logFile, 7, 7, false)
	defer w.Close()

	// Should have default values: maxAge=7, maxBackups=7
	if w.MaxAge != 7 {
		t.Errorf("MaxAge should default to 7, got %d", w.MaxAge)
	}
	if w.MaxBackups != 7 {
		t.Errorf("MaxBackups should default to 7, got %d", w.MaxBackups)
	}
	if w.Compress {
		t.Error("Compress should be false when not specified")
	}
}

// TestRotateWriter_Write tests basic write functionality
func TestRotateWriter_Write(t *testing.T) {
	tmpDir := t.TempDir()
	logFile := filepath.Join(tmpDir, "write.log")

	w := &RotateWriter{
		Filename: logFile,
		Cycle:    CycleDaily,
	}
	defer w.Close()

	testData := []byte("hello world\n")
	n, err := w.Write(testData)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if n != len(testData) {
		t.Errorf("Write returned %d bytes, want %d", n, len(testData))
	}

	// Verify file content
	content, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}
	if string(content) != string(testData) {
		t.Errorf("Content = %q, want %q", string(content), string(testData))
	}
}

// TestRotateWriter_MultipleWrites tests multiple sequential writes
func TestRotateWriter_MultipleWrites(t *testing.T) {
	tmpDir := t.TempDir()
	logFile := filepath.Join(tmpDir, "multi.log")

	w := &RotateWriter{
		Filename: logFile,
		Cycle:    CycleDaily,
	}
	defer w.Close()

	messages := []string{
		"first line\n",
		"second line\n",
		"third line\n",
	}

	for _, msg := range messages {
		if _, err := w.Write([]byte(msg)); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}

	content, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}

	expected := strings.Join(messages, "")
	if string(content) != expected {
		t.Errorf("Content = %q, want %q", string(content), expected)
	}
}

// TestRotateWriter_Rotate tests forced rotation
func TestRotateWriter_Rotate(t *testing.T) {
	tmpDir := t.TempDir()
	logFile := filepath.Join(tmpDir, "rotate.log")

	w := &RotateWriter{
		Filename: logFile,
		Cycle:    CycleDaily,
	}
	defer w.Close()

	// Write initial content
	w.Write([]byte("before rotation\n"))

	// Force rotation
	if err := w.Rotate(); err != nil {
		t.Fatalf("Rotate failed: %v", err)
	}

	// Write new content after rotation
	w.Write([]byte("after rotation\n"))

	// The original file should now contain only the new content
	content, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}
	if string(content) != "after rotation\n" {
		t.Errorf("After rotation, content = %q, want %q", string(content), "after rotation\n")
	}

	// A backup file should exist
	dirEntries, err := os.ReadDir(tmpDir)
	if err != nil {
		t.Fatalf("Failed to read directory: %v", err)
	}

	foundBackup := false
	for _, entry := range dirEntries {
		name := entry.Name()
		if name != "rotate.log" && strings.HasPrefix(name, "rotate-") {
			foundBackup = true
			break
		}
	}
	if !foundBackup {
		t.Fatal("Expected backup file to be created after rotation")
	}
}

// TestBackupName tests backup filename generation
func TestBackupName(t *testing.T) {
	tests := []struct {
		name       string
		timeFormat string
		last       time.Time
		want       string
	}{
		{
			name:       "/var/log/app.log",
			timeFormat: "20060102",
			last:       time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC),
			want:       "/var/log/app-20260115.log",
		},
		{
			name:       "/tmp/test.txt",
			timeFormat: "20060102-150405",
			last:       time.Date(2026, 6, 16, 10, 30, 45, 0, time.UTC),
			want:       "/tmp/test-20260616-103045.txt",
		},
		{
			name:       "simple.log",
			timeFormat: "200601",
			last:       time.Date(2026, 12, 25, 0, 0, 0, 0, time.UTC),
			want:       "simple-202612.log",
		},
	}

	for _, tt := range tests {
		got := backupName(tt.name, tt.timeFormat, tt.last)
		if got != tt.want {
			t.Errorf("backupName(%q, %q, ...) = %q, want %q", tt.name, tt.timeFormat, got, tt.want)
		}
	}
}

// TestCompressLogFile tests gzip compression functionality
func TestCompressLogFile(t *testing.T) {
	tmpDir := t.TempDir()
	srcFile := filepath.Join(tmpDir, "original.log")
	dstFile := filepath.Join(tmpDir, "compressed.log.gz")

	// Create original file with test data
	originalContent := []byte("This is test content for compression\nLine 2\nLine 3\n")
	if err := os.WriteFile(srcFile, originalContent, 0644); err != nil {
		t.Fatalf("Failed to create source file: %v", err)
	}

	// Compress the file
	if err := compressLogFile(srcFile, dstFile); err != nil {
		t.Fatalf("compressLogFile failed: %v", err)
	}

	// Verify compressed file exists
	if _, err := os.Stat(dstFile); err != nil {
		t.Fatal("Compressed file should exist")
	}

	// Verify original file was removed
	if _, err := os.Stat(srcFile); !os.IsNotExist(err) {
		t.Fatal("Original file should be removed after compression")
	}

	// Read and decompress to verify content
	f, err := os.Open(dstFile)
	if err != nil {
		t.Fatalf("Failed to open compressed file: %v", err)
	}
	defer f.Close()

	gzReader, err := gzip.NewReader(f)
	if err != nil {
		t.Fatalf("Failed to create gzip reader: %v", err)
	}
	defer gzReader.Close()

	decompressed, err := io.ReadAll(gzReader)
	if err != nil {
		t.Fatalf("Failed to decompress: %v", err)
	}

	if string(decompressed) != string(originalContent) {
		t.Errorf("Decompressed content = %q, want %q", string(decompressed), string(originalContent))
	}
}

// TestTimeFromName tests parsing timestamps from backup filenames
func TestTimeFromName(t *testing.T) {
	w := &RotateWriter{
		Filename: "/var/log/app.log",
		Cycle:    CycleDaily,
	}

	tests := []struct {
		filename string
		prefix   string
		ext      string
		wantErr  bool
	}{
		{
			filename: "app-20260116.log",
			prefix:   "app-",
			ext:      ".log",
			wantErr:  false,
		},
		{
			filename: "app-20260116-143045.log",
			prefix:   "app-",
			ext:      ".log",
			wantErr:  false,
		},
		{
			filename: "wrong-prefix.log",
			prefix:   "app-",
			ext:      ".log",
			wantErr:  true,
		},
		{
			filename: "app-20260116.txt",
			prefix:   "app-",
			ext:      ".log",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		_, err := w.timeFromName(tt.filename, tt.prefix, tt.ext)
		if (err != nil) != tt.wantErr {
			t.Errorf("timeFromName(%q) error = %v, wantErr %v", tt.filename, err, tt.wantErr)
		}
	}
}

// TestOldLogFiles tests listing and sorting of old log files
func TestOldLogFiles(t *testing.T) {
	tmpDir := t.TempDir()
	logFile := filepath.Join(tmpDir, "test.log")

	w := &RotateWriter{
		Filename: logFile,
		Cycle:    CycleDaily,
	}

	// Create some backup files with different timestamps
	backups := []struct {
		name    string
		content string
		modTime time.Time
	}{
		{"test-20260110.log", "old log 1\n", time.Date(2026, 1, 10, 0, 0, 0, 0, time.UTC)},
		{"test-20260115.log", "recent log\n", time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC)},
		{"test-20260112.log", "middle log\n", time.Date(2026, 1, 12, 0, 0, 0, 0, time.UTC)},
	}

	for _, b := range backups {
		path := filepath.Join(tmpDir, b.name)
		if err := os.WriteFile(path, []byte(b.content), 0644); err != nil {
			t.Fatalf("Failed to create backup %s: %v", b.name, err)
		}
		os.Chtimes(path, b.modTime, b.modTime)
	}

	files, err := w.oldLogFiles()
	if err != nil {
		t.Fatalf("oldLogFiles failed: %v", err)
	}

	// Should find all 3 files
	if len(files) != 3 {
		t.Errorf("Expected 3 files, got %d", len(files))
	}

	// Files should be sorted by timestamp descending (newest first)
	if len(files) >= 2 && files[0].timestamp.Before(files[1].timestamp) {
		t.Error("Files should be sorted by timestamp descending")
	}
}

// TestMaxBackups_Cleanup tests automatic cleanup of old backups
func TestMaxBackups_Cleanup(t *testing.T) {
	tmpDir := t.TempDir()
	logFile := filepath.Join(tmpDir, "cleanup.log")

	w := &RotateWriter{
		Filename:   logFile,
		Cycle:      CycleDaily,
		MaxBackups: 2,
	}
	defer w.Close()

	// Create more than MaxBackups backup files
	for i := 0; i < 5; i++ {
		date := time.Date(2026, 1, 10+i, 0, 0, 0, 0, time.UTC)
		backupPath := filepath.Join(tmpDir, fmt.Sprintf("cleanup-%s.log", date.Format("20060102")))
		os.WriteFile(backupPath, []byte(fmt.Sprintf("log %d\n", i)), 0644)
		os.Chtimes(backupPath, date, date)
	}

	// Trigger cleanup
	w.millRunOnce()

	// Should only have 2 backup files remaining
	files, _ := w.oldLogFiles()
	if len(files) > 2 {
		t.Errorf("Expected at most 2 files after cleanup, got %d", len(files))
	}
}

// TestMaxAge_Cleanup tests age-based cleanup
func TestMaxAge_Cleanup(t *testing.T) {
	// Save current time function and restore it later
	origTime := currentTime
	defer func() { currentTime = origTime }()

	now := time.Date(2026, 6, 20, 0, 0, 0, 0, time.UTC)
	currentTime = func() time.Time { return now }

	tmpDir := t.TempDir()
	logFile := filepath.Join(tmpDir, "age.log")

	w := &RotateWriter{
		Filename: logFile,
		Cycle:    CycleDaily,
		MaxAge:   7, // Keep only 7 days
	}
	defer w.Close()

	// Create files older than 7 days and newer than 7 days
	oldDate := now.Add(-8 * 24 * time.Hour)
	newDate := now.Add(-3 * 24 * time.Hour)

	oldPath := filepath.Join(tmpDir, fmt.Sprintf("age-%s.log", oldDate.Format("20060102")))
	newPath := filepath.Join(tmpDir, fmt.Sprintf("age-%s.log", newDate.Format("20060102")))

	os.WriteFile(oldPath, []byte("old log\n"), 0644)
	os.Chtimes(oldPath, oldDate, oldDate)

	os.WriteFile(newPath, []byte("new log\n"), 0644)
	os.Chtimes(newPath, newDate, newDate)

	// Trigger cleanup
	w.millRunOnce()

	// Old file should be deleted, new file should remain
	if _, err := os.Stat(oldPath); !os.IsNotExist(err) {
		t.Error("Old log file should be cleaned up")
	}
	if _, err := os.Stat(newPath); os.IsNotExist(err) {
		t.Error("New log file should still exist")
	}
}

// TestByFormatTime_Sort tests the sorting implementation
func TestByFormatTime_Sort(t *testing.T) {
	times := []time.Time{
		time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 1, 3, 0, 0, 0, 0, time.UTC),
	}

	var logs byFormatTime
	for _, t := range times {
		logs = append(logs, logInfo{timestamp: t})
	}

	sort.Sort(logs)

	// After sorting, should be in descending order
	for i := 1; i < len(logs); i++ {
		if logs[i-1].timestamp.Before(logs[i].timestamp) {
			t.Error("byFormatTime should sort in descending order")
		}
	}
}

// TestDifferentRotationCycles tests different rotation cycles work correctly
func TestDifferentRotationCycles(t *testing.T) {
	tmpDir := t.TempDir()

	cycles := []RotateCycle{CycleHourly, CycleMinutely}

	for _, cycle := range cycles {
		logFile := filepath.Join(tmpDir, fmt.Sprintf("cycle%d_test.log", int(cycle)))

		w := &RotateWriter{
			Filename: logFile,
			Cycle:    cycle,
		}

		_, err := w.Write([]byte("test data"))
		w.Close()

		if err != nil {
			t.Errorf("Write failed for cycle %v: %v", cycle, err)
		}

		// Verify file was created
		if _, err := os.Stat(logFile); err != nil {
			t.Errorf("File should exist for cycle %v: %v", cycle, err)
		}
	}
}

// BenchmarkRotateWriter_Write benchmarks rotation writer write performance
func BenchmarkRotateWriter_Write(b *testing.B) {
	tmpDir := b.TempDir()
	logFile := filepath.Join(tmpDir, "bench.log")

	w := &RotateWriter{
		Filename: logFile,
		Cycle:    CycleDaily,
	}
	defer w.Close()

	data := []byte("benchmark log entry with some data\n")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w.Write(data)
	}
	b.StopTimer()
}

// BenchmarkRotateWriter_Rotate benchmarks rotation operation
func BenchmarkRotateWriter_Rotate(b *testing.B) {
	tmpDir := b.TempDir()
	logFile := filepath.Join(tmpDir, "bench_rotate.log")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w := &RotateWriter{
			Filename: logFile,
			Cycle:    CycleDaily,
		}
		w.Write([]byte("data before rotation"))
		w.Rotate()
		w.Close()
	}
	b.StopTimer()
}

// TestConcurrentWrites tests concurrent write safety
func TestConcurrentWrites(t *testing.T) {
	tmpDir := t.TempDir()
	logFile := filepath.Join(tmpDir, "concurrent.log")

	w := &RotateWriter{
		Filename: logFile,
		Cycle:    CycleDaily,
	}
	defer w.Close()

	const goroutines = 10
	const writesPerGoroutine = 100

	done := make(chan bool, goroutines)

	for g := 0; g < goroutines; g++ {
		go func(id int) {
			for i := 0; i < writesPerGoroutine; i++ {
				msg := fmt.Sprintf("goroutine %d, write %d\n", id, i)
				if _, err := w.Write([]byte(msg)); err != nil {
					t.Errorf("Write error: %v", err)
					return
				}
			}
			done <- true
		}(g)
	}

	// Wait for all goroutines
	for i := 0; i < goroutines; i++ {
		<-done
	}

	// Verify file has content
	info, err := os.Stat(logFile)
	if err != nil {
		t.Fatalf("Failed to stat file: %v", err)
	}
	if info.Size() == 0 {
		t.Fatal("File should have content after concurrent writes")
	}
}

// TestClose_Idempotent tests that Close can be called multiple times safely
func TestClose_Idempotent(t *testing.T) {
	tmpDir := t.TempDir()
	logFile := filepath.Join(tmpDir, "close.log")

	w := &RotateWriter{
		Filename: logFile,
		Cycle:    CycleDaily,
	}

	w.Write([]byte("data"))

	// Multiple closes should not panic or error
	err1 := w.Close()
	err2 := w.Close()
	err3 := w.Close()

	if err1 != nil || err2 != nil || err3 != nil {
		t.Errorf("Multiple closes should not return errors: %v, %v, %v", err1, err2, err3)
	}
}

// TestDailyRotateWriter_Embedding tests DailyRotateWriter embedding
func TestDailyRotateWriter_Embedding(t *testing.T) {
	tmpDir := t.TempDir()
	logFile := filepath.Join(tmpDir, "daily_embed.log")

	dw := NewDailyRotateWriter(logFile, 7, 3, false)
	defer dw.Close()

	// Should be able to use RotateWriter methods through embedded type
	_, err := dw.Write([]byte("test via daily writer"))
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Should be able to call Rotate
	if err := dw.Rotate(); err != nil {
		t.Fatalf("Rotate failed: %v", err)
	}

	// File should still be usable
	_, err = dw.Write([]byte("after rotate"))
	if err != nil {
		t.Fatalf("Write after rotate failed: %v", err)
	}
}

// TestNewRotateWriter tests creating a RotateWriter directly with custom cycle
func TestNewRotateWriter(t *testing.T) {
	tmpDir := t.TempDir()
	logFile := filepath.Join(tmpDir, "direct.log")

	w := NewRotateWriter(CycleHourly, logFile, 5, 0, true)
	defer w.Close()

	if w.Cycle != CycleHourly {
		t.Errorf("Cycle = %v, want %v", w.Cycle, CycleHourly)
	}
	if w.Filename != logFile {
		t.Errorf("Filename = %v, want %v", w.Filename, logFile)
	}
	if w.MaxBackups != 5 {
		t.Errorf("MaxBackups = %v, want %v", w.MaxBackups, 5)
	}
	if w.MaxAge != 0 {
		t.Errorf("MaxAge = %v, want %v", w.MaxAge, 0)
	}
	if !w.Compress {
		t.Error("Compress should be true")
	}
}

// TestNewLoggerByWriter tests creating logger from a custom writer
func TestNewLoggerByWriter(t *testing.T) {
	tmpDir := t.TempDir()
	logFile := filepath.Join(tmpDir, "bywriter.log")

	fh, err := NewFileWriter(logFile)
	if err != nil {
		t.Fatalf("Failed to create file writer: %v", err)
	}
	defer fh.Close()

	// Test JSON format logger
	jsonLogger := NewLoggerByWriter(fh, true, false)
	jsonLogger.Info("json test", "key", "value")
	content, _ := os.ReadFile(logFile)
	if !contains(string(content), "json test") {
		t.Error("JSON logger should write to file")
	}
}

// TestParseLevel_EdgeCases tests ParseLevel with edge case values
func TestParseLevel_EdgeCases(t *testing.T) {
	tests := []struct {
		input    string
		expected slog.Level
		desc     string
	}{
		{"", slog.LevelInfo, "empty string defaults to Info"},
		{"NOTICE", slog.LevelInfo, "NOTICE maps to Info"},
		{"notice", slog.LevelInfo, "notice (lowercase) maps to Info"},
		{"ERR", slog.LevelError, "ERR maps to Error"},
		{"err", slog.LevelError, "err (lowercase) maps to Error"},
		{"FATAL", slog.LevelError, "FATAL maps to Error"},
		{"fatal", slog.LevelError, "fatal (lowercase) maps to Error"},
		{"unknown_level", slog.LevelInfo, "unknown level defaults to Info"},
		{"WARNING", slog.LevelWarn, "WARNING maps to Warn"},
		{"warning", slog.LevelWarn, "warning (lowercase) maps to Warn"},
	}

	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			result := ParseLevel(tt.input)
			if result != tt.expected {
				t.Errorf("ParseLevel(%q) = %v, want %v (%s)", tt.input, result, tt.expected, tt.desc)
			}
		})
	}
}
