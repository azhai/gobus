package log

import (
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	compressSuffix = ".gz"             // gzip file suffix
	backupTimeFmt  = "20060102-150405" // full timestamp format for backup filenames
)

// currentTime can be overridden in tests
var currentTime = time.Now

// RotateCycle defines the time granularity for log file rotation
type RotateCycle int

const (
	CycleMonthly  RotateCycle = iota // Rotate monthly, backup format: 200601
	CycleWeekly                      // Rotate weekly, backup format: 20060102
	CycleDaily                       // Rotate daily, backup format: 20060102
	CycleHourly                      // Rotate hourly, backup format: 2006010215
	CycleMinutely                    // Rotate every minute, backup format: 20060102-1504
)

// layout returns the Go time format string used to determine if a period boundary has been crossed
func (c RotateCycle) layout() string {
	switch c {
	case CycleMonthly:
		return "200601"
	case CycleWeekly:
		return "20060102"
	case CycleDaily:
		return "20060102"
	case CycleHourly:
		return "2006010215"
	default:
		return "20060102-1504"
	}
}

// periodLabel formats the current time into a period identifier string for the given cycle
func (c RotateCycle) periodLabel(now time.Time) string {
	return now.Format(c.layout())
}

// NewRotateWriter creates a RotateWriter with the specified rotation cycle and retention settings.
// cycle: the rotation cycle (CycleMonthly, CycleWeekly, CycleDaily, CycleHourly, or CycleMinutely).
// maxBackups: maximum number of old log files to retain (0 = no limit).
// maxAge: maximum days to retain old log files (0 = no limit).
// compress: whether to gzip-compress old log files.
func NewRotateWriter(cycle RotateCycle, filename string, maxBackups, maxAge int, compress bool) *RotateWriter {
	return &RotateWriter{
		Cycle:      cycle,
		Filename:   filename,
		MaxBackups: maxBackups,
		MaxAge:     maxAge,
		Compress:   compress,
	}
}

// DailyRotateWriter is a convenience type that embeds RotateWriter with CycleDaily.
// It provides daily log rotation out of the box.
type DailyRotateWriter struct {
	*RotateWriter
}

// NewDailyRotateWriter creates a DailyRotateWriter with the given filename and retention settings.
// maxBackups: maximum number of old log files to retain (0 = no limit).
// maxAge: maximum days to retain old log files (0 = no limit).
// compress: whether to gzip-compress old log files.
func NewDailyRotateWriter(filename string, maxBackups, maxAge int, compress bool) *DailyRotateWriter {
	return &DailyRotateWriter{
		RotateWriter: NewRotateWriter(CycleDaily, filename, maxBackups, maxAge, compress),
	}
}

// RotateWriter is an io.WriteCloser that rotates log files by time period.
// It supports monthly, weekly, daily, hourly, and minutely rotation.
// When a period boundary is crossed, the current file is renamed with a timestamp
// and a new file is created. Old files can be compressed and cleaned up automatically.
//
// Only one process should write to the same log file at a time.
var _ io.WriteCloser = (*RotateWriter)(nil)

type RotateWriter struct {
	Cycle      RotateCycle // Rotation cycle (monthly, weekly, daily, hourly, minutely)
	Filename   string      // Log file path; defaults to <process>-log in os.TempDir()
	MaxBackups int         // Maximum number of old log files to retain (0 = no limit)
	MaxAge     int         // Maximum days to retain old log files (0 = no limit)
	Compress   bool        // Whether to gzip-compress rotated log files

	openPeriod string     // current period label, used to detect boundary crossing
	size       int64      // current file size in bytes
	file       *os.File   // current log file handle
	mu         sync.Mutex // protects all mutable state

	millCh    chan struct{} // signal channel for async compression/cleanup
	startMill sync.Once     // ensures mill goroutine is started only once
}

// Write implements io.Writer. If the current time crosses a period boundary,
// the file is rotated before writing.
func (w *RotateWriter) Write(p []byte) (n int, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		if err = w.openExistingOrNew(); err != nil {
			return 0, err
		}
	}

	period := w.periodLabel(currentTime())
	if w.openPeriod != period {
		if err = w.rotate(); err != nil {
			return 0, err
		}
	}

	n, err = w.file.Write(p)
	if n > 0 {
		w.size += int64(n)
	}
	return n, err
}

func (w *RotateWriter) periodLabel(t time.Time) string {
	return w.Cycle.periodLabel(t)
}

// Close implements io.Closer, closing the current log file.
func (w *RotateWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.close()
}

// Rotate forces a rotation of the log file, closing the current file and creating a new one.
// Useful for initiating rotations outside the normal cycle, such as in response to SIGHUP.
func (w *RotateWriter) Rotate() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.rotate()
}

// close closes the file without holding the mutex (caller must hold w.mu)
func (w *RotateWriter) close() error {
	if w.file == nil {
		return nil
	}
	err := w.file.Close()
	w.file = nil
	return err
}

// rotate closes the current file, creates a new one, and triggers background compression/cleanup
func (w *RotateWriter) rotate() error {
	if err := w.close(); err != nil {
		return err
	}
	if err := w.openNew(); err != nil {
		return err
	}
	w.mill()
	return nil
}

// openNew creates a new log file, renaming any existing file with a backup timestamp
func (w *RotateWriter) openNew() error {
	if err := os.MkdirAll(w.dir(), 0755); err != nil {
		return fmt.Errorf("can't make directories for new logfile: %s", err)
	}

	name := w.filename()
	mode := os.FileMode(0600)
	info, err := os.Stat(name)
	if err == nil {
		mode = info.Mode()
		newName := backupName(name, w.Cycle.layout(), info.ModTime())
		if err = os.Rename(name, newName); err != nil {
			return fmt.Errorf("can't rename log file: %s", err)
		}
	}

	flag := os.O_CREATE | os.O_WRONLY | os.O_TRUNC
	f, err := os.OpenFile(name, flag, mode)
	if err != nil {
		return fmt.Errorf("can't open new logfile: %s", err)
	}
	w.file = f
	w.size = 0
	w.openPeriod = w.periodLabel(currentTime())
	return nil
}

// openExistingOrNew opens an existing log file for appending, or creates a new one if it doesn't exist.
// If the existing file was last modified in a previous period, it is rotated first.
func (w *RotateWriter) openExistingOrNew() error {
	w.mill()

	fn := w.filename()
	info, err := os.Stat(fn)
	if os.IsNotExist(err) {
		return w.openNew()
	}
	if err != nil {
		return fmt.Errorf("error getting log file info: %s", err)
	}

	// If the file was last modified in a previous period, rotate it first
	filePeriod := w.periodLabel(info.ModTime())
	currentPeriod := w.periodLabel(currentTime())
	if filePeriod != currentPeriod {
		return w.rotateStaleFile(fn, info)
	}

	f, err := os.OpenFile(fn, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return w.openNew()
	}
	w.file = f
	w.size = info.Size()
	w.openPeriod = currentPeriod
	return nil
}

// rotateStaleFile renames an existing log file from a previous period and creates a new one
func (w *RotateWriter) rotateStaleFile(fn string, info os.FileInfo) error {
	mode := info.Mode()
	newName := backupName(fn, w.Cycle.layout(), info.ModTime())
	if err := os.Rename(fn, newName); err != nil {
		return fmt.Errorf("can't rename stale log file: %s", err)
	}

	flag := os.O_CREATE | os.O_WRONLY | os.O_TRUNC
	f, err := os.OpenFile(fn, flag, mode)
	if err != nil {
		return fmt.Errorf("can't open new logfile: %s", err)
	}
	w.file = f
	w.size = 0
	w.openPeriod = w.periodLabel(currentTime())
	w.mill()
	return nil
}

// filename returns the log file path, defaulting to <process>-log in os.TempDir()
func (w *RotateWriter) filename() string {
	if w.Filename != "" {
		return w.Filename
	}
	name := filepath.Base(os.Args[0]) + ".log"
	return filepath.Join(os.TempDir(), name)
}

func (w *RotateWriter) dir() string {
	return filepath.Dir(w.filename())
}

// prefixAndExt extracts the prefix (basename + "-") and extension from the filename for matching backups
func (w *RotateWriter) prefixAndExt() (prefix, ext string) {
	fn := filepath.Base(w.filename())
	ext = filepath.Ext(fn)
	prefix = fn[:len(fn)-len(ext)] + "-"
	return prefix, ext
}

// timeFromName parses the timestamp from a backup filename given its prefix and extension
func (w *RotateWriter) timeFromName(filename, prefix, ext string) (time.Time, error) {
	if !strings.HasPrefix(filename, prefix) {
		return time.Time{}, errors.New("mismatched prefix")
	}
	if !strings.HasSuffix(filename, ext) {
		return time.Time{}, errors.New("mismatched extension")
	}
	ts := filename[len(prefix) : len(filename)-len(ext)]
	short := w.Cycle.layout()
	if len(ts) == len(short) {
		return time.Parse(short, ts)
	}
	return time.Parse(backupTimeFmt, ts)
}

// oldLogFiles returns all backup log files in the log directory, sorted by timestamp (newest first)
func (w *RotateWriter) oldLogFiles() ([]logInfo, error) {
	entries, err := os.ReadDir(w.dir())
	if err != nil {
		return nil, fmt.Errorf("can't read log file directory: %s", err)
	}

	var logFiles []logInfo
	prefix, ext := w.prefixAndExt()

	for _, f := range entries {
		if f.IsDir() {
			continue
		}
		var t time.Time
		var info os.FileInfo
		if t, err = w.timeFromName(f.Name(), prefix, ext); err == nil {
			if info, err = f.Info(); err == nil {
				logFiles = append(logFiles, logInfo{t, info})
			}
			continue
		}
		if t, err = w.timeFromName(f.Name(), prefix, ext+compressSuffix); err == nil {
			if info, err = f.Info(); err == nil {
				logFiles = append(logFiles, logInfo{t, info})
			}
		}
	}

	sort.Sort(byFormatTime(logFiles))
	return logFiles, nil
}

// millRunOnce performs one round of compression and removal of old log files
func (w *RotateWriter) millRunOnce() error {
	if w.MaxBackups == 0 && w.MaxAge == 0 && !w.Compress {
		return nil
	}

	files, err := w.oldLogFiles()
	if err != nil {
		return err
	}

	var compressList, remove []logInfo

	if w.MaxBackups > 0 && w.MaxBackups < len(files) {
		preserved := make(map[string]bool)
		var remaining []logInfo
		for _, f := range files {
			fn := strings.TrimSuffix(f.Name(), compressSuffix)
			preserved[fn] = true
			if len(preserved) > w.MaxBackups {
				remove = append(remove, f)
			} else {
				remaining = append(remaining, f)
			}
		}
		files = remaining
	}

	if w.MaxAge > 0 {
		diff := time.Duration(int64(24*time.Hour) * int64(w.MaxAge))
		cutoff := currentTime().Add(-diff)
		var remaining []logInfo
		for _, f := range files {
			if f.timestamp.Before(cutoff) {
				remove = append(remove, f)
			} else {
				remaining = append(remaining, f)
			}
		}
		files = remaining
	}

	if w.Compress {
		for _, f := range files {
			if !strings.HasSuffix(f.Name(), compressSuffix) {
				compressList = append(compressList, f)
			}
		}
	}

	for _, f := range remove {
		errRemove := os.Remove(filepath.Join(w.dir(), f.Name()))
		if err == nil && errRemove != nil {
			err = errRemove
		}
	}
	for _, f := range compressList {
		fn := filepath.Join(w.dir(), f.Name())
		errCompress := compressLogFile(fn, fn+compressSuffix)
		if err == nil && errCompress != nil {
			err = errCompress
		}
	}

	return err
}

// millRun is the background goroutine that processes compression/cleanup signals
func (w *RotateWriter) millRun() {
	for range w.millCh {
		_ = w.millRunOnce()
	}
}

// mill non-blockingly signals the background goroutine to run compression/cleanup
func (w *RotateWriter) mill() {
	w.startMill.Do(func() {
		w.millCh = make(chan struct{}, 1)
		go w.millRun()
	})
	select {
	case w.millCh <- struct{}{}:
	default:
	}
}

// backupName constructs a backup filename by inserting a timestamp before the extension
func backupName(name, timeFormat string, last time.Time) string {
	dir := filepath.Dir(name)
	fn := filepath.Base(name)
	ext := filepath.Ext(fn)
	prefix := fn[:len(fn)-len(ext)]
	timestamp := last.Format(timeFormat)
	return filepath.Join(dir, fmt.Sprintf("%s-%s%s", prefix, timestamp, ext))
}

// compressLogFile gzip-compresses src to dst and removes the original file on success
func compressLogFile(src, dst string) (err error) {
	f, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("failed to open log file: %v", err)
	}
	defer f.Close()

	fi, err := os.Stat(src)
	if err != nil {
		return fmt.Errorf("failed to stat log file: %v", err)
	}

	gzf, err := os.OpenFile(dst, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, fi.Mode())
	if err != nil {
		return fmt.Errorf("failed to open compressed log file: %v", err)
	}
	defer gzf.Close()

	gz := gzip.NewWriter(gzf)
	defer func() {
		if err != nil {
			os.Remove(dst)
			err = fmt.Errorf("failed to compress log file: %v", err)
		}
	}()

	if _, err = io.Copy(gz, f); err != nil {
		return err
	}
	if err = gz.Close(); err != nil {
		return err
	}
	if err = gzf.Close(); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Remove(src); err != nil {
		return err
	}
	return nil
}

// logInfo pairs a backup file's parsed timestamp with its file info
type logInfo struct {
	timestamp time.Time
	os.FileInfo
}

// byFormatTime sorts logInfo by timestamp descending (newest first)
type byFormatTime []logInfo

func (b byFormatTime) Less(i, j int) bool { return b[i].timestamp.After(b[j].timestamp) }
func (b byFormatTime) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }
func (b byFormatTime) Len() int           { return len(b) }
