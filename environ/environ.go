package environ

import (
	"bufio"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"unicode"
)

const (
	// MaxRecurDepth is the maximum recursion depth for nested environment variables
	// It limits how deeply nested environment variables can be expanded
	MaxRecurDepth = 3
	PrefixName    = "CFG_PREFIX"
)

// Environ represents an environment variable manager
// It stores environment variables in a map and supports loading from a file and the system environment
type Environ struct {
	PrefixInOS *string
	mu         sync.RWMutex
	storage    map[string]Entry
}

// NewEnv creates a new Environ instance and loads environment variables from the default .env file
// It panics if the file cannot be opened or read
func NewEnv() *Environ {
	return MustLoadEnvFile(".env")
}

// NewEmptyEnviron creates a new empty Environ instance that falls back to system
// environment variables when a key is not found in its internal storage.
func NewEmptyEnviron() *Environ {
	return &Environ{storage: make(map[string]Entry)}
}

// NewEnvWithFile is an alias for MustWithFile, kept for backward compatibility
func NewEnvWithFile(filename string) *Environ {
	return MustLoadEnvFile(filename)
}

// MustLoadEnvFile creates a new Environ instance and loads environment variables from the specified file
// It panics if the file cannot be opened or read
func MustLoadEnvFile(filename string) *Environ {
	env, err := LoadEnvFile(filename)
	if err != nil {
		panic(err)
	}
	return env
}

// LoadEnvFile creates a new Environ instance and loads environment variables from the specified file
// It returns an error if the file cannot be opened or read
func LoadEnvFile(filename string) (*Environ, error) {
	env := &Environ{storage: make(map[string]Entry)}
	err := env.Load(os.Open(filename))
	return env, err
}

// Load loads environment variables from a file and stores them in the internal storage
// It reads from the provided reader and handles closing the reader
func (v *Environ) Load(reader io.ReadCloser, err error) error {
	if err != nil || reader == nil {
		return err
	}
	// Close the file and handle any potential error
	defer func() {
		if closeErr := reader.Close(); closeErr != nil {
			err = closeErr
		}
	}()
	return v.ScanLines(reader)
}

// ScanLines reads environment variables from a reader and stores them in the internal storage
// It skips comments (lines starting with #) and empty lines
// It splits each line into key-value pairs using the first '=' character
// It trims whitespace from keys and values and removes surrounding quotes if present
// If there is an error reading the file, it returns the error
func (v *Environ) ScanLines(reader io.Reader) error {
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		line := strings.TrimLeftFunc(scanner.Text(), unicode.IsSpace)

		// Skip comments or empty lines
		if len(line) == 0 || strings.HasPrefix(line, "#") {
			continue
		}

		// Split key and value by the first occurrence of '='
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}

		key := strings.TrimRightFunc(parts[0], unicode.IsSpace)
		value := strings.TrimSpace(parts[1])

		// Remove surrounding quotes if any
		if len(value) >= 2 {
			firstChar, lastChar := value[0], value[len(value)-1]
			if (firstChar == '"' || firstChar == '\'') && (firstChar == lastChar) {
				value = value[1 : len(value)-1]
			}
		}

		v.mu.Lock()
		v.storage[key] = Entry{Key: key, Value: value}
		v.mu.Unlock()
	}

	return scanner.Err()
}

// Lookup searches for an environment variable by key
// It first checks the internal storage. If not found, it checks the system environment variables
// If the variable is found, it returns the Entry and true; otherwise, it returns an empty Entry and false
// If the variable is found in the system environment, it is added to the internal storage
func (v *Environ) Lookup(key string) (Entry, bool) {
	v.mu.RLock()
	entry, ok := v.storage[key]
	v.mu.RUnlock()
	if ok {
		return entry, true
	} else if key == PrefixName {
		return Entry{}, false
	}

	prefix := v.GetSectionPrefix()
	ee := os.Getenv(prefix + key)
	if ee != "" {
		entry := Entry{Key: key, Value: ee}
		v.mu.Lock()
		v.storage[key] = entry
		v.mu.Unlock()
		return entry, true
	}

	return Entry{}, false
}

// Get retrieves and expands the value of an environment variable by key.
func (v *Environ) recurGet(key string, depth int) string {
	s := v.GetStr(key)
	if s != "" && depth <= MaxRecurDepth && strings.Contains(s, "$") {
		return os.Expand(s, func(k string) string {
			return v.recurGet(k, depth+1)
		})
	}
	return s
}

// Get retrieves and expands the value of an environment variable by key
// It recursively expands nested environment variables up to MaxRecurDepth
func (v *Environ) Get(key string) string {
	return v.recurGet(key, 1)
}

// GetStr retrieves the string value of an environment variable by key
// If the variable is not found, it returns the fallback value
func (v *Environ) GetStr(key string, fallback ...string) string {
	if entry, ok := v.Lookup(key); ok || len(fallback) == 0 {
		return entry.Str()
	}
	return fallback[0]
}

// GetInt retrieves the integer value of an environment variable by key
// If the variable is not found or the value cannot be converted to an integer, it returns the fallback value
func (v *Environ) GetInt(key string, fallback ...int) int {
	if entry, ok := v.Lookup(key); ok || len(fallback) == 0 {
		return entry.Int()
	}
	return fallback[0]
}

// GetInt64 retrieves the 64-bit integer value of an environment variable by key
// If the variable is not found or the value cannot be converted to a 64-bit integer, it returns the fallback value
func (v *Environ) GetInt64(key string, fallback ...int64) int64 {
	if entry, ok := v.Lookup(key); ok || len(fallback) == 0 {
		return entry.Int64()
	}
	return fallback[0]
}

// GetBool retrieves the boolean value of an environment variable by key
// It supports "yes", "no", "true", and "false" as valid boolean values
// If the variable is not found or the value cannot be converted to a boolean, it returns the fallback value
func (v *Environ) GetBool(key string, fallback ...bool) bool {
	if entry, ok := v.Lookup(key); ok || len(fallback) == 0 {
		return entry.Bool()
	}
	return fallback[0]
}

// Entry represents an environment variable entry with a key and a value
// It stores a single environment variable's key-value pair
type Entry struct {
	Key   string // Environment variable key
	Value string // Environment variable value
}

// Str returns the string value of the entry
// It returns the raw string value of the environment variable
func (t *Entry) Str() string {
	return t.Value
}

// Int converts the entry's value to an integer
// If the conversion fails, it returns 0
func (t *Entry) Int() int {
	if val, err := strconv.Atoi(t.Value); err == nil {
		return val
	}
	return 0
}

// Int64 converts the entry's value to a 64-bit integer
// If the conversion fails, it returns 0
func (t *Entry) Int64() int64 {
	if val, err := strconv.ParseInt(t.Value, 10, 64); err == nil {
		return val
	}
	return 0
}

// Bool converts the entry's value to a boolean
// It supports "yes", "no", "true", and "false" as valid boolean values
// If the conversion fails, it returns false
func (t *Entry) Bool() bool {
	t.Value = strings.ToLower(t.Value)
	switch t.Value {
	case "on", "true", "yes":
		return true
	case "false", "no", "off":
		return false
	}
	if val, err := strconv.ParseBool(t.Value); err == nil {
		return val
	}
	return false
}

func (v *Environ) GetWithFallback(key, fallback string) string {
	if val := v.GetStr(key); val != "" {
		return val
	}
	return v.GetStr(fallback)
}

func (v *Environ) GetSectionPrefix() string {
	if v.PrefixInOS == nil {
		v.PrefixInOS = new(string)
		*v.PrefixInOS = v.GetStr(PrefixName, "")
	}
	return *v.PrefixInOS
}
