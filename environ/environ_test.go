package environ

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func TestNewEmptyEnviron(t *testing.T) {
	env := NewEmptyEnviron()
	if env == nil {
		t.Fatal("NewEmptyEnviron should return non-nil")
	}
}

func TestEnviron_ScanLines(t *testing.T) {
	env := NewEmptyEnviron()
	input := "KEY1=value1\nKEY2=value2\n# comment\n\nKEY3=value3"
	err := env.ScanLines(strings.NewReader(input))
	if err != nil {
		t.Fatalf("ScanLines failed: %v", err)
	}

	if v, ok := env.Lookup("KEY1"); !ok || v.Value != "value1" {
		t.Errorf("Expected KEY1=value1, got %v", v)
	}
	if v, ok := env.Lookup("KEY2"); !ok || v.Value != "value2" {
		t.Errorf("Expected KEY2=value2, got %v", v)
	}
	if v, ok := env.Lookup("KEY3"); !ok || v.Value != "value3" {
		t.Errorf("Expected KEY3=value3, got %v", v)
	}
}

func TestEnviron_ScanLines_QuotedValues(t *testing.T) {
	env := NewEmptyEnviron()
	input := `KEY1="quoted value"` + "\n" + `KEY2='single quoted'`
	err := env.ScanLines(strings.NewReader(input))
	if err != nil {
		t.Fatalf("ScanLines failed: %v", err)
	}

	if v, ok := env.Lookup("KEY1"); !ok || v.Value != "quoted value" {
		t.Errorf("Expected KEY1=quoted value, got %v", v)
	}
	if v, ok := env.Lookup("KEY2"); !ok || v.Value != "single quoted" {
		t.Errorf("Expected KEY2=single quoted, got %v", v)
	}
}

func TestEnviron_ScanLines_Whitespace(t *testing.T) {
	env := NewEmptyEnviron()
	input := "  KEY1  =  value1  "
	err := env.ScanLines(strings.NewReader(input))
	if err != nil {
		t.Fatalf("ScanLines failed: %v", err)
	}

	if v, ok := env.Lookup("KEY1"); !ok || v.Value != "value1" {
		t.Errorf("Expected KEY1=value1, got %v", v)
	}
}

func TestEnviron_ScanLines_SkipInvalid(t *testing.T) {
	env := NewEmptyEnviron()
	input := "NOEQUALSSIGN\nKEY1=value1\n=NOKEY"
	err := env.ScanLines(strings.NewReader(input))
	if err != nil {
		t.Fatalf("ScanLines failed: %v", err)
	}

	if _, ok := env.Lookup("NOEQUALSSIGN"); ok {
		t.Error("Should skip lines without =")
	}
	if v, ok := env.Lookup("KEY1"); !ok || v.Value != "value1" {
		t.Errorf("Expected KEY1=value1, got %v", v)
	}
}

func TestEnviron_GetStr(t *testing.T) {
	env := NewEmptyEnviron()
	env.ScanLines(strings.NewReader("KEY1=hello"))

	if v := env.GetStr("KEY1"); v != "hello" {
		t.Errorf("Expected hello, got %s", v)
	}
	if v := env.GetStr("NONEXISTENT", "fallback"); v != "fallback" {
		t.Errorf("Expected fallback, got %s", v)
	}
}

func TestEnviron_GetInt(t *testing.T) {
	env := NewEmptyEnviron()
	env.ScanLines(strings.NewReader("NUM=42"))

	if v := env.GetInt("NUM"); v != 42 {
		t.Errorf("Expected 42, got %d", v)
	}
	if v := env.GetInt("NONEXISTENT", 99); v != 99 {
		t.Errorf("Expected 99, got %d", v)
	}
}

func TestEnviron_GetInt64(t *testing.T) {
	env := NewEmptyEnviron()
	env.ScanLines(strings.NewReader("NUM=9999999999"))

	if v := env.GetInt64("NUM"); v != 9999999999 {
		t.Errorf("Expected 9999999999, got %d", v)
	}
}

func TestEnviron_GetBool(t *testing.T) {
	tests := []struct {
		input    string
		expected bool
	}{
		{"YES=true", true},
		{"NO=false", false},
		{"ON=on", true},
		{"OFF=off", false},
		{"YES_VAL=yes", true},
		{"NO_VAL=no", false},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			env := NewEmptyEnviron()
			env.ScanLines(strings.NewReader(tt.input))
			parts := strings.SplitN(tt.input, "=", 2)
			if v := env.GetBool(parts[0]); v != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, v)
			}
		})
	}

	env := NewEmptyEnviron()
	if v := env.GetBool("NONEXISTENT", true); v != true {
		t.Errorf("Expected fallback true, got %v", v)
	}
}

func TestEnviron_Lookup_SystemEnv(t *testing.T) {
	env := NewEmptyEnviron()

	key := "GOBUS_TEST_ENV_LOOKUP"
	os.Setenv(key, "from_system")
	defer os.Unsetenv(key)

	v, ok := env.Lookup(key)
	if !ok {
		t.Fatal("Should find system env var")
	}
	if v.Value != "from_system" {
		t.Errorf("Expected from_system, got %s", v.Value)
	}
}

func TestEnviron_Lookup_NotFound(t *testing.T) {
	env := NewEmptyEnviron()
	_, ok := env.Lookup("SURELY_NONEXISTENT_KEY_XYZ")
	if ok {
		t.Error("Should not find non-existent key")
	}
}

func TestEntry_Str(t *testing.T) {
	e := Entry{Key: "K", Value: "hello"}
	if e.Str() != "hello" {
		t.Errorf("Expected hello, got %s", e.Str())
	}
}

func TestEntry_Int(t *testing.T) {
	e := Entry{Key: "K", Value: "42"}
	if e.Int() != 42 {
		t.Errorf("Expected 42, got %d", e.Int())
	}

	e2 := Entry{Key: "K", Value: "notanumber"}
	if e2.Int() != 0 {
		t.Errorf("Expected 0 for invalid int, got %d", e2.Int())
	}
}

func TestEntry_Int64(t *testing.T) {
	e := Entry{Key: "K", Value: "9999999999"}
	if e.Int64() != 9999999999 {
		t.Errorf("Expected 9999999999, got %d", e.Int64())
	}

	e2 := Entry{Key: "K", Value: "notanumber"}
	if e2.Int64() != 0 {
		t.Errorf("Expected 0 for invalid int64, got %d", e2.Int64())
	}
}

func TestEntry_Bool(t *testing.T) {
	tests := []struct {
		value    string
		expected bool
	}{
		{"true", true},
		{"false", false},
		{"yes", true},
		{"no", false},
		{"on", true},
		{"off", false},
		{"invalid", false},
	}
	for _, tt := range tests {
		e := Entry{Key: "K", Value: tt.value}
		if e.Bool() != tt.expected {
			t.Errorf("Entry{Value:%s}.Bool() = %v, expected %v", tt.value, e.Bool(), tt.expected)
		}
	}
}

func TestEnviron_GetWithFallback(t *testing.T) {
	env := NewEmptyEnviron()
	env.ScanLines(strings.NewReader("KEY1=value1"))

	if v := env.GetWithFallback("KEY1", "KEY2"); v != "value1" {
		t.Errorf("Expected value1, got %s", v)
	}
}

func TestEnviron_Get_RecursiveExpansion(t *testing.T) {
	os.Setenv("GOBUS_BASE", "hello")
	os.Setenv("GOBUS_EXPAND", "$GOBUS_BASE world")
	defer os.Unsetenv("GOBUS_BASE")
	defer os.Unsetenv("GOBUS_EXPAND")

	env := NewEmptyEnviron()
	env.ScanLines(strings.NewReader("EXPAND=$GOBUS_EXPAND"))

	result := env.Get("EXPAND")
	if result != "hello world" {
		t.Errorf("Expected 'hello world', got '%s'", result)
	}
}

func TestEnviron_MaxRecurDepth(t *testing.T) {
	if MaxRecurDepth != 3 {
		t.Errorf("Expected MaxRecurDepth=3, got %d", MaxRecurDepth)
	}
}

// ============================================================
// 文件加载测试
// ============================================================

func TestLoadEnvFile(t *testing.T) {
	dir := t.TempDir()
	f := filepath.Join(dir, ".env")
	os.WriteFile(f, []byte("FILE_KEY=file_value\nNUM=100"), 0644)

	env, err := LoadEnvFile(f)
	if err != nil {
		t.Fatalf("LoadEnvFile failed: %v", err)
	}
	if v := env.GetStr("FILE_KEY"); v != "file_value" {
		t.Errorf("Expected file_value, got %s", v)
	}
	if v := env.GetInt("NUM"); v != 100 {
		t.Errorf("Expected 100, got %d", v)
	}
}

func TestLoadEnvFile_NotFound(t *testing.T) {
	_, err := LoadEnvFile("/nonexistent/path/.env")
	if err == nil {
		t.Error("Expected error for nonexistent file")
	}
}

func TestMustLoadEnvFile(t *testing.T) {
	dir := t.TempDir()
	f := filepath.Join(dir, ".env")
	os.WriteFile(f, []byte("MUST_KEY=must_value"), 0644)

	env := MustLoadEnvFile(f)
	if v := env.GetStr("MUST_KEY"); v != "must_value" {
		t.Errorf("Expected must_value, got %s", v)
	}
}

func TestMustLoadEnvFile_Panic(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Error("Expected panic for nonexistent file")
		}
	}()
	MustLoadEnvFile("/nonexistent/path/.env")
}

func TestNewEnvWithFile(t *testing.T) {
	dir := t.TempDir()
	f := filepath.Join(dir, ".env")
	os.WriteFile(f, []byte("WF_KEY=wf_value"), 0644)

	env := NewEnvWithFile(f)
	if v := env.GetStr("WF_KEY"); v != "wf_value" {
		t.Errorf("Expected wf_value, got %s", v)
	}
}

func TestEnviron_Load(t *testing.T) {
	env := NewEmptyEnviron()
	dir := t.TempDir()
	f := filepath.Join(dir, "test.env")
	os.WriteFile(f, []byte("LOAD_KEY=load_value"), 0644)

	err := env.Load(os.Open(f))
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if v := env.GetStr("LOAD_KEY"); v != "load_value" {
		t.Errorf("Expected load_value, got %s", v)
	}
}

func TestEnviron_Load_Error(t *testing.T) {
	env := NewEmptyEnviron()
	err := env.Load(nil, fmt.Errorf("test error"))
	if err == nil || err.Error() != "test error" {
		t.Errorf("Expected test error, got %v", err)
	}
}

func TestEnviron_Load_NilReader(t *testing.T) {
	env := NewEmptyEnviron()
	err := env.Load(nil, nil)
	if err != nil {
		t.Errorf("Expected nil error for nil reader, got %v", err)
	}
}

// ============================================================
// 分支覆盖补充
// ============================================================

func TestEnviron_GetInt64_Fallback(t *testing.T) {
	env := NewEmptyEnviron()
	if v := env.GetInt64("NONEXISTENT", 999); v != 999 {
		t.Errorf("Expected fallback 999, got %d", v)
	}
}

func TestEntry_Bool_ParseBool(t *testing.T) {
	e := Entry{Key: "K", Value: "1"}
	if !e.Bool() {
		t.Error("Expected true for '1'")
	}
	e2 := Entry{Key: "K", Value: "0"}
	if e2.Bool() {
		t.Error("Expected false for '0'")
	}
}

func TestEnviron_GetWithFallback_Fallback(t *testing.T) {
	env := NewEmptyEnviron()
	env.ScanLines(strings.NewReader("KEY2=fallback_val"))

	if v := env.GetWithFallback("NONEXISTENT", "KEY2"); v != "fallback_val" {
		t.Errorf("Expected fallback_val, got %s", v)
	}
}

func TestEnviron_Get_NoExpansion(t *testing.T) {
	env := NewEmptyEnviron()
	env.ScanLines(strings.NewReader("PLAIN=just_text"))

	result := env.Get("PLAIN")
	if result != "just_text" {
		t.Errorf("Expected just_text, got %s", result)
	}
}

func TestEnviron_Lookup_EmptyKey(t *testing.T) {
	env := NewEmptyEnviron()
	_, ok := env.Lookup("")
	if ok {
		t.Error("Empty key should not be found")
	}
}

func TestEnviron_GetSectionPrefix(t *testing.T) {
	env := NewEmptyEnviron()
	if p := env.GetSectionPrefix(); p != "" {
		t.Errorf("Expected empty prefix, got %s", p)
	}

	prefix := "APP_"
	env2 := &Environ{PrefixInOS: &prefix, storage: make(map[string]Entry)}
	if p := env2.GetSectionPrefix(); p != "APP_" {
		t.Errorf("Expected APP_, got %s", p)
	}
}

// ============================================================
// 数据竞争测试
// ============================================================

func TestRace_EnvironConcurrentLookup(t *testing.T) {
	env := NewEmptyEnviron()
	env.ScanLines(strings.NewReader("KEY1=value1\nKEY2=value2\nKEY3=value3"))

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("KEY%d", (i%3)+1)
			_, _ = env.Lookup(key)
		}(i)
	}
	wg.Wait()
}

func TestRace_EnvironConcurrentGet(t *testing.T) {
	env := NewEmptyEnviron()
	env.ScanLines(strings.NewReader("STR=hello\nNUM=42\nFLAG=true"))

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(4)
		go func() {
			defer wg.Done()
			_ = env.GetStr("STR")
		}()
		go func() {
			defer wg.Done()
			_ = env.GetInt("NUM")
		}()
		go func() {
			defer wg.Done()
			_ = env.GetInt64("NUM")
		}()
		go func() {
			defer wg.Done()
			_ = env.GetBool("FLAG")
		}()
	}
	wg.Wait()
}

func TestRace_EnvironConcurrentLookupAndScanLines(t *testing.T) {
	env := NewEmptyEnviron()
	env.ScanLines(strings.NewReader("INITIAL=value"))

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			input := fmt.Sprintf("KEY%d=value%d", i, i)
			_ = env.ScanLines(strings.NewReader(input))
		}(i)
		go func() {
			defer wg.Done()
			_, _ = env.Lookup("INITIAL")
		}()
	}
	wg.Wait()
}
