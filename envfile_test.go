package main

import (
	"os"
	"path/filepath"
	"testing"

	flag "github.com/spf13/pflag"
)

func TestParseEnvFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, ".env")
	content := `# comment
DBDOCS_DATABASE=mydb
DBDOCS_PORT=5433

 UNKNOWN_FOO=bar
DBDOCS_HOSTNAME = "quoted-host"
`
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	m, err := parseEnvFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if m["DBDOCS_DATABASE"] != "mydb" {
		t.Fatalf("database: got %q", m["DBDOCS_DATABASE"])
	}
	if m["DBDOCS_PORT"] != "5433" {
		t.Fatalf("port: got %q", m["DBDOCS_PORT"])
	}
	if m["DBDOCS_HOSTNAME"] != "quoted-host" {
		t.Fatalf("hostname: got %q", m["DBDOCS_HOSTNAME"])
	}
	if _, ok := m["UNKNOWN_FOO"]; !ok {
		t.Fatal("expected UNKNOWN_FOO in map")
	}
}

func TestApplyEnvFileToFlags_OmitsUnknownKeys(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	hostname := fs.StringP("hostname", "h", "localhost", "")
	database := fs.StringP("database", "d", "", "")
	if err := applyEnvFileToFlags(fs, map[string]string{
		"DBDOCS_DATABASE": "fromfile",
		"OTHER":           "ignored",
	}); err != nil {
		t.Fatal(err)
	}
	if *database != "fromfile" {
		t.Fatalf("database: got %q", *database)
	}
	if *hostname != "localhost" {
		t.Fatalf("hostname should stay default: got %q", *hostname)
	}
}

func TestAbsoluteFromInvocation_relativeUsesCwd(t *testing.T) {
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(wd) })

	dir := t.TempDir()
	sub := filepath.Join(dir, "cfg")
	if err := os.MkdirAll(sub, 0755); err != nil {
		t.Fatal(err)
	}
	envPath := filepath.Join(sub, "settings.env")
	if err := os.WriteFile(envPath, []byte("DBDOCS_DATABASE=x\n"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}

	got, err := absoluteFromInvocation(filepath.Join("cfg", "settings.env"))
	if err != nil {
		t.Fatal(err)
	}
	want := envPath
	if filepath.Clean(got) != filepath.Clean(want) {
		t.Fatalf("resolved path: got %q want %q", got, want)
	}

	full, err := absoluteFromInvocation(envPath)
	if err != nil {
		t.Fatal(err)
	}
	if filepath.Clean(full) != filepath.Clean(envPath) {
		t.Fatalf("absolute input: got %q want %q", full, envPath)
	}
}

func TestResolveEnvFilePath_defaultDotEnv(t *testing.T) {
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(wd) })

	dir := t.TempDir()
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	oldArgs := os.Args
	t.Cleanup(func() { os.Args = oldArgs })
	os.Args = []string{"dbdocs"}

	abs, explicit, err := resolveEnvFilePath()
	if err != nil {
		t.Fatal(err)
	}
	if explicit {
		t.Fatal("expected explicit=false when CLI/env omit path")
	}
	want := filepath.Join(dir, defaultEnvFileName)
	if filepath.Clean(abs) != filepath.Clean(want) {
		t.Fatalf("path: got %q want %q", abs, want)
	}
}
