package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	flag "github.com/spf13/pflag"
)

const defaultEnvFileName = ".env"

// envFileKeys maps DBDOCS_* keys from a dotenv file to pflag names.
var envFileKeys = map[string]string{
	"DBDOCS_HOSTNAME": "hostname",
	"DBDOCS_DATABASE": "database",
	"DBDOCS_USERNAME": "username",
	"DBDOCS_PASSWORD": "password",
	"DBDOCS_PORT":     "port",
	"DBDOCS_SCHEMA":   "schema",
	"DBDOCS_OUTPUT":   "output",
	"DBDOCS_TABLES":   "tables",
}

// rawEnvFilePath returns the path from -e/--env-file or DBDOCS_ENV_FILE (may be relative).
func rawEnvFilePath() string {
	if p := envFilePathFromArgs(); p != "" {
		return p
	}
	return os.Getenv("DBDOCS_ENV_FILE")
}

// resolveEnvFilePath returns the absolute path to read: explicit path from CLI/DBDOCS_ENV_FILE,
// otherwise defaultEnvFileName in the invocation directory. The second result is whether the
// user set a path explicitly (versus the default filename).
func resolveEnvFilePath() (abs string, explicit bool, err error) {
	raw := rawEnvFilePath()
	explicit = raw != ""
	if raw == "" {
		raw = defaultEnvFileName
	}
	abs, err = absoluteFromInvocation(raw)
	return abs, explicit, err
}

func absoluteFromInvocation(p string) (string, error) {
	if p == "" {
		return "", nil
	}
	if filepath.IsAbs(p) {
		return filepath.Clean(p), nil
	}
	cwd, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("cannot resolve relative env file path: %w", err)
	}
	return filepath.Join(cwd, filepath.Clean(p)), nil
}

func envFilePathFromArgs() string {
	args := os.Args[1:]
	for i := 0; i < len(args); i++ {
		a := args[i]
		if strings.HasPrefix(a, "--env-file=") {
			return strings.TrimPrefix(a, "--env-file=")
		}
		if a == "--env-file" && i+1 < len(args) {
			return args[i+1]
		}
		if strings.HasPrefix(a, "-e=") {
			return strings.TrimPrefix(a, "-e=")
		}
		if a == "-e" && i+1 < len(args) {
			return args[i+1]
		}
	}
	return ""
}

// parseEnvFile reads KEY=VALUE lines (comments with # and blank lines skipped).
func parseEnvFile(path string) (map[string]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	out := make(map[string]string)
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		idx := strings.IndexByte(line, '=')
		if idx <= 0 {
			continue
		}
		key := strings.TrimSpace(line[:idx])
		val := strings.TrimSpace(line[idx+1:])
		val = unquoteEnvValue(val)
		out[key] = val
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func unquoteEnvValue(s string) string {
	if len(s) >= 2 {
		if s[0] == '"' && s[len(s)-1] == '"' {
			return s[1 : len(s)-1]
		}
		if s[0] == '\'' && s[len(s)-1] == '\'' {
			return s[1 : len(s)-1]
		}
	}
	return s
}

// applyEnvFileToFlags sets flag values from parsed file content for known DBDOCS_* keys only.
func applyEnvFileToFlags(fs *flag.FlagSet, data map[string]string) error {
	for key, val := range data {
		flagName, ok := envFileKeys[key]
		if !ok {
			continue
		}
		if err := fs.Set(flagName, val); err != nil {
			return fmt.Errorf("%s: %w", key, err)
		}
	}
	return nil
}
