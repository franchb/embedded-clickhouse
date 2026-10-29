package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testTags = `aaa	refs/tags/v26.3.2.3-lts
bbb	refs/tags/v26.3.60.2-lts
ccc	refs/tags/v26.3.60.2-lts^{}
ddd	refs/tags/v26.3.9.1-lts
eee	refs/tags/v26.1.12.23-stable
fff	refs/tags/v26.8.34.13-lts
ggg	refs/tags/v26.9.25.15-stable
hhh	refs/tags/v26.11.1.1-testing
iii	refs/tags/v27.3.1.5-lts
`

const testConfig = `package embeddedclickhouse

// ClickHouseVersion represents a ClickHouse server version string.
type ClickHouseVersion string

// Version constants, newest first.

// V26_3 is ClickHouse 26.3 (LTS channel).
const V26_3 ClickHouseVersion = "26.3.2.3-lts"

// V26_1 is ClickHouse 26.1 (stable channel).
//
// Deprecated: unsupported.
const V26_1 ClickHouseVersion = "26.1.12.23-stable"

// DefaultVersion is the default ClickHouse version used when none is specified.
const DefaultVersion = V26_3
`

// Command-line flags of run, as tests pass them.
const (
	flagConfig = "-config"
	flagReadme = "-readme"
	flagTags   = "-tags"
)

func mustVersion(t *testing.T, s string) version {
	t.Helper()

	v, ok := parseVersion(s)
	require.True(t, ok, s)

	return v
}

func TestLatestPerLine(t *testing.T) {
	t.Parallel()

	latest, err := latestPerLine(strings.NewReader(testTags))
	require.NoError(t, err)

	// 26.3.60.2 beats 26.3.9.1 numerically, not lexically; testing tags are ignored.
	assert.Equal(t, "26.3.60.2-lts", latest[lineKey{26, 3, "lts"}].String())
	assert.Equal(t, "26.1.12.23-stable", latest[lineKey{26, 1, "stable"}].String())
	assert.Len(t, latest, 5)
}

func TestUpdate(t *testing.T) {
	t.Parallel()

	latest, err := latestPerLine(strings.NewReader(testTags))
	require.NoError(t, err)

	upd := &updater{latest: latest, published: func(v version) error {
		if v.String() == "27.3.1.5-lts" {
			return errNotFound
		}

		return nil
	}, changes: nil, skipped: nil}

	got, err := upd.update(testConfig)
	require.NoError(t, err)

	assert.Contains(t, got, `const V26_3 ClickHouseVersion = "26.3.60.2-lts"`)
	assert.NotContains(t, got, "V26_9", "stable lines are never added")
	assert.NotContains(t, got, "V27_3", "an unpublished release is not taken")
	assert.Contains(t, got, "const DefaultVersion = V26_3", "the default is never changed")

	// The new LTS line goes above the existing constants, after the group comment.
	assert.Contains(t, got, "// Version constants, newest first.\n\n"+
		"// V26_8 is ClickHouse 26.8 (LTS channel).\n"+
		`const V26_8 ClickHouseVersion = "26.8.34.13-lts"`+"\n\n"+
		"// V26_3 is ClickHouse 26.3 (LTS channel).\n")

	assert.Equal(t, []string{
		"`V26_3`: 26.3.2.3-lts → 26.3.60.2-lts",
		"`V26_8`: new LTS line, 26.8.34.13-lts",
	}, upd.changes)
	require.Len(t, upd.skipped, 1)
	assert.Contains(t, upd.skipped[0], "27.3.1.5-lts")

	// A second run over the result changes nothing.
	again := &updater{latest: latest, published: upd.published, changes: nil, skipped: nil}
	got2, err := again.update(got)
	require.NoError(t, err)
	assert.Equal(t, got, got2)
	assert.Empty(t, again.changes)
}

func TestVersionTableAndReplace(t *testing.T) {
	t.Parallel()

	table, err := versionTable(testConfig)
	require.NoError(t, err)
	assert.Equal(t, "| Constant | Version | Channel |\n|----------|---------|---------|\n"+
		"| `V26_3` | 26.3.2.3-lts | LTS (default) |\n"+
		"| `V26_1` | 26.1.12.23-stable | Stable (deprecated) |\n", table)

	readme := "intro\n<!-- versions:begin (generated) -->\nold\nrows\n<!-- versions:end -->\noutro\n"
	got, err := replaceTable(readme, table)
	require.NoError(t, err)
	assert.Equal(t, "intro\n<!-- versions:begin (generated) -->\n"+table+"<!-- versions:end -->\noutro\n", got)

	_, err = replaceTable("no markers", table)
	require.ErrorIs(t, err, errNoMarkers)
}

func TestCheckPublished(t *testing.T) {
	t.Parallel()

	ver := mustVersion(t, "26.8.34.13-lts")
	missing := "clickhouse-macos-aarch64"

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/"+missing) || !strings.HasPrefix(r.URL.Path, "/v26.8.34.13-lts/") {
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)

	err := checkPublished(context.Background(), srv.Client(), srv.URL, ver)
	require.ErrorIs(t, err, errNotFound)
	assert.Contains(t, err.Error(), missing)

	missing = "none"

	require.NoError(t, checkPublished(context.Background(), srv.Client(), srv.URL, ver))
}

// TestRun drives the command end to end against a stub release server on which every
// asset exists except those of 27.3.1.5-lts.
func TestRun(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/v27.3.1.5-lts/") {
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)

	dir := t.TempDir()
	path := func(name string) string { return filepath.Join(dir, name) }

	readme := "# versions\n<!-- versions:begin -->\nstale\n<!-- versions:end -->\n"

	require.NoError(t, os.WriteFile(path("config.go"), []byte(testConfig), 0o600))
	require.NoError(t, os.WriteFile(path("README.md"), []byte(readme), 0o600))
	require.NoError(t, os.WriteFile(path("tags.txt"), []byte(testTags), 0o600))

	var out strings.Builder

	err := run(t.Context(), []string{
		flagConfig, path("config.go"), flagReadme, path("README.md"), flagTags, path("tags.txt"),
		"-summary", path("summary.md"), "-base-url", srv.URL,
	}, &out)
	require.NoError(t, err)

	config, err := os.ReadFile(path("config.go"))
	require.NoError(t, err)
	assert.Contains(t, string(config), `const V26_8 ClickHouseVersion = "26.8.34.13-lts"`)

	gotReadme, err := os.ReadFile(path("README.md"))
	require.NoError(t, err)
	assert.Contains(t, string(gotReadme), "| `V26_8` | 26.8.34.13-lts | LTS |\n")
	assert.NotContains(t, string(gotReadme), "stale")

	summaryFile, err := os.ReadFile(path("summary.md"))
	require.NoError(t, err)
	assert.Equal(t, out.String(), string(summaryFile))
	assert.Contains(t, out.String(), "- `V26_3`: 26.3.2.3-lts → 26.3.60.2-lts\n")
	assert.Contains(t, out.String(), "Not ready yet")
	assert.Contains(t, out.String(), "27.3.1.5-lts: release asset not published")
}

func TestRun_Errors(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	tags := filepath.Join(dir, "tags.txt")
	require.NoError(t, os.WriteFile(tags, []byte(testTags), 0o600))

	noMarkers := filepath.Join(dir, "README.md")
	require.NoError(t, os.WriteFile(noMarkers, []byte("no table here\n"), 0o600))

	config := filepath.Join(dir, "config.go")
	require.NoError(t, os.WriteFile(config, []byte(testConfig), 0o600))

	noConstants := filepath.Join(dir, "empty.go")
	require.NoError(t, os.WriteFile(noConstants, []byte("package embeddedclickhouse\n"), 0o600))

	noDefault := filepath.Join(dir, "nodefault.go")
	require.NoError(t, os.WriteFile(noDefault,
		[]byte(strings.Replace(testConfig, "const DefaultVersion = V26_3\n", "", 1)), 0o600))

	readme := filepath.Join(dir, "TABLE.md")
	require.NoError(t, os.WriteFile(readme, []byte("<!-- versions:begin -->\n<!-- versions:end -->\n"), 0o600))

	tests := []struct {
		name string
		args []string
	}{
		{"unknown flag", []string{"-nope"}},
		{"missing tags", []string{flagTags, filepath.Join(dir, "absent")}},
		{"missing config", []string{flagTags, tags, flagConfig, filepath.Join(dir, "absent.go")}},
		{"readme without markers", []string{flagTags, tags, flagConfig, config, flagReadme, noMarkers}},
		{"missing readme", []string{flagTags, tags, flagConfig, config, flagReadme, filepath.Join(dir, "absent.md")}},
		{"no constants", []string{flagTags, tags, flagConfig, noConstants, flagReadme, readme}},
		{"no default", []string{flagTags, tags, flagConfig, noDefault, flagReadme, readme}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Every asset check fails fast against an unreachable base URL.
			args := slices.Concat(tt.args, []string{"-base-url", "http://127.0.0.1:1"})
			require.Error(t, run(t.Context(), args, io.Discard))
		})
	}

	// A failed run leaves config.go as it was.
	got, err := os.ReadFile(config)
	require.NoError(t, err)
	assert.Equal(t, testConfig, string(got))
}
