package embeddedclickhouse

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFetch_StallAborts(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", "10")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("abc"))
		w.(http.Flusher).Flush()
		<-release // then go silent
	}))
	defer ts.Close()
	defer close(release)

	opts := fetchOptions{headers: nil, maxBytes: maxDownloadBytes, stall: 200 * time.Millisecond}

	start := time.Now()
	_, err := fetch(t.Context(), ts.URL, &bytes.Buffer{}, opts)

	require.ErrorIs(t, err, ErrDownloadStalled)
	assert.Less(t, time.Since(start), 5*time.Second)
}

func TestFetch_SlowButProgressingIsNotAStall(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		for range 6 {
			_, _ = w.Write([]byte("x"))
			w.(http.Flusher).Flush()
			time.Sleep(50 * time.Millisecond)
		}
	}))
	defer ts.Close()

	// Total transfer time (~300ms) exceeds the stall timeout, but data keeps arriving.
	opts := fetchOptions{headers: nil, maxBytes: maxDownloadBytes, stall: 150 * time.Millisecond}

	var buf bytes.Buffer

	status, err := fetch(t.Context(), ts.URL, &buf, opts)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, status)
	assert.Equal(t, "xxxxxx", buf.String())
}

func TestFetch_SizeLimit(t *testing.T) {
	t.Parallel()

	payload := strings.Repeat("x", 100)

	cases := map[string]http.HandlerFunc{
		"declared Content-Length": func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(payload))
		},
		"chunked, no Content-Length": func(w http.ResponseWriter, _ *http.Request) {
			for range 10 {
				_, _ = w.Write([]byte(payload[:10]))
				w.(http.Flusher).Flush()
			}
		},
	}

	for name, handler := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			ts := httptest.NewServer(handler)
			defer ts.Close()

			opts := fetchOptions{headers: nil, maxBytes: 50, stall: time.Minute}

			_, err := fetch(t.Context(), ts.URL, &bytes.Buffer{}, opts)
			require.ErrorIs(t, err, ErrDownloadTooLarge)
		})
	}
}

func TestDownloadFile_RemovesPartialFileOnError(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(strings.Repeat("x", 100)))
	}))
	defer ts.Close()

	dest := filepath.Join(t.TempDir(), "archive")

	_, err := downloadFile(t.Context(), ts.URL, dest, fetchOptions{headers: nil, maxBytes: 10, stall: time.Minute})
	require.ErrorIs(t, err, ErrDownloadTooLarge)

	_, statErr := os.Stat(dest)
	assert.True(t, os.IsNotExist(statErr), "partial download must be removed")
}

func TestEnsureBinaryContext_CanceledAbortsDownload(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})

	ts := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		<-release
	}))
	defer ts.Close()
	defer close(release)

	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()

	cfg := DefaultConfig().
		CustomArchiveURL(ts.URL + "/ch.tgz").
		CachePath(t.TempDir()).
		Logger(nil)

	_, err := EnsureBinaryContext(ctx, cfg)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestDownloadHeader_SentToCustomArchiveURL(t *testing.T) {
	t.Parallel()

	archive := createTestArchive(t, t.TempDir())

	body, err := os.ReadFile(archive)
	require.NoError(t, err)

	var gotAuth, gotToken []string

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Values("Authorization")
		gotToken = r.Header.Values("Private-Token")
		_, _ = w.Write(body)
	}))
	defer ts.Close()

	cfg := DefaultConfig().
		CustomArchiveURL(ts.URL+"/ch.tgz").
		CachePath(t.TempDir()).
		Logger(nil).
		DownloadHeader("Authorization", "Bearer s3cret").
		DownloadHeader("PRIVATE-TOKEN", "glpat-x")

	_, err = ensureBinary(t.Context(), cfg)
	require.NoError(t, err)

	assert.Equal(t, []string{"Bearer s3cret"}, gotAuth)
	assert.Equal(t, []string{"glpat-x"}, gotToken)
}

// TestDownloadHeader_DroppedOnCrossHostRedirect: a registry that redirects to another
// host (e.g. object storage) must not receive the credentials meant for the registry.
// PRIVATE-TOKEN is not on net/http's own sensitive-header list, so without the redirect
// policy it would be forwarded.
func TestDownloadHeader_DroppedOnCrossHostRedirect(t *testing.T) {
	t.Parallel()

	var storageAuth, storageToken []string

	storage := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		storageAuth = r.Header.Values("Authorization")
		storageToken = r.Header.Values("Private-Token")
		_, _ = w.Write([]byte("payload"))
	}))
	defer storage.Close()

	var registryToken []string

	registry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		registryToken = r.Header.Values("Private-Token")
		http.Redirect(w, r, storage.URL+"/blob", http.StatusFound)
	}))
	defer registry.Close()

	headers := http.Header{}
	headers.Set("Authorization", "Bearer s3cret")
	headers.Set("Private-Token", "glpat-x")

	var buf bytes.Buffer

	status, err := fetch(t.Context(), registry.URL+"/pkg", &buf, downloadOptions(headers, maxDownloadBytes))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, status)
	assert.Equal(t, "payload", buf.String())

	assert.Equal(t, []string{"glpat-x"}, registryToken, "the chosen host still gets the header")
	assert.Empty(t, storageToken, "custom header leaked across a cross-host redirect")
	assert.Empty(t, storageAuth, "Authorization leaked across a cross-host redirect")
}

// TestDownloadHeader_KeptOnSameHostRedirect: a redirect within the chosen host keeps
// the headers, so a registry's own path redirects still authenticate.
func TestDownloadHeader_KeptOnSameHostRedirect(t *testing.T) {
	t.Parallel()

	var finalToken []string

	mux := http.NewServeMux()
	mux.HandleFunc("/pkg", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/blob", http.StatusFound)
	})
	mux.HandleFunc("/blob", func(w http.ResponseWriter, r *http.Request) {
		finalToken = r.Header.Values("Private-Token")
		_, _ = w.Write([]byte("payload"))
	})

	ts := httptest.NewServer(mux)
	defer ts.Close()

	headers := http.Header{}
	headers.Set("Private-Token", "glpat-x")

	_, err := fetch(t.Context(), ts.URL+"/pkg", &bytes.Buffer{}, downloadOptions(headers, maxDownloadBytes))
	require.NoError(t, err)
	assert.Equal(t, []string{"glpat-x"}, finalToken)
}

func TestStripHeadersOffOrigin_RedirectLimit(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/again", http.StatusFound)
	}))
	defer ts.Close()

	_, err := fetch(t.Context(), ts.URL, &bytes.Buffer{}, downloadOptions(nil, maxDownloadBytes))
	require.ErrorIs(t, err, errTooManyRedirects)
}

func TestDownloadHeader_NotSentToDefaultSource(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig().DownloadHeader("Authorization", "Bearer s3cret")

	assert.Nil(t, userHeaders(cfg, false), "headers must never go to the default GitHub source")
	assert.Equal(t, "Bearer s3cret", userHeaders(cfg, true).Get("Authorization"))
}

func TestDownloadHeader_Immutable(t *testing.T) {
	t.Parallel()

	base := DefaultConfig().DownloadHeader("X-A", "1")
	derived := base.DownloadHeader("X-A", "2")

	assert.Equal(t, []string{"1"}, base.httpHeaders.Values("X-A"))
	assert.Equal(t, []string{"1", "2"}, derived.httpHeaders.Values("X-A"))
}

func TestCachedBinaryPath_KeyedByRepositoryURL(t *testing.T) {
	t.Parallel()

	def := cachedBinaryPath("/c", testVersion, "")

	assert.Equal(t, def, cachedBinaryPath("/c", testVersion, defaultBaseURL), "explicit default URL keeps the default entry")
	assert.Equal(t, def, cachedBinaryPath("/c", testVersion, defaultBaseURL+"/"))

	mirrorA := cachedBinaryPath("/c", testVersion, "https://mirror-a.example.com/releases")
	mirrorB := cachedBinaryPath("/c", testVersion, "https://mirror-b.example.com/releases")

	assert.NotEqual(t, def, mirrorA)
	assert.NotEqual(t, mirrorA, mirrorB)
	assert.Equal(t, mirrorA, cachedBinaryPath("/c", testVersion, "https://mirror-a.example.com/releases/"))
}

func TestCacheDir_RelativeOverrideIsAbsolute(t *testing.T) {
	t.Parallel()

	dir, err := cacheDir("../shared-cache")
	require.NoError(t, err)
	assert.True(t, filepath.IsAbs(dir))
	assert.NotContains(t, dir, "..")
}

func TestWriteExecutable_DotDotGuard(t *testing.T) {
	t.Parallel()

	root := t.TempDir()

	// ".." inside a name is not traversal and must be accepted.
	ok := filepath.Join(root, "ci..runner", "clickhouse")
	require.NoError(t, writeExecutable(strings.NewReader("bin"), ok))

	// A ".." segment is rejected even though Clean would fold it away.
	bad := root + "/sub/../../escape/clickhouse"
	require.ErrorIs(t, writeExecutable(strings.NewReader("bin"), bad), ErrInvalidPath)

	_, err := os.Stat(filepath.Join(filepath.Dir(root), "escape"))
	assert.True(t, os.IsNotExist(err), "nothing may be written outside root")
}

func TestExtractClickHouseBinary_SizeLimit(t *testing.T) {
	t.Parallel()

	archive := createTestArchive(t, t.TempDir())
	dest := filepath.Join(t.TempDir(), "clickhouse")

	err := extractClickHouseBinaryLimit(archive, dest, 4)
	require.ErrorIs(t, err, ErrBinaryTooLarge)

	_, statErr := os.Stat(dest)
	require.ErrorIs(t, statErr, os.ErrNotExist)

	require.NoError(t, extractClickHouseBinaryLimit(archive, dest, maxBinaryBytes))
}
