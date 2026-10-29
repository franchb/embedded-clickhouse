// Command chversions keeps the ClickHouse version constants in config.go current. It
// moves each constant to the latest patch release of its line, adds a constant for each
// LTS line newer than all existing ones, and regenerates the version table in README.md.
//
// It reads the tag list from a file holding the output of
//
//	git ls-remote --tags https://github.com/ClickHouse/ClickHouse.git
//
// and takes a release only once every asset the library downloads for it is published.
// It never changes DefaultVersion and never removes a constant: both are API decisions
// left to a reviewer. The clickhouse-versions workflow runs it and opens a pull request.
package main

import (
	"bufio"
	"cmp"
	"context"
	"errors"
	"flag"
	"fmt"
	"go/format"
	"io"
	"net/http"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
)

// defaultBaseURL matches the library's default download location (platform.go).
const defaultBaseURL = "https://github.com/ClickHouse/ClickHouse/releases/download"

const (
	channelLTS = "lts"

	tableBegin = "<!-- versions:begin"
	tableEnd   = "<!-- versions:end -->"

	requestTimeout = 30 * time.Second
	filePerm       = 0o644
)

var (
	errNoConstants = errors.New("no version constants found")
	errNoDefault   = errors.New("no DefaultVersion constant found")
	errNoMarkers   = errors.New("version table markers not found")
	errNotFound    = errors.New("release asset not published")
)

var (
	tagRe     = regexp.MustCompile(`refs/tags/v(\d+)\.(\d+)\.(\d+)\.(\d+)-(lts|stable)$`)
	constRe   = regexp.MustCompile(`(?m)^const (V\d+_\d+) ClickHouseVersion = "([^"]+)"\n`)
	defaultRe = regexp.MustCompile(`(?m)^const DefaultVersion = (V\d+_\d+)$`)
)

// version is a ClickHouse release such as 26.8.34.13-lts.
type version struct {
	parts   [4]int
	channel string
}

// lineKey identifies a release line: all patches of 26.8 LTS share one key.
type lineKey struct {
	major, minor int
	channel      string
}

// constant is one exported version constant in config.go.
type constant struct {
	name       string
	ver        version
	deprecated bool
}

func parseVersion(s string) (version, bool) {
	var zero version

	match := tagRe.FindStringSubmatch("refs/tags/v" + s)
	if match == nil {
		return zero, false
	}

	var parts [4]int

	for i := range parts {
		num, err := strconv.Atoi(match[i+1])
		if err != nil {
			return zero, false
		}

		parts[i] = num
	}

	return version{parts: parts, channel: match[5]}, true
}

func (v version) String() string {
	return v.numeric() + "-" + v.channel
}

// numeric drops the channel suffix: 26.8.34.13.
func (v version) numeric() string {
	return fmt.Sprintf("%d.%d.%d.%d", v.parts[0], v.parts[1], v.parts[2], v.parts[3])
}

func (v version) key() lineKey {
	return lineKey{major: v.parts[0], minor: v.parts[1], channel: v.channel}
}

func (v version) compare(o version) int {
	return slices.Compare(v.parts[:], o.parts[:])
}

func (k lineKey) compare(o lineKey) int {
	return cmp.Or(cmp.Compare(k.major, o.major), cmp.Compare(k.minor, o.minor))
}

func (k lineKey) constName() string {
	return fmt.Sprintf("V%d_%d", k.major, k.minor)
}

// latestPerLine reads git ls-remote output and returns the newest release of each line.
// Peeled entries (^{}) and tags that are not lts or stable releases are ignored.
func latestPerLine(r io.Reader) (map[lineKey]version, error) {
	latest := map[lineKey]version{}

	sc := bufio.NewScanner(r)
	for sc.Scan() {
		match := tagRe.FindStringSubmatch(sc.Text())
		if match == nil {
			continue
		}

		ver, ok := parseVersion(strings.TrimPrefix(match[0], "refs/tags/v"))
		if !ok {
			continue
		}

		if cur, seen := latest[ver.key()]; !seen || cur.compare(ver) < 0 {
			latest[ver.key()] = ver
		}
	}

	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("read tags: %w", err)
	}

	return latest, nil
}

// docStart returns the offset of the doc comment directly above the line at pos, or pos
// when the line has none.
func docStart(src string, pos int) int {
	start := pos
	for start > 0 {
		prev := strings.LastIndex(src[:start-1], "\n") + 1
		if !strings.HasPrefix(src[prev:start], "//") {
			break
		}

		start = prev
	}

	return start
}

func parseConstants(src string) ([]constant, error) {
	var consts []constant

	for _, loc := range constRe.FindAllStringSubmatchIndex(src, -1) {
		ver, ok := parseVersion(src[loc[4]:loc[5]])
		if !ok {
			return nil, fmt.Errorf("%w: constant %s has unrecognized version %q",
				errNoConstants, src[loc[2]:loc[3]], src[loc[4]:loc[5]])
		}

		doc := src[docStart(src, loc[0]):loc[0]]
		consts = append(consts, constant{
			name:       src[loc[2]:loc[3]],
			ver:        ver,
			deprecated: strings.Contains(doc, "// Deprecated:"),
		})
	}

	if len(consts) == 0 {
		return nil, errNoConstants
	}

	return consts, nil
}

func constLine(name string, v version) string {
	return fmt.Sprintf("const %s ClickHouseVersion = %q\n", name, v.String())
}

// updater applies new releases to config.go. published reports whether all assets of
// a release can be downloaded; a release that is not ready yet is retried on a later run.
type updater struct {
	latest    map[lineKey]version
	published func(version) error
	changes   []string
	skipped   []string
}

func (u *updater) ready(v version) bool {
	if err := u.published(v); err != nil {
		u.skipped = append(u.skipped, fmt.Sprintf("%s: %v", v, err))

		return false
	}

	return true
}

// update returns config.go with every constant at the latest published patch of its
// line and new LTS lines added above the existing constants, newest first.
func (u *updater) update(src string) (string, error) {
	consts, err := parseConstants(src)
	if err != nil {
		return "", err
	}

	newest := consts[0].ver.key()

	for _, c := range consts {
		if c.ver.key().compare(newest) > 0 {
			newest = c.ver.key()
		}

		l, ok := u.latest[c.ver.key()]
		if !ok || l.compare(c.ver) <= 0 || !u.ready(l) {
			continue
		}

		src = strings.Replace(src, constLine(c.name, c.ver), constLine(c.name, l), 1)
		u.changes = append(u.changes, fmt.Sprintf("`%s`: %s → %s", c.name, c.ver, l))
	}

	added := u.newLTSLines(newest)
	if len(added) > 0 {
		first := constRe.FindStringIndex(src)[0]
		at := docStart(src, first)

		var buf strings.Builder

		for _, v := range slices.Backward(added) {
			k := v.key()
			fmt.Fprintf(&buf, "// %s is ClickHouse %d.%d (LTS channel).\n", k.constName(), k.major, k.minor)
			buf.WriteString(constLine(k.constName(), v) + "\n")

			u.changes = append(u.changes, fmt.Sprintf("`%s`: new LTS line, %s", k.constName(), v))
		}

		src = src[:at] + buf.String() + src[at:]
	}

	out, err := format.Source([]byte(src))
	if err != nil {
		return "", fmt.Errorf("format config: %w", err)
	}

	return string(out), nil
}

// newLTSLines returns the latest published release of each LTS line newer than newest,
// oldest first.
func (u *updater) newLTSLines(newest lineKey) []version {
	var added []version

	for k, v := range u.latest {
		if k.channel == channelLTS && k.compare(newest) > 0 && u.ready(v) {
			added = append(added, v)
		}
	}

	slices.SortFunc(added, func(a, b version) int { return a.compare(b) })

	return added
}

func versionTable(src string) (string, error) {
	consts, err := parseConstants(src)
	if err != nil {
		return "", err
	}

	def := defaultRe.FindStringSubmatch(src)
	if def == nil {
		return "", errNoDefault
	}

	var buf strings.Builder

	buf.WriteString("| Constant | Version | Channel |\n|----------|---------|---------|\n")

	for _, c := range consts {
		label := "Stable"
		if c.ver.channel == channelLTS {
			label = "LTS"
		}

		switch {
		case c.name == def[1]:
			label += " (default)"
		case c.deprecated:
			label += " (deprecated)"
		}

		fmt.Fprintf(&buf, "| `%s` | %s | %s |\n", c.name, c.ver, label)
	}

	return buf.String(), nil
}

// replaceTable swaps the lines between the begin and end markers for table.
func replaceTable(readme, table string) (string, error) {
	begin := strings.Index(readme, tableBegin)
	end := strings.Index(readme, tableEnd)

	if begin < 0 || end < begin {
		return "", errNoMarkers
	}

	bodyStart := begin + strings.Index(readme[begin:], "\n") + 1

	return readme[:bodyStart] + table + readme[end:], nil
}

// assetNames lists what the library downloads for v on its supported platforms
// (platform.go). macOS binaries ship without a .sha512 file.
func assetNames(v version) []string {
	linux := "clickhouse-common-static-" + v.numeric()

	return []string{
		linux + "-amd64.tgz",
		linux + "-amd64.tgz.sha512",
		linux + "-arm64.tgz",
		linux + "-arm64.tgz.sha512",
		"clickhouse-macos",
		"clickhouse-macos-aarch64",
	}
}

func checkPublished(ctx context.Context, client *http.Client, baseURL string, v version) error {
	for _, name := range assetNames(v) {
		url := fmt.Sprintf("%s/v%s/%s", baseURL, v, name)

		req, err := http.NewRequestWithContext(ctx, http.MethodHead, url, nil)
		if err != nil {
			return fmt.Errorf("build request: %w", err)
		}

		resp, err := client.Do(req)
		if err != nil {
			return fmt.Errorf("check %s: %w", name, err)
		}

		resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("%w: %s (HTTP %d)", errNotFound, name, resp.StatusCode)
		}
	}

	return nil
}

func summary(changes, skipped []string) string {
	var buf strings.Builder

	for _, c := range changes {
		buf.WriteString("- " + c + "\n")
	}

	if len(skipped) > 0 {
		buf.WriteString("\nNot ready yet (assets still uploading), retried on the next run:\n\n")

		for _, s := range skipped {
			buf.WriteString("- " + s + "\n")
		}
	}

	return buf.String()
}

func run(ctx context.Context, args []string, stdout io.Writer) error {
	flags := flag.NewFlagSet("chversions", flag.ContinueOnError)
	configPath := flags.String("config", "config.go", "Go file declaring the version constants")
	readmePath := flags.String("readme", "README.md", "README with the generated version table")
	tagsPath := flags.String("tags", "", "file with `git ls-remote --tags` output of ClickHouse")
	summaryPath := flags.String("summary", "", "write a Markdown list of the changes to this file")
	baseURL := flags.String("base-url", defaultBaseURL, "release download location to check assets on")

	if err := flags.Parse(args); err != nil {
		return fmt.Errorf("parse flags: %w", err)
	}

	tags, err := os.Open(*tagsPath)
	if err != nil {
		return fmt.Errorf("open tags: %w", err)
	}
	defer tags.Close()

	latest, err := latestPerLine(tags)
	if err != nil {
		return err
	}

	src, err := os.ReadFile(*configPath)
	if err != nil {
		return fmt.Errorf("read config: %w", err)
	}

	client := &http.Client{Timeout: requestTimeout}
	upd := &updater{latest: latest, published: func(v version) error {
		return checkPublished(ctx, client, *baseURL, v)
	}, changes: nil, skipped: nil}

	newSrc, err := upd.update(string(src))
	if err != nil {
		return err
	}

	readme, err := os.ReadFile(*readmePath)
	if err != nil {
		return fmt.Errorf("read readme: %w", err)
	}

	table, err := versionTable(newSrc)
	if err != nil {
		return err
	}

	newReadme, err := replaceTable(string(readme), table)
	if err != nil {
		return err
	}

	//nolint:gosec // G703: the paths are the maintainer's own command-line flags
	if err := os.WriteFile(*configPath, []byte(newSrc), filePerm); err != nil {
		return fmt.Errorf("write config: %w", err)
	}

	//nolint:gosec // G703: as above
	if err := os.WriteFile(*readmePath, []byte(newReadme), filePerm); err != nil {
		return fmt.Errorf("write readme: %w", err)
	}

	report := summary(upd.changes, upd.skipped)
	fmt.Fprint(stdout, report)

	if *summaryPath != "" {
		if err := os.WriteFile(*summaryPath, []byte(report), filePerm); err != nil {
			return fmt.Errorf("write summary: %w", err)
		}
	}

	return nil
}

func main() {
	if err := run(context.Background(), os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "chversions:", err)
		os.Exit(1)
	}
}
