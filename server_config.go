package embeddedclickhouse

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"text/template"
)

// commonConfigTemplates holds the config blocks shared by single-node and cluster
// configs. Both templates are parsed together with it, so the shared parts cannot drift.
const commonConfigTemplates = `
{{- define "logger"}}
    <logger>
        <level>warning</level>
        <console>1</console>
    </logger>
{{- end}}

{{- define "listen"}}
{{- if .ListenHost}}
    <listen_host>{{.ListenHost}}</listen_host>
{{- end}}
{{- end}}

{{- define "paths"}}
    <path>{{xmlEscape .DataDir}}/</path>
    <tmp_path>{{xmlEscape .TmpDir}}/</tmp_path>
    <user_files_path>{{xmlEscape .UserFilesDir}}/</user_files_path>
    <format_schema_path>{{xmlEscape .FormatSchemaDir}}/</format_schema_path>
{{- end}}

{{- define "access"}}
    <users>
        <default>
            <password></password>
            <networks>
                <ip>::1</ip>
                <ip>127.0.0.1</ip>
            </networks>
            <profile>default</profile>
            <quota>default</quota>
            <access_management>1</access_management>
        </default>
    </users>

    <profiles>
        <default/>
    </profiles>

    <quotas>
        <default/>
    </quotas>
{{- end}}

{{- define "settings"}}
{{- range $key, $value := .Settings}}
    <{{$key}}>{{xmlEscape $value}}</{{$key}}>
{{- end}}
{{- end -}}
`

const configTemplate = `<?xml version="1.0"?>
<clickhouse>
{{- template "logger" .}}
{{- template "listen" .}}

    <tcp_port>{{.TCPPort}}</tcp_port>
    <http_port>{{.HTTPPort}}</http_port>
{{template "paths" .}}
{{template "access" .}}
{{- template "settings" .}}
</clickhouse>
`

// serverManagedKeys are the top-level elements the single-node template writes itself.
// A user setting with one of these names would emit a second, conflicting element.
//
//nolint:gochecknoglobals // immutable lookup table
var serverManagedKeys = []string{
	"logger", "tcp_port", "http_port",
	"path", "tmp_path", "user_files_path", "format_schema_path",
	"users", "profiles", "quotas",
}

// validSettingKey matches safe XML element names for ClickHouse settings.
var validSettingKey = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_]*$`)

// xmlEscapeString escapes a string so it is safe to embed in an XML text node.
func xmlEscapeString(s string) string {
	var buf bytes.Buffer

	_ = xml.EscapeText(&buf, []byte(s))

	return buf.String()
}

// newConfigTemplate parses a config template together with the shared blocks.
func newConfigTemplate(name, body string) *template.Template {
	return template.Must(template.New(name).Funcs(template.FuncMap{
		"xmlEscape": xmlEscapeString,
	}).Parse(commonConfigTemplates + body))
}

//nolint:gochecknoglobals // compile once, reuse
var configTmpl = newConfigTemplate("config", configTemplate)

// baseConfigData holds the template fields shared by single-node and cluster configs.
type baseConfigData struct {
	// ListenHost is written as <listen_host> unless empty. An explicit listen_host
	// turns off ClickHouse's implicit listen_try mode, in which a failed bind is only
	// logged as a warning and the server keeps running without that listener.
	ListenHost      string
	TCPPort         uint32
	HTTPPort        uint32
	DataDir         string
	TmpDir          string
	UserFilesDir    string
	FormatSchemaDir string
	Settings        map[string]string
}

// newBaseConfigData lays out the standard server directories under dir.
func newBaseConfigData(dir string, tcpPort, httpPort uint32, settings map[string]string) baseConfigData {
	return baseConfigData{
		ListenHost:      defaultListenHost(settings),
		TCPPort:         tcpPort,
		HTTPPort:        httpPort,
		DataDir:         filepath.Join(dir, "data"),
		TmpDir:          filepath.Join(dir, "tmp"),
		UserFilesDir:    filepath.Join(dir, "user_files"),
		FormatSchemaDir: filepath.Join(dir, "format_schemas"),
		Settings:        settings,
	}
}

// defaultListenHost returns the loopback address to listen on, or "" when the user
// supplies their own listen_host through Settings. Either way listen_host is set, so
// ClickHouse aborts startup if it cannot bind a port rather than silently skipping it.
func defaultListenHost(settings map[string]string) string {
	if _, ok := settings["listen_host"]; ok {
		return ""
	}

	return loopbackHost
}

// dirs returns the directories that must exist before the server starts.
func (b baseConfigData) dirs() []string {
	return []string{b.DataDir, b.TmpDir, b.UserFilesDir, b.FormatSchemaDir}
}

// validateSettings rejects setting keys that are not safe XML element names, or that
// name an element the config template already writes (managed).
func validateSettings(settings map[string]string, managed []string) error {
	for key := range settings {
		if !validSettingKey.MatchString(key) {
			return fmt.Errorf("%w: %q (must match [a-zA-Z][a-zA-Z0-9_]*)", ErrInvalidSettingKey, key)
		}

		if slices.Contains(managed, key) {
			return fmt.Errorf("%w: %q is managed by embedded-clickhouse and cannot be overridden",
				ErrInvalidSettingKey, key)
		}
	}

	return nil
}

// makeDirs creates every directory in dirs.
func makeDirs(dirs ...string) error {
	for _, d := range dirs {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return fmt.Errorf("embedded-clickhouse: create dir %s: %w", d, err)
		}
	}

	return nil
}

// writeConfigFile renders tmpl into path. On any failure the partial file is removed,
// so a half-written config is never left behind.
func writeConfigFile(path string, tmpl *template.Template, data any) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("embedded-clickhouse: create config: %w", err)
	}

	if err := tmpl.Execute(f, data); err != nil {
		f.Close()
		os.Remove(path)

		return fmt.Errorf("embedded-clickhouse: write config: %w", err)
	}

	if err := f.Close(); err != nil {
		os.Remove(path)

		return fmt.Errorf("embedded-clickhouse: close config: %w", err)
	}

	return nil
}

// writeServerConfig generates a ClickHouse XML config file in the given directory.
func writeServerConfig(dir string, tcpPort, httpPort uint32, settings map[string]string) (string, error) {
	if err := validateSettings(settings, serverManagedKeys); err != nil {
		return "", err
	}

	data := newBaseConfigData(dir, tcpPort, httpPort, settings)

	if err := makeDirs(data.dirs()...); err != nil {
		return "", err
	}

	configPath := filepath.Join(dir, "config.xml")

	if err := writeConfigFile(configPath, configTmpl, data); err != nil {
		return "", err
	}

	return configPath, nil
}
