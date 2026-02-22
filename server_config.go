package embeddedclickhouse

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"text/template"
)

const configTemplate = `<?xml version="1.0"?>
<clickhouse>
    <logger>
        <level>warning</level>
        <console>1</console>
    </logger>

    <tcp_port>{{.TCPPort}}</tcp_port>
    <http_port>{{.HTTPPort}}</http_port>

    <path>{{xmlEscape .DataDir}}/</path>
    <tmp_path>{{xmlEscape .TmpDir}}/</tmp_path>
    <user_files_path>{{xmlEscape .UserFilesDir}}/</user_files_path>
    <format_schema_path>{{xmlEscape .FormatSchemaDir}}/</format_schema_path>

    <!-- 1 GiB default; override via Settings({"max_server_memory_usage": "..."}) -->
    <max_server_memory_usage>1073741824</max_server_memory_usage>

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
{{range $key, $value := .Settings}}
    <{{$key}}>{{xmlEscape $value}}</{{$key}}>
{{end}}
</clickhouse>
`

// validSettingKey matches safe XML element names for ClickHouse settings.
var validSettingKey = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_]*$`)

// xmlEscapeString escapes a string so it is safe to embed in an XML text node.
func xmlEscapeString(s string) string {
	var buf bytes.Buffer

	_ = xml.EscapeText(&buf, []byte(s))

	return buf.String()
}

//nolint:gochecknoglobals // compile once, reuse
var configTmpl = template.Must(template.New("config").Funcs(template.FuncMap{
	"xmlEscape": xmlEscapeString,
}).Parse(configTemplate))

type serverConfigData struct {
	TCPPort         uint32
	HTTPPort        uint32
	DataDir         string
	TmpDir          string
	UserFilesDir    string
	FormatSchemaDir string
	Settings        map[string]string
}

// writeServerConfig generates a ClickHouse XML config file in the given directory.
func writeServerConfig(dir string, tcpPort, httpPort uint32, settings map[string]string) (string, error) {
	for k := range settings {
		if !validSettingKey.MatchString(k) {
			return "", fmt.Errorf("%w: %q (must match [a-zA-Z][a-zA-Z0-9_]*)", ErrInvalidSettingKey, k)
		}
	}

	dataDir := filepath.Join(dir, "data")
	tmpDir := filepath.Join(dir, "tmp")
	userFilesDir := filepath.Join(dir, "user_files")
	formatSchemaDir := filepath.Join(dir, "format_schemas")

	for _, d := range []string{dataDir, tmpDir, userFilesDir, formatSchemaDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return "", fmt.Errorf("embedded-clickhouse: create dir %s: %w", d, err)
		}
	}

	configPath := filepath.Join(dir, "config.xml")

	f, err := os.Create(configPath)
	if err != nil {
		return "", fmt.Errorf("embedded-clickhouse: create config: %w", err)
	}

	data := serverConfigData{
		TCPPort:         tcpPort,
		HTTPPort:        httpPort,
		DataDir:         dataDir,
		TmpDir:          tmpDir,
		UserFilesDir:    userFilesDir,
		FormatSchemaDir: formatSchemaDir,
		Settings:        settings,
	}

	if err := configTmpl.Execute(f, data); err != nil {
		f.Close()
		return "", fmt.Errorf("embedded-clickhouse: write config: %w", err)
	}

	if err := f.Close(); err != nil {
		return "", fmt.Errorf("embedded-clickhouse: close config: %w", err)
	}

	return configPath, nil
}
