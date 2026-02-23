package embeddedclickhouse

import (
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"text/template"
)

const clusterConfigTemplate = `<?xml version="1.0"?>
<clickhouse>
    <logger>
        <level>warning</level>
        <console>1</console>
    </logger>

    <tcp_port>{{.TCPPort}}</tcp_port>
    <http_port>{{.HTTPPort}}</http_port>
    <interserver_http_port>{{.InterserverPort}}</interserver_http_port>
    <interserver_http_host>127.0.0.1</interserver_http_host>

    <path>{{xmlEscape .DataDir}}/</path>
    <tmp_path>{{xmlEscape .TmpDir}}/</tmp_path>
    <user_files_path>{{xmlEscape .UserFilesDir}}/</user_files_path>
    <format_schema_path>{{xmlEscape .FormatSchemaDir}}/</format_schema_path>

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

    <keeper_server>
        <tcp_port>{{.KeeperPort}}</tcp_port>
        <server_id>{{.ServerID}}</server_id>
        <log_storage_path>{{xmlEscape .KeeperLogDir}}/</log_storage_path>
        <snapshot_storage_path>{{xmlEscape .KeeperSnapshotDir}}/</snapshot_storage_path>
        <coordination_settings>
            <operation_timeout_ms>10000</operation_timeout_ms>
            <session_timeout_ms>30000</session_timeout_ms>
            <raft_logs_level>warning</raft_logs_level>
        </coordination_settings>
        <raft_configuration>
{{- range .RaftServers}}
            <server>
                <id>{{.ID}}</id>
                <hostname>127.0.0.1</hostname>
                <port>{{.Port}}</port>
            </server>
{{- end}}
        </raft_configuration>
    </keeper_server>

    <zookeeper>
{{- range .KeeperNodes}}
        <node>
            <host>127.0.0.1</host>
            <port>{{.Port}}</port>
        </node>
{{- end}}
    </zookeeper>

    <remote_servers>
        <test_cluster>
            <shard>
                <internal_replication>true</internal_replication>
{{- range .ClusterReplicas}}
                <replica>
                    <host>127.0.0.1</host>
                    <port>{{.Port}}</port>
                </replica>
{{- end}}
            </shard>
        </test_cluster>
    </remote_servers>

    <distributed_ddl>
        <path>/clickhouse/task_queue/ddl</path>
    </distributed_ddl>

    <macros>
        <shard>01</shard>
        <replica>{{.ReplicaName}}</replica>
    </macros>
{{range $key, $value := .Settings}}
    <{{$key}}>{{xmlEscape $value}}</{{$key}}>
{{- end}}
</clickhouse>
`

//nolint:gochecknoglobals // compile once, reuse
var clusterConfigTmpl = template.Must(template.New("cluster-config").Funcs(template.FuncMap{
	"xmlEscape": xmlEscapeString,
}).Parse(clusterConfigTemplate))

// raftServer describes one server entry inside <raft_configuration>.
type raftServer struct {
	ID   int
	Port uint32
}

// keeperNode describes one <node> entry inside <zookeeper>.
type keeperNode struct {
	Port uint32
}

// clusterReplica describes one <replica> entry inside <remote_servers>.
type clusterReplica struct {
	Port uint32
}

// clusterNodePorts holds the 5 allocated ports for a single cluster node.
type clusterNodePorts struct {
	TCP         uint32
	HTTP        uint32
	Interserver uint32
	Keeper      uint32
	KeeperRaft  uint32
}

// clusterTopology is pre-computed shared topology built from all node ports.
type clusterTopology struct {
	Nodes    []clusterNodePorts
	Settings map[string]string
}

// clusterNodeConfigData is the template data for a single cluster node.
type clusterNodeConfigData struct {
	TCPPort           uint32
	HTTPPort          uint32
	InterserverPort   uint32
	KeeperPort        uint32
	ServerID          int
	DataDir           string
	TmpDir            string
	UserFilesDir      string
	FormatSchemaDir   string
	KeeperLogDir      string
	KeeperSnapshotDir string
	ReplicaName       string
	RaftServers       []raftServer
	KeeperNodes       []keeperNode
	ClusterReplicas   []clusterReplica
	Settings          map[string]string
}

// buildClusterTopology creates a clusterTopology from allocated ports and user settings.
func buildClusterTopology(ports []clusterNodePorts, settings map[string]string) clusterTopology {
	merged := make(map[string]string, len(settings))
	maps.Copy(merged, settings)

	return clusterTopology{
		Nodes:    ports,
		Settings: merged,
	}
}

// writeClusterNodeConfig generates a ClickHouse XML config for one cluster node.
func writeClusterNodeConfig(dir string, nodeIndex int, topo clusterTopology) (string, error) {
	for k := range topo.Settings {
		if !validSettingKey.MatchString(k) {
			return "", fmt.Errorf("%w: %q (must match [a-zA-Z][a-zA-Z0-9_]*)", ErrInvalidSettingKey, k)
		}
	}

	node := topo.Nodes[nodeIndex]

	dataDir := filepath.Join(dir, "data")
	tmpDir := filepath.Join(dir, "tmp")
	userFilesDir := filepath.Join(dir, "user_files")
	formatSchemaDir := filepath.Join(dir, "format_schemas")
	keeperLogDir := filepath.Join(dir, "coordination", "log")
	keeperSnapshotDir := filepath.Join(dir, "coordination", "snapshots")

	for _, d := range []string{dataDir, tmpDir, userFilesDir, formatSchemaDir, keeperLogDir, keeperSnapshotDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return "", fmt.Errorf("embedded-clickhouse: create dir %s: %w", d, err)
		}
	}

	raftServers := make([]raftServer, len(topo.Nodes))
	keeperNodes := make([]keeperNode, len(topo.Nodes))
	clusterReplicas := make([]clusterReplica, len(topo.Nodes))

	for i, n := range topo.Nodes {
		raftServers[i] = raftServer{ID: i + 1, Port: n.KeeperRaft}
		keeperNodes[i] = keeperNode{Port: n.Keeper}
		clusterReplicas[i] = clusterReplica{Port: n.TCP}
	}

	data := clusterNodeConfigData{
		TCPPort:           node.TCP,
		HTTPPort:          node.HTTP,
		InterserverPort:   node.Interserver,
		KeeperPort:        node.Keeper,
		ServerID:          nodeIndex + 1,
		DataDir:           dataDir,
		TmpDir:            tmpDir,
		UserFilesDir:      userFilesDir,
		FormatSchemaDir:   formatSchemaDir,
		KeeperLogDir:      keeperLogDir,
		KeeperSnapshotDir: keeperSnapshotDir,
		ReplicaName:       fmt.Sprintf("replica_%02d", nodeIndex+1),
		RaftServers:       raftServers,
		KeeperNodes:       keeperNodes,
		ClusterReplicas:   clusterReplicas,
		Settings:          topo.Settings,
	}

	configPath := filepath.Join(dir, "config.xml")

	f, err := os.Create(configPath)
	if err != nil {
		return "", fmt.Errorf("embedded-clickhouse: create config: %w", err)
	}

	if err := clusterConfigTmpl.Execute(f, data); err != nil {
		f.Close()

		return "", fmt.Errorf("embedded-clickhouse: write cluster config: %w", err)
	}

	if err := f.Close(); err != nil {
		return "", fmt.Errorf("embedded-clickhouse: close config: %w", err)
	}

	return configPath, nil
}
