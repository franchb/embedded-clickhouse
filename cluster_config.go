package embeddedclickhouse

import (
	"fmt"
	"maps"
	"path/filepath"
	"slices"
)

const clusterConfigTemplate = `<?xml version="1.0"?>
<clickhouse>
{{- template "logger" .}}
{{- template "listen" .}}

    <tcp_port>{{.TCPPort}}</tcp_port>
    <http_port>{{.HTTPPort}}</http_port>
    <interserver_http_port>{{.InterserverPort}}</interserver_http_port>
    <interserver_http_host>127.0.0.1</interserver_http_host>
{{template "paths" .}}
{{template "access" .}}

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
        <cluster>test_cluster</cluster>
    </macros>
{{- template "settings" .}}
</clickhouse>
`

// clusterManagedKeys are the top-level elements the cluster template writes itself,
// on top of the single-node ones.
//
//nolint:gochecknoglobals // immutable lookup table
var clusterManagedKeys = append(slices.Clone(serverManagedKeys),
	"interserver_http_port", "interserver_http_host",
	"keeper_server", "zookeeper", "remote_servers", "distributed_ddl", "macros",
)

//nolint:gochecknoglobals // compile once, reuse
var clusterConfigTmpl = newConfigTemplate("cluster-config", clusterConfigTemplate)

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

// all returns every port of the node.
func (p clusterNodePorts) all() []uint32 {
	return []uint32{p.TCP, p.HTTP, p.Interserver, p.Keeper, p.KeeperRaft}
}

// clusterTopology is pre-computed shared topology built from all node ports.
type clusterTopology struct {
	Nodes    []clusterNodePorts
	Settings map[string]string
}

// clusterNodeConfigData is the template data for a single cluster node.
type clusterNodeConfigData struct {
	baseConfigData

	InterserverPort   uint32
	KeeperPort        uint32
	ServerID          int
	KeeperLogDir      string
	KeeperSnapshotDir string
	ReplicaName       string
	RaftServers       []raftServer
	KeeperNodes       []keeperNode
	ClusterReplicas   []clusterReplica
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
	if err := validateSettings(topo.Settings, clusterManagedKeys); err != nil {
		return "", err
	}

	node := topo.Nodes[nodeIndex]
	base := newBaseConfigData(dir, node.TCP, node.HTTP, topo.Settings)
	keeperLogDir := filepath.Join(dir, "coordination", "log")
	keeperSnapshotDir := filepath.Join(dir, "coordination", "snapshots")

	if err := makeDirs(append(base.dirs(), keeperLogDir, keeperSnapshotDir)...); err != nil {
		return "", err
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
		baseConfigData:    base,
		InterserverPort:   node.Interserver,
		KeeperPort:        node.Keeper,
		ServerID:          nodeIndex + 1,
		KeeperLogDir:      keeperLogDir,
		KeeperSnapshotDir: keeperSnapshotDir,
		ReplicaName:       fmt.Sprintf("replica_%02d", nodeIndex+1),
		RaftServers:       raftServers,
		KeeperNodes:       keeperNodes,
		ClusterReplicas:   clusterReplicas,
	}

	configPath := filepath.Join(dir, "config.xml")

	if err := writeConfigFile(configPath, clusterConfigTmpl, data); err != nil {
		return "", err
	}

	return configPath, nil
}
