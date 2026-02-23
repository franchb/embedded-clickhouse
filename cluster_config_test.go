package embeddedclickhouse

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func threeNodeTopology() clusterTopology {
	ports := []clusterNodePorts{
		{TCP: 19000, HTTP: 18123, Interserver: 19009, Keeper: 19181, KeeperRaft: 19234},
		{TCP: 29000, HTTP: 28123, Interserver: 29009, Keeper: 29181, KeeperRaft: 29234},
		{TCP: 39000, HTTP: 38123, Interserver: 39009, Keeper: 39181, KeeperRaft: 39234},
	}

	return buildClusterTopology(ports, nil)
}

func TestWriteClusterNodeConfig_XMLCorrectness(t *testing.T) {
	t.Parallel()

	topo := threeNodeTopology()
	dir := t.TempDir()

	configPath, err := writeClusterNodeConfig(dir, 0, topo)
	if err != nil {
		t.Fatal(err)
	}

	content, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}

	xml := string(content)

	checks := []string{
		"<tcp_port>19000</tcp_port>",
		"<http_port>18123</http_port>",
		"<interserver_http_port>19009</interserver_http_port>",
		"<server_id>1</server_id>",
		"<shard>01</shard>",
		"<replica>replica_01</replica>",
		// Keeper ports from all 3 nodes in raft_configuration.
		"<port>19234</port>",
		"<port>29234</port>",
		"<port>39234</port>",
		// Zookeeper nodes.
		"<port>19181</port>",
		"<port>29181</port>",
		"<port>39181</port>",
		// Cluster replicas (TCP ports).
		"<port>19000</port>",
		"<port>29000</port>",
		"<port>39000</port>",
		// Distributed DDL.
		"<distributed_ddl>",
		"<path>/clickhouse/task_queue/ddl</path>",
	}

	for _, check := range checks {
		if !strings.Contains(xml, check) {
			t.Errorf("config missing %q", check)
		}
	}
}

func TestWriteClusterNodeConfig_Node1HasServerID2(t *testing.T) {
	t.Parallel()

	topo := threeNodeTopology()
	dir := t.TempDir()

	configPath, err := writeClusterNodeConfig(dir, 1, topo)
	if err != nil {
		t.Fatal(err)
	}

	content, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}

	xml := string(content)

	if !strings.Contains(xml, "<server_id>2</server_id>") {
		t.Error("node 1 should have server_id 2")
	}

	if !strings.Contains(xml, "<replica>replica_02</replica>") {
		t.Error("node 1 should have replica_02 macro")
	}

	if !strings.Contains(xml, "<tcp_port>29000</tcp_port>") {
		t.Error("node 1 should use its own TCP port")
	}
}

func TestWriteClusterNodeConfig_CreatesKeeperDirs(t *testing.T) {
	t.Parallel()

	topo := threeNodeTopology()
	dir := t.TempDir()

	_, err := writeClusterNodeConfig(dir, 0, topo)
	if err != nil {
		t.Fatal(err)
	}

	subdirs := []string{
		"data",
		"tmp",
		"user_files",
		"format_schemas",
		filepath.Join("coordination", "log"),
		filepath.Join("coordination", "snapshots"),
	}

	for _, sub := range subdirs {
		info, statErr := os.Stat(filepath.Join(dir, sub))
		if statErr != nil {
			t.Errorf("subdir %q not created: %v", sub, statErr)
			continue
		}

		if !info.IsDir() {
			t.Errorf("%q is not a directory", sub)
		}
	}
}

func TestBuildClusterTopology_NilSettings(t *testing.T) {
	t.Parallel()

	topo := buildClusterTopology([]clusterNodePorts{
		{TCP: 1, HTTP: 2, Interserver: 3, Keeper: 4, KeeperRaft: 5},
	}, nil)

	if len(topo.Settings) != 0 {
		t.Errorf("expected empty settings for nil input, got %v", topo.Settings)
	}
}

func TestBuildClusterTopology_UserSettings(t *testing.T) {
	t.Parallel()

	topo := buildClusterTopology([]clusterNodePorts{
		{TCP: 1, HTTP: 2, Interserver: 3, Keeper: 4, KeeperRaft: 5},
	}, map[string]string{
		"max_server_memory_usage": "2147483648",
	})

	if topo.Settings["max_server_memory_usage"] != "2147483648" {
		t.Errorf("expected user setting, got %s", topo.Settings["max_server_memory_usage"])
	}
}

func TestWriteClusterNodeConfig_InvalidSettingKey(t *testing.T) {
	t.Parallel()

	topo := buildClusterTopology(
		[]clusterNodePorts{{TCP: 1, HTTP: 2, Interserver: 3, Keeper: 4, KeeperRaft: 5}},
		map[string]string{"bad key!": "value"},
	)
	dir := t.TempDir()

	_, err := writeClusterNodeConfig(dir, 0, topo)
	if err == nil {
		t.Fatal("expected error for invalid setting key")
	}
}

func TestWriteClusterNodeConfig_DifferentNodes(t *testing.T) {
	t.Parallel()

	topo := threeNodeTopology()

	for nodeIdx := range 3 {
		dir := t.TempDir()

		configPath, err := writeClusterNodeConfig(dir, nodeIdx, topo)
		if err != nil {
			t.Fatalf("node %d: %v", nodeIdx, err)
		}

		content, err := os.ReadFile(configPath)
		if err != nil {
			t.Fatalf("node %d: %v", nodeIdx, err)
		}

		xml := string(content)

		// Each node should have its own TCP port.
		wantTCP := topo.Nodes[nodeIdx].TCP
		wantTag := fmt.Sprintf("<tcp_port>%d</tcp_port>", wantTCP)

		if !strings.Contains(xml, wantTag) {
			t.Errorf("node %d: missing %s", nodeIdx, wantTag)
		}

		// But all nodes share the same raft configuration.
		for ri := range 3 {
			raftTag := fmt.Sprintf("<port>%d</port>", topo.Nodes[ri].KeeperRaft)
			if !strings.Contains(xml, raftTag) {
				t.Errorf("node %d: missing raft port for node %d: %s", nodeIdx, ri, raftTag)
			}
		}
	}
}
