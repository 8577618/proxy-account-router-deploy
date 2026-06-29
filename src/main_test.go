package main

import (
	"context"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func TestDiscoverNodesBlocksDefaultRegions(t *testing.T) {
	dir := t.TempDir()
	appConfig := filepath.Join(dir, "app-config.json")
	writeFile(t, appConfig, `{
		"ports": [
			{"port":18101,"tag":"socks-1-USA-01","target":"USA-01"},
			{"port":18102,"tag":"socks-2-HKG-01","target":"HKG-01"},
			{"port":18103,"tag":"socks-3-RUS-01","target":"RUS-01"}
		]
	}`)

	cfg := mustConfig(t, Config{AppConfigPath: appConfig})
	nodes, err := discoverNodes(cfg)
	if err != nil {
		t.Fatalf("discoverNodes() error = %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("len(nodes) = %d, want 1: %#v", len(nodes), nodes)
	}
	if nodes["18101"].Region != "USA" {
		t.Fatalf("USA node missing after filtering: %#v", nodes)
	}
}

func TestDiscoverNodesExcludesMetadataAndVmessOutbounds(t *testing.T) {
	dir := t.TempDir()
	appConfig := filepath.Join(dir, "app-config.json")
	singBoxConfig := filepath.Join(dir, "sing-box.json")
	writeFile(t, appConfig, `{
		"ports": [
			{"port":18101,"tag":"socks-1-USA-01","target":"USA-01"},
			{"port":18102,"tag":"socks-2-traffic","target":"剩余流量：372.42 GB"},
			{"port":18103,"tag":"socks-3-info","target":"🇨🇳永久地址：WWW.V2NY.COM"}
		]
	}`)
	writeFile(t, singBoxConfig, `{
		"outbounds": [
			{"tag":"USA-01","type":"trojan"},
			{"tag":"剩余流量：372.42 GB","type":"trojan"},
			{"tag":"🇨🇳永久地址：WWW.V2NY.COM","type":"vmess"}
		]
	}`)

	cfg := mustConfig(t, Config{AppConfigPath: appConfig, SingBoxConfigPath: singBoxConfig})
	nodes, err := discoverNodes(cfg)
	if err != nil {
		t.Fatalf("discoverNodes() error = %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("len(nodes) = %d, want 1: %#v", len(nodes), nodes)
	}
	if _, ok := nodes["18101"]; !ok {
		t.Fatalf("USA node missing after metadata filtering: %#v", nodes)
	}
}

func TestDiscoverFromClashProfilesFiltersAndAssignsManagedPorts(t *testing.T) {
	dir := t.TempDir()
	profile := filepath.Join(dir, "profile.yaml")
	writeFile(t, profile, `
proxies:
  - { name: "🇺🇸 USA·美国01", type: trojan, server: example.com, port: 443, password: secret, sni: www.example.com, skip-cert-verify: true, client-fingerprint: chrome }
  - { name: "🇭🇰 HKG·香港01", type: trojan, server: example.com, port: 443, password: secret }
  - { name: "🇨🇳永久地址：WWW.V2NY.COM", type: vmess, server: info.example.com, port: 443, uuid: deadbeef }
`)
	cfg := mustConfig(t, Config{
		NodeSource:        "clash",
		ClashProfilePaths: []string{profile},
		ManagedSingBox: ManagedSingBox{
			NodePortStart: 19200,
			NodePortEnd:   19210,
		},
	})
	nodes, err := discoverNodes(cfg)
	if err != nil {
		t.Fatalf("discoverNodes() error = %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("len(nodes) = %d, want 1: %#v", len(nodes), nodes)
	}
	node := nodes["19200"]
	if node.Region != "USA" || node.Type != "trojan" {
		t.Fatalf("node = %#v, want USA trojan", node)
	}
}

func TestDiscoverFromClashProfilesSupportsChineseRegionsAndFiltersMetadata(t *testing.T) {
	dir := t.TempDir()
	profile := filepath.Join(dir, "profile.yaml")
	writeFile(t, profile, `
proxies:
  - { name: "Traffic: 6.96 GB | 400 GB", type: trojan, server: example.com, port: 443, password: secret }
  - { name: "Expire: 2027-01-24", type: trojan, server: example.com, port: 443, password: secret }
  - { name: "🇯🇵 日本高级 IEPL 专线 1", type: trojan, server: example.com, port: 443, password: secret, sni: m.ctrip.com }
  - { name: "🇺🇸 美国高级 IEPL 专线 1", type: trojan, server: example.com, port: 443, password: secret, sni: m.ctrip.com }
  - { name: "🇭🇰 香港高级 IEPL 专线 1", type: trojan, server: example.com, port: 443, password: secret, sni: m.ctrip.com }
`)
	cfg := mustConfig(t, Config{
		NodeSource:        "clash",
		ClashProfilePaths: []string{profile},
		ManagedSingBox: ManagedSingBox{
			NodePortStart: 19200,
			NodePortEnd:   19210,
		},
	})
	nodes, err := discoverNodes(cfg)
	if err != nil {
		t.Fatalf("discoverNodes() error = %v", err)
	}
	if len(nodes) != 2 {
		t.Fatalf("len(nodes) = %d, want 2: %#v", len(nodes), nodes)
	}
	if got := nodes["19200"].Region; got != "JPN" {
		t.Fatalf("first node region = %q, want JPN", got)
	}
	if got := nodes["19201"].Region; got != "USA" {
		t.Fatalf("second node region = %q, want USA", got)
	}
}

func TestManagedSingBoxConfigUsesBootstrapDNSAndNoUTLS(t *testing.T) {
	dir := t.TempDir()
	profile := filepath.Join(dir, "profile.yaml")
	writeFile(t, profile, `
proxies:
  - { name: "🇺🇸 USA·美国01", type: trojan, server: example.com, port: 443, password: secret, sni: www.example.com, skip-cert-verify: true, client-fingerprint: chrome }
`)
	cfg := mustConfig(t, Config{
		NodeSource:        "clash",
		ClashProfilePaths: []string{profile},
		ManagedSingBox: ManagedSingBox{
			ListenHost:    "127.0.0.1",
			NodePortStart: 19200,
			NodePortEnd:   19210,
		},
	})
	config, err := managedSingBoxConfig(cfg)
	if err != nil {
		t.Fatalf("managedSingBoxConfig() error = %v", err)
	}
	outbounds := config["outbounds"].([]map[string]any)
	outbound := outbounds[0]
	if _, ok := outbound["utls"]; ok {
		t.Fatalf("outbound has top-level utls: %#v", outbound)
	}
	tls := outbound["tls"].(map[string]any)
	if _, ok := tls["utls"]; ok {
		t.Fatalf("outbound tls has utls: %#v", tls)
	}
	resolver := outbound["domain_resolver"].(map[string]any)
	if got := resolver["server"]; got != "dns-bootstrap" {
		t.Fatalf("domain resolver = %v, want dns-bootstrap", got)
	}
}

func TestManagedSingBoxConfigSupportsShadowsocks(t *testing.T) {
	dir := t.TempDir()
	profile := filepath.Join(dir, "profile.yaml")
	writeFile(t, profile, `
proxies:
  - { name: "🇺🇸 USA·美国01", type: ss, server: example.com, port: 8388, cipher: aes-128-gcm, password: secret }
`)
	cfg := mustConfig(t, Config{
		NodeSource:        "clash",
		ClashProfilePaths: []string{profile},
		ManagedSingBox: ManagedSingBox{
			ListenHost:    "127.0.0.1",
			NodePortStart: 19200,
			NodePortEnd:   19210,
		},
	})
	config, err := managedSingBoxConfig(cfg)
	if err != nil {
		t.Fatalf("managedSingBoxConfig() error = %v", err)
	}
	outbound := config["outbounds"].([]map[string]any)[0]
	if got := outbound["type"]; got != "shadowsocks" {
		t.Fatalf("outbound type = %v, want shadowsocks", got)
	}
	if got := outbound["method"]; got != "aes-128-gcm" {
		t.Fatalf("method = %v, want aes-128-gcm", got)
	}
}

func TestInitialRegionBalancesByCapacity(t *testing.T) {
	r := newTestRouter(t, Config{})
	r.nodes = map[string]Node{
		"us1": {ID: "us1", Region: "USA"},
		"us2": {ID: "us2", Region: "USA"},
		"jp1": {ID: "jp1", Region: "JPN"},
	}
	r.health = map[string]*HealthState{
		"us1": {Healthy: true},
		"us2": {Healthy: true},
		"jp1": {Healthy: true},
	}
	r.accounts = map[string]*AccountState{
		"a1": {Name: "a1", LockedRegion: "USA"},
		"a2": {Name: "a2", LockedRegion: "JPN"},
	}

	if got := r.pickInitialRegionLocked(AccountConfig{}); got != "USA" {
		t.Fatalf("pickInitialRegionLocked() = %q, want USA", got)
	}
}

func TestAssignmentDoesNotCrossLockedRegion(t *testing.T) {
	r := newTestRouter(t, Config{})
	r.nodes = map[string]Node{
		"us1": {ID: "us1", Region: "USA"},
		"us2": {ID: "us2", Region: "USA"},
		"jp1": {ID: "jp1", Region: "JPN"},
	}
	r.health = map[string]*HealthState{
		"us1": {Healthy: false},
		"us2": {Healthy: true},
		"jp1": {Healthy: true},
	}
	r.accounts = map[string]*AccountState{
		"a1": {Name: "a1", LockedRegion: "USA", CurrentNode: "us1"},
	}

	r.reconcileAssignments()
	if got := r.accounts["a1"].CurrentNode; got != "us2" {
		t.Fatalf("CurrentNode = %q, want us2", got)
	}
}

func TestCurrentNodeForAccountLazySwitchesWithinLockedRegion(t *testing.T) {
	r := newTestRouter(t, Config{})
	r.nodes = map[string]Node{
		"us1": {ID: "us1", Region: "USA"},
		"us2": {ID: "us2", Region: "USA"},
		"jp1": {ID: "jp1", Region: "JPN"},
	}
	r.health = map[string]*HealthState{
		"us1": {Healthy: false},
		"us2": {Healthy: true},
		"jp1": {Healthy: true},
	}
	r.accounts = map[string]*AccountState{
		"a1": {Name: "a1", LockedRegion: "USA", CurrentNode: "us1"},
	}

	node, err := r.currentNodeForAccount("a1")
	if err != nil {
		t.Fatalf("currentNodeForAccount() error = %v", err)
	}
	if node.ID != "us2" {
		t.Fatalf("node.ID = %q, want us2", node.ID)
	}
	if got := r.accounts["a1"].CurrentNode; got != "us2" {
		t.Fatalf("CurrentNode = %q, want us2", got)
	}
}

func TestLoadConfigDefaultsFixedProxyToSocks5(t *testing.T) {
	cfg := mustConfig(t, Config{
		Accounts: []AccountConfig{{
			Name:       "a1",
			ListenPort: 19100,
			FixedProxy: &ProxyRef{
				Host: "127.0.0.1",
				Port: 1080,
			},
		}},
	})
	if cfg.Accounts[0].FixedProxy.Type != "socks5" {
		t.Fatalf("FixedProxy.Type = %q, want socks5", cfg.Accounts[0].FixedProxy.Type)
	}
	if len(cfg.Accounts[0].FixedProxies) != 1 {
		t.Fatalf("len(FixedProxies) = %d, want 1", len(cfg.Accounts[0].FixedProxies))
	}
}

func TestLoadConfigParsesFixedProxyURL(t *testing.T) {
	cfg := mustConfig(t, Config{
		Accounts: []AccountConfig{{
			Name:       "a1",
			ListenPort: 19100,
			FixedProxy: &ProxyRef{
				URL: "socks5://user:pass@127.0.0.1:1080",
			},
		}},
	})
	proxy := cfg.Accounts[0].FixedProxy
	if proxy.Type != "socks5" || proxy.Host != "127.0.0.1" || proxy.Port != 1080 || proxy.Username != "user" || proxy.Password != "pass" {
		t.Fatalf("FixedProxy = %#v, want parsed socks5 url", proxy)
	}
}

func TestLoadConfigRejectsMultipleFixedProxiesForNow(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	writeFile(t, path, `
accounts:
  - name: a1
    listen_port: 19100
    fixed_proxies:
      - { host: "127.0.0.1", port: 1080 }
      - { host: "127.0.0.2", port: 1081 }
`)
	if _, err := loadConfig(path); err == nil {
		t.Fatalf("loadConfig() error = nil, want multiple fixed proxies rejected")
	}
}

func TestLoadStateNormalizesFixedProxyCompatibilityFields(t *testing.T) {
	r := newTestRouter(t, Config{})
	writeFile(t, r.cfg.StatePath, `{
		"accounts": {
			"a1": {
				"name": "a1",
				"listen_port": 19100,
				"fixed_proxy": {"type": "socks5", "host": "127.0.0.1", "port": 1080},
				"fixed_proxies": [{"type": "socks5", "host": "127.0.0.1", "port": 1080}]
			}
		}
	}`)

	if err := r.loadState(); err != nil {
		t.Fatalf("loadState() error = %v", err)
	}
	state := r.accounts["a1"]
	if state.FixedProxy == nil || state.FixedProxy.Type != "socks5" {
		t.Fatalf("FixedProxy = %#v, want normalized socks5 proxy", state.FixedProxy)
	}
	if len(state.FixedProxies) != 1 {
		t.Fatalf("len(FixedProxies) = %d, want 1", len(state.FixedProxies))
	}
}

func TestFixedProxySkipsRecoveryAndSummaryRouteAlerts(t *testing.T) {
	r := newTestRouter(t, Config{})
	r.accounts = map[string]*AccountState{
		"a1": {
			Name:       "a1",
			ListenPort: 19100,
			FixedProxy: &ProxyRef{
				Type: "socks5",
				Host: "127.0.0.1",
				Port: 1080,
			},
		},
	}

	if nodes := r.recoveryHealthNodesForPortLocked(19100); len(nodes) != 0 {
		t.Fatalf("recoveryHealthNodesForPortLocked() = %#v, want none", nodes)
	}
	recovered, err := r.recoverAccountProxyByPort(19100)
	if err != nil {
		t.Fatalf("recoverAccountProxyByPort() error = %v", err)
	}
	if recovered {
		t.Fatalf("recoverAccountProxyByPort() recovered = true, want false")
	}
	if issues := r.summarySnapshot().UnroutableAccounts; len(issues) != 0 {
		t.Fatalf("UnroutableAccounts = %#v, want none", issues)
	}
}

func TestReloadStoresRegionForFixedProxyAccount(t *testing.T) {
	dir := t.TempDir()
	appConfig := filepath.Join(dir, "app-config.json")
	writeFile(t, appConfig, `{
		"ports": [
			{"port":18101,"tag":"socks-1-USA-01","target":"USA-01"}
		]
	}`)

	r := newTestRouter(t, Config{AppConfigPath: appConfig})
	r.cfg.Accounts = []AccountConfig{{
		Name:       "a1",
		ListenPort: freePort(t),
		Region:     "USA",
		FixedProxy: &ProxyRef{Type: "socks5", Host: "127.0.0.1", Port: 1080},
	}}

	if err := r.reloadNodesAndAssignments(context.Background()); err != nil {
		t.Fatalf("reloadNodesAndAssignments() error = %v", err)
	}
	state := r.accounts["a1"]
	if state.LockedRegion != "USA" {
		t.Fatalf("LockedRegion = %q, want USA", state.LockedRegion)
	}
	if state.CurrentNode != "" {
		t.Fatalf("CurrentNode = %q, want empty while fixed proxy is set", state.CurrentNode)
	}
	r.health["18101"].Healthy = true

	if err := r.clearFixedProxy("a1"); err != nil {
		t.Fatalf("clearFixedProxy() error = %v", err)
	}
	if got := r.accounts["a1"].CurrentNode; got != "18101" {
		t.Fatalf("CurrentNode = %q, want USA node 18101", got)
	}
}

func TestDialAccountTargetUsesFixedProxyAuthWithoutSwitching(t *testing.T) {
	addr, done := startSocks5AuthServer(t, "user", "pass")
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("split addr: %v", err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("parse port: %v", err)
	}

	r := newTestRouter(t, Config{})
	r.accounts = map[string]*AccountState{
		"a1": {
			Name:         "a1",
			ListenPort:   19100,
			LockedRegion: "USA",
			CurrentNode:  "us1",
			FixedProxy: &ProxyRef{
				Type:     "socks5",
				Host:     host,
				Port:     port,
				Username: "user",
				Password: "pass",
			},
		},
	}

	conn, err := r.dialAccountTarget(context.Background(), "a1", "example.com:443")
	if err != nil {
		t.Fatalf("dialAccountTarget() error = %v", err)
	}
	conn.Close()
	<-done
	if got := r.accounts["a1"].CurrentNode; got != "us1" {
		t.Fatalf("CurrentNode = %q, want unchanged us1", got)
	}
}

func TestNonStrictAccountFallsBackWhenLockedRegionDown(t *testing.T) {
	r := newTestRouter(t, Config{AllowRegionFallback: true, RegionFallbacks: []string{"JPN"}})
	r.nodes = map[string]Node{
		"tw1": {ID: "tw1", Region: "TWN"},
		"jp1": {ID: "jp1", Region: "JPN"},
	}
	r.health = map[string]*HealthState{
		"tw1": {Healthy: false},
		"jp1": {Healthy: true},
	}
	r.accounts = map[string]*AccountState{
		"a1": {Name: "a1", LockedRegion: "TWN", CurrentNode: "tw1", FallbackRegions: []string{"JPN"}},
	}

	node, err := r.currentNodeForAccount("a1")
	if err != nil {
		t.Fatalf("currentNodeForAccount() error = %v", err)
	}
	if node.ID != "jp1" {
		t.Fatalf("node.ID = %q, want jp1", node.ID)
	}
}

func TestLockedAccountDoesNotUseGlobalFallbackRegions(t *testing.T) {
	r := newTestRouter(t, Config{AllowRegionFallback: true, RegionFallbacks: []string{"JPN"}})
	r.nodes = map[string]Node{
		"tw1": {ID: "tw1", Region: "TWN"},
		"jp1": {ID: "jp1", Region: "JPN"},
	}
	r.health = map[string]*HealthState{
		"tw1": {Healthy: false},
		"jp1": {Healthy: true},
	}
	r.accounts = map[string]*AccountState{
		"a1": {Name: "a1", LockedRegion: "TWN", CurrentNode: "tw1"},
	}

	if _, err := r.currentNodeForAccount("a1"); err == nil {
		t.Fatalf("currentNodeForAccount() error = nil, want no global fallback")
	}
	if got := r.accounts["a1"].CurrentNode; got != "tw1" {
		t.Fatalf("CurrentNode = %q, want tw1", got)
	}
}

func TestStrictAccountDoesNotFallbackWhenLockedRegionDown(t *testing.T) {
	r := newTestRouter(t, Config{AllowRegionFallback: true, RegionFallbacks: []string{"JPN"}})
	r.nodes = map[string]Node{
		"tw1": {ID: "tw1", Region: "TWN"},
		"jp1": {ID: "jp1", Region: "JPN"},
	}
	r.health = map[string]*HealthState{
		"tw1": {Healthy: false},
		"jp1": {Healthy: true},
	}
	r.accounts = map[string]*AccountState{
		"a1": {Name: "a1", LockedRegion: "TWN", CurrentNode: "tw1", StrictRegion: true},
	}

	if _, err := r.currentNodeForAccount("a1"); err == nil {
		t.Fatalf("currentNodeForAccount() error = nil, want no fallback error")
	}
	if got := r.accounts["a1"].CurrentNode; got != "tw1" {
		t.Fatalf("CurrentNode = %q, want tw1", got)
	}
}

func TestSummaryReportsAlertsAndPortPool(t *testing.T) {
	r := newTestRouter(t, Config{
		AllowRegionFallback: true,
		AutoSync: AutoSyncConfig{
			Enabled:         true,
			ListenPortStart: 19100,
			ListenPortEnd:   19104,
		},
	})
	r.nodes = map[string]Node{
		"sg1": {ID: "sg1", Region: "SGP"},
	}
	r.health = map[string]*HealthState{
		"sg1": {Healthy: false},
	}
	r.accounts = map[string]*AccountState{
		"a1": {Name: "a1", ListenPort: 19100, LockedRegion: "SGP", CurrentNode: "sg1", StrictRegion: true},
	}

	summary := r.summarySnapshot()
	if summary.PortPool == nil {
		t.Fatalf("PortPool = nil")
	}
	if summary.PortPool.Capacity != 5 || summary.PortPool.Used != 1 || summary.PortPool.Remaining != 4 {
		t.Fatalf("PortPool = %#v, want capacity 5 used 1 remaining 4", summary.PortPool)
	}
	if len(summary.Alerts) < 2 {
		t.Fatalf("Alerts = %#v, want region and strict account alerts", summary.Alerts)
	}
	var foundRegion, foundStrict bool
	for _, alert := range summary.Alerts {
		if alert.Type == "region_no_healthy_nodes" {
			foundRegion = true
		}
		if alert.Type == "strict_account_unroutable" {
			foundStrict = true
		}
	}
	if !foundRegion || !foundStrict {
		t.Fatalf("Alerts = %#v, want region and strict account alerts", summary.Alerts)
	}
}

func TestUnlockRegionClearsStrictLockAndRepicksNode(t *testing.T) {
	r := newTestRouter(t, Config{AllowRegionFallback: true, RegionFallbacks: []string{"JPN"}})
	r.nodes = map[string]Node{
		"sg1": {ID: "sg1", Region: "SGP"},
		"jp1": {ID: "jp1", Region: "JPN"},
	}
	r.health = map[string]*HealthState{
		"sg1": {Healthy: false},
		"jp1": {Healthy: true},
	}
	r.accounts = map[string]*AccountState{
		"a1": {Name: "a1", ListenPort: 19100, LockedRegion: "SGP", CurrentNode: "sg1", StrictRegion: true, FallbackRegions: []string{"USA"}},
	}

	if err := r.unlockRegion("a1"); err != nil {
		t.Fatalf("unlockRegion() error = %v", err)
	}
	state := r.accounts["a1"]
	if state.LockedRegion != "" || state.StrictRegion || len(state.FallbackRegions) != 0 {
		t.Fatalf("state after unlock = %#v, want no lock", state)
	}
	if state.CurrentNode != "jp1" {
		t.Fatalf("CurrentNode = %q, want jp1", state.CurrentNode)
	}
}

func TestRecoverAccountProxyByPortKeepsHealthyNode(t *testing.T) {
	r := newTestRouter(t, Config{})
	r.nodes = map[string]Node{
		"us1": {ID: "us1", Region: "USA"},
	}
	r.health = map[string]*HealthState{
		"us1": {Healthy: true},
	}
	r.accounts = map[string]*AccountState{
		"sub2api:6": {Name: "sub2api:6", ListenPort: 19107, LockedRegion: "USA", CurrentNode: "us1"},
	}

	recovered, err := r.recoverAccountProxyByPort(19107)
	if err != nil {
		t.Fatalf("recoverAccountProxyByPort() error = %v", err)
	}
	if !recovered {
		t.Fatalf("recoverAccountProxyByPort() recovered = false, want true")
	}
	if got := r.accounts["sub2api:6"].CurrentNode; got != "us1" {
		t.Fatalf("CurrentNode = %q, want us1", got)
	}
}

func TestRecoverAccountProxyByPortSwitchesToHealthyCandidate(t *testing.T) {
	r := newTestRouter(t, Config{})
	r.nodes = map[string]Node{
		"us1": {ID: "us1", Region: "USA"},
		"us2": {ID: "us2", Region: "USA"},
	}
	r.health = map[string]*HealthState{
		"us1": {Healthy: false},
		"us2": {Healthy: true},
	}
	r.accounts = map[string]*AccountState{
		"sub2api:6": {Name: "sub2api:6", ListenPort: 19107, LockedRegion: "USA", CurrentNode: "us1"},
	}

	recovered, err := r.recoverAccountProxyByPort(19107)
	if err != nil {
		t.Fatalf("recoverAccountProxyByPort() error = %v", err)
	}
	if !recovered {
		t.Fatalf("recoverAccountProxyByPort() recovered = false, want true")
	}
	if got := r.accounts["sub2api:6"].CurrentNode; got != "us2" {
		t.Fatalf("CurrentNode = %q, want us2", got)
	}
}

func TestRecoverAccountProxyByPortKeepsStrictAccountDownWhenNoHealthyRegionNode(t *testing.T) {
	r := newTestRouter(t, Config{AllowRegionFallback: true, RegionFallbacks: []string{"JPN"}})
	r.nodes = map[string]Node{
		"sg1": {ID: "sg1", Region: "SGP"},
		"jp1": {ID: "jp1", Region: "JPN"},
	}
	r.health = map[string]*HealthState{
		"sg1": {Healthy: false},
		"jp1": {Healthy: true},
	}
	r.accounts = map[string]*AccountState{
		"sub2api:6": {Name: "sub2api:6", ListenPort: 19107, LockedRegion: "SGP", CurrentNode: "sg1", StrictRegion: true},
	}

	recovered, err := r.recoverAccountProxyByPort(19107)
	if err != nil {
		t.Fatalf("recoverAccountProxyByPort() error = %v", err)
	}
	if recovered {
		t.Fatalf("recoverAccountProxyByPort() recovered = true, want false")
	}
	if got := r.accounts["sub2api:6"].CurrentNode; got != "sg1" {
		t.Fatalf("CurrentNode = %q, want sg1", got)
	}
}

func TestRecoverAccountProxyByPortUsesExplicitFallbackForNonStrictAccount(t *testing.T) {
	r := newTestRouter(t, Config{AllowRegionFallback: true})
	r.nodes = map[string]Node{
		"sg1": {ID: "sg1", Region: "SGP"},
		"jp1": {ID: "jp1", Region: "JPN"},
	}
	r.health = map[string]*HealthState{
		"sg1": {Healthy: false},
		"jp1": {Healthy: true},
	}
	r.accounts = map[string]*AccountState{
		"sub2api:6": {Name: "sub2api:6", ListenPort: 19107, LockedRegion: "SGP", CurrentNode: "sg1", FallbackRegions: []string{"JPN"}},
	}

	recovered, err := r.recoverAccountProxyByPort(19107)
	if err != nil {
		t.Fatalf("recoverAccountProxyByPort() error = %v", err)
	}
	if !recovered {
		t.Fatalf("recoverAccountProxyByPort() recovered = false, want true")
	}
	if got := r.accounts["sub2api:6"].CurrentNode; got != "jp1" {
		t.Fatalf("CurrentNode = %q, want jp1", got)
	}
}

func TestRecoveryHealthNodesForPortUsesStrictRegionOnly(t *testing.T) {
	r := newTestRouter(t, Config{AllowRegionFallback: true, RegionFallbacks: []string{"JPN"}})
	r.nodes = map[string]Node{
		"sg1": {ID: "sg1", Region: "SGP"},
		"sg2": {ID: "sg2", Region: "SGP"},
		"jp1": {ID: "jp1", Region: "JPN"},
	}
	r.accounts = map[string]*AccountState{
		"sub2api:6": {Name: "sub2api:6", ListenPort: 19107, LockedRegion: "SGP", CurrentNode: "sg1", StrictRegion: true},
	}

	nodes := r.recoveryHealthNodesForPortLocked(19107)
	got := nodeIDs(nodes)
	want := []string{"sg1", "sg2"}
	if !stringSlicesEqual(got, want) {
		t.Fatalf("recoveryHealthNodesForPortLocked() = %v, want %v", got, want)
	}
}

func TestRecoveryHealthNodesForPortIncludesFallbackCandidates(t *testing.T) {
	r := newTestRouter(t, Config{AllowRegionFallback: true})
	r.nodes = map[string]Node{
		"sg1": {ID: "sg1", Region: "SGP"},
		"jp1": {ID: "jp1", Region: "JPN"},
		"us1": {ID: "us1", Region: "USA"},
	}
	r.accounts = map[string]*AccountState{
		"sub2api:6": {Name: "sub2api:6", ListenPort: 19107, LockedRegion: "SGP", CurrentNode: "sg1", FallbackRegions: []string{"JPN"}},
	}

	nodes := r.recoveryHealthNodesForPortLocked(19107)
	got := nodeIDs(nodes)
	want := []string{"jp1", "sg1"}
	if !stringSlicesEqual(got, want) {
		t.Fatalf("recoveryHealthNodesForPortLocked() = %v, want %v", got, want)
	}
}

func TestRecoveryTimeoutAtLeastHealthTimeout(t *testing.T) {
	cfg := Config{HealthTimeout: 20 * time.Second, SyncTimeout: 5 * time.Second}

	if got, want := sub2APIRecoveryTimeout(cfg), 25*time.Second; got != want {
		t.Fatalf("sub2APIRecoveryTimeout() = %v, want %v", got, want)
	}
	if got, want := sub2APIRecoveryDBTimeout(cfg), 15*time.Second; got != want {
		t.Fatalf("sub2APIRecoveryDBTimeout() = %v, want %v", got, want)
	}
}

func TestReloadKeepsExistingRegionLockWhileHealthIsUnknown(t *testing.T) {
	dir := t.TempDir()
	appConfig := filepath.Join(dir, "app-config.json")
	writeFile(t, appConfig, `{
		"ports": [
			{"port":18128,"tag":"socks-1-SGP-01","target":"SGP-01"},
			{"port":18109,"tag":"socks-2-JPN-01","target":"JPN-01"}
		]
	}`)

	r := newTestRouter(t, Config{AppConfigPath: appConfig})
	r.cfg.Accounts = []AccountConfig{{Name: "a1", ListenPort: freePort(t)}}
	r.accounts = map[string]*AccountState{
		"a1": {Name: "a1", ListenPort: r.cfg.Accounts[0].ListenPort, LockedRegion: "SGP", CurrentNode: "18128"},
	}
	r.health = map[string]*HealthState{
		"18128": {Healthy: false},
		"18109": {Healthy: true},
	}

	if err := r.reloadNodesAndAssignments(context.Background()); err != nil {
		t.Fatalf("reloadNodesAndAssignments() error = %v", err)
	}
	if got := r.accounts["a1"].LockedRegion; got != "SGP" {
		t.Fatalf("LockedRegion = %q, want SGP", got)
	}
	if got := r.accounts["a1"].CurrentNode; got != "18128" {
		t.Fatalf("CurrentNode = %q, want 18128", got)
	}
}

func TestConfigReloadAddsAndRemovesListeners(t *testing.T) {
	r := newTestRouter(t, Config{ListenHost: "127.0.0.1"})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	r.syncListenersLocked(ctx, Config{
		ListenHost: "127.0.0.1",
		Accounts:   []AccountConfig{{Name: "a1", ListenPort: freePort(t)}},
	}, []AccountConfig{{Name: "a1", ListenPort: freePort(t)}})
	if len(r.listeners) != 1 {
		t.Fatalf("listeners after add = %d, want 1", len(r.listeners))
	}
	r.syncListenersLocked(ctx, Config{ListenHost: "127.0.0.1"}, nil)
	if len(r.listeners) != 0 {
		t.Fatalf("listeners after remove = %d, want 0", len(r.listeners))
	}
}

func newTestRouter(t *testing.T, cfg Config) *Router {
	t.Helper()
	cfg = mustConfig(t, cfg)
	cfg.StatePath = filepath.Join(t.TempDir(), "state.json")
	return &Router{
		cfg:              cfg,
		nodes:            map[string]Node{},
		health:           map[string]*HealthState{},
		accounts:         map[string]*AccountState{},
		listeners:        map[int]net.Listener{},
		listenerAccounts: map[int]string{},
	}
}

func mustConfig(t *testing.T, cfg Config) Config {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	data, err := yaml.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal config: %v", err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	loaded, err := loadConfig(path)
	if err != nil {
		t.Fatalf("loadConfig() error = %v", err)
	}
	return loaded
}

func writeFile(t *testing.T, path, data string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(data), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func nodeIDs(nodes []Node) []string {
	out := make([]string, 0, len(nodes))
	for _, node := range nodes {
		out = append(out, node.ID)
	}
	return out
}

func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen free port: %v", err)
	}
	defer ln.Close()
	_, portStr, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		t.Fatalf("split addr: %v", err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("parse port: %v", err)
	}
	time.Sleep(10 * time.Millisecond)
	return port
}

func startSocks5AuthServer(t *testing.T, username, password string) (string, <-chan struct{}) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen socks server: %v", err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer ln.Close()
		conn, err := ln.Accept()
		if err != nil {
			t.Errorf("accept socks server: %v", err)
			return
		}
		defer conn.Close()

		header := make([]byte, 2)
		if _, err := io.ReadFull(conn, header); err != nil {
			t.Errorf("read hello: %v", err)
			return
		}
		methods := make([]byte, int(header[1]))
		if _, err := io.ReadFull(conn, methods); err != nil {
			t.Errorf("read methods: %v", err)
			return
		}
		if _, err := conn.Write([]byte{0x05, 0x02}); err != nil {
			t.Errorf("write auth method: %v", err)
			return
		}

		authHead := make([]byte, 2)
		if _, err := io.ReadFull(conn, authHead); err != nil {
			t.Errorf("read auth head: %v", err)
			return
		}
		user := make([]byte, int(authHead[1]))
		if _, err := io.ReadFull(conn, user); err != nil {
			t.Errorf("read user: %v", err)
			return
		}
		passLen := make([]byte, 1)
		if _, err := io.ReadFull(conn, passLen); err != nil {
			t.Errorf("read pass len: %v", err)
			return
		}
		pass := make([]byte, int(passLen[0]))
		if _, err := io.ReadFull(conn, pass); err != nil {
			t.Errorf("read pass: %v", err)
			return
		}
		if string(user) != username || string(pass) != password {
			t.Errorf("auth = %q/%q, want %q/%q", user, pass, username, password)
			return
		}
		if _, err := conn.Write([]byte{0x01, 0x00}); err != nil {
			t.Errorf("write auth ok: %v", err)
			return
		}

		req := make([]byte, 4)
		if _, err := io.ReadFull(conn, req); err != nil {
			t.Errorf("read request: %v", err)
			return
		}
		if got := string(readSocksAddrForTest(t, conn, req[3])); got != "example.com" {
			t.Errorf("target host = %q, want example.com", got)
			return
		}
		portBytes := make([]byte, 2)
		if _, err := io.ReadFull(conn, portBytes); err != nil {
			t.Errorf("read target port: %v", err)
			return
		}
		if _, err := conn.Write([]byte{0x05, 0x00, 0x00, 0x01, 127, 0, 0, 1, 0, 0}); err != nil {
			t.Errorf("write connect ok: %v", err)
		}
	}()
	return ln.Addr().String(), done
}

func readSocksAddrForTest(t *testing.T, r io.Reader, atyp byte) []byte {
	t.Helper()
	if atyp != 0x03 {
		t.Fatalf("atyp = %d, want domain", atyp)
	}
	lenBuf := make([]byte, 1)
	if _, err := io.ReadFull(r, lenBuf); err != nil {
		t.Fatalf("read host len: %v", err)
	}
	host := make([]byte, int(lenBuf[0]))
	if _, err := io.ReadFull(r, host); err != nil {
		t.Fatalf("read host: %v", err)
	}
	return host
}
