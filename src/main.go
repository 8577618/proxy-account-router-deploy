package main

import (
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/lib/pq"
	"gopkg.in/yaml.v3"
)

type Config struct {
	ListenHost             string            `yaml:"listen_host"`
	NodeSource             string            `yaml:"node_source"`
	Sub2Socks5Host         string            `yaml:"sub2socks5_host"`
	UpstreamSocksHost      string            `yaml:"upstream_socks_host"`
	SingBoxConfigPath      string            `yaml:"sing_box_config_path"`
	AppConfigPath          string            `yaml:"app_config_path"`
	SubscriptionStatePath  string            `yaml:"subscription_state_path"`
	ClashProfilePaths      []string          `yaml:"clash_profile_paths"`
	ManagedSingBox         ManagedSingBox    `yaml:"managed_sing_box"`
	StatePath              string            `yaml:"state_path"`
	ReloadInterval         time.Duration     `yaml:"reload_interval"`
	HealthInterval         time.Duration     `yaml:"health_interval"`
	HealthTimeout          time.Duration     `yaml:"health_timeout"`
	HealthURL              string            `yaml:"health_url"`
	HealthTargets          []HealthTarget    `yaml:"health_targets"`
	HealthMinSuccesses     int               `yaml:"health_min_successes"`
	HealthyAfterSuccesses  int               `yaml:"healthy_after_successes"`
	UnhealthyAfterFailures int               `yaml:"unhealthy_after_failures"`
	AllowRegionFallback    bool              `yaml:"allow_region_fallback"`
	RegionFallbacks        []string          `yaml:"region_fallbacks"`
	ClientHandshakeTimeout time.Duration     `yaml:"client_handshake_timeout"`
	UpstreamDialTimeout    time.Duration     `yaml:"upstream_dial_timeout"`
	MaxHealthConcurrency   int               `yaml:"max_health_concurrency"`
	SyncTimeout            time.Duration     `yaml:"sync_timeout"`
	RegionAliases          map[string]string `yaml:"region_aliases"`
	BlockedRegions         []string          `yaml:"blocked_regions"`
	SpeedWeightCap         float64           `yaml:"speed_weight_cap"`
	MinNodeWeight          float64           `yaml:"min_node_weight"`
	MaxNodeWeight          float64           `yaml:"max_node_weight"`
	AutoSync               AutoSyncConfig    `yaml:"auto_sync"`
	Accounts               []AccountConfig   `yaml:"accounts"`
}

type HealthTarget struct {
	Name           string `yaml:"name" json:"name"`
	URL            string `yaml:"url" json:"url"`
	ExpectStatuses []int  `yaml:"expect_statuses" json:"expect_statuses,omitempty"`
}

type AutoSyncConfig struct {
	Enabled         bool                  `yaml:"enabled"`
	Interval        time.Duration         `yaml:"interval"`
	ListenPortStart int                   `yaml:"listen_port_start"`
	ListenPortEnd   int                   `yaml:"listen_port_end"`
	ProxyHost       string                `yaml:"proxy_host"`
	CLIProxyAPI     CLIProxyAPISyncConfig `yaml:"cliproxyapi"`
	Sub2API         Sub2APISyncConfig     `yaml:"sub2api"`
}

type CLIProxyAPISyncConfig struct {
	Enabled bool   `yaml:"enabled"`
	AuthDir string `yaml:"auth_dir"`
}

type Sub2APISyncConfig struct {
	Enabled                       bool   `yaml:"enabled"`
	RecoverProxyTempUnschedulable *bool  `yaml:"recover_proxy_temp_unschedulable"`
	EnvFile                       string `yaml:"env_file"`
	Host                          string `yaml:"host"`
	Port                          int    `yaml:"port"`
	User                          string `yaml:"user"`
	Password                      string `yaml:"password"`
	DBName                        string `yaml:"dbname"`
	SSLMode                       string `yaml:"sslmode"`
}

type ManagedSingBox struct {
	Enabled       bool   `yaml:"enabled"`
	BinaryPath    string `yaml:"binary_path"`
	ConfigPath    string `yaml:"config_path"`
	WorkDir       string `yaml:"work_dir"`
	ListenHost    string `yaml:"listen_host"`
	NodePortStart int    `yaml:"node_port_start"`
	NodePortEnd   int    `yaml:"node_port_end"`
}

type AccountConfig struct {
	Name             string     `yaml:"name"`
	Project          string     `yaml:"project"`
	ListenPort       int        `yaml:"listen_port"`
	Region           string     `yaml:"region"`
	StrictRegion     bool       `yaml:"strict_region"`
	PreferredRegions []string   `yaml:"preferred_regions"`
	FallbackRegions  []string   `yaml:"fallback_regions"`
	FixedProxy       *ProxyRef  `yaml:"fixed_proxy"`
	FixedProxies     []ProxyRef `yaml:"fixed_proxies"`
}

type ProxyRef struct {
	Type     string `yaml:"type" json:"type,omitempty"`
	URL      string `yaml:"url" json:"url,omitempty"`
	Host     string `yaml:"host" json:"host"`
	Port     int    `yaml:"port" json:"port"`
	Username string `yaml:"username" json:"username,omitempty"`
	Password string `yaml:"password" json:"password,omitempty"`
}

type Node struct {
	ID     string `json:"id"`
	Port   int    `json:"port"`
	Tag    string `json:"tag"`
	Target string `json:"target"`
	Region string `json:"region"`
	Type   string `json:"type,omitempty"`
}

type HealthState struct {
	Healthy         bool                 `json:"healthy"`
	Successes       int                  `json:"successes"`
	Failures        int                  `json:"failures"`
	LastError       string               `json:"last_error,omitempty"`
	LatencyMS       int64                `json:"latency_ms,omitempty"`
	LatencyEWMAms   float64              `json:"latency_ewma_ms,omitempty"`
	Targets         []TargetHealthResult `json:"targets,omitempty"`
	CheckedAt       time.Time            `json:"checked_at"`
	LastHealthyAt   time.Time            `json:"last_healthy_at,omitempty"`
	LastUnhealthyAt time.Time            `json:"last_unhealthy_at,omitempty"`
}

type TargetHealthResult struct {
	Name      string `json:"name"`
	URL       string `json:"url"`
	OK        bool   `json:"ok"`
	Status    int    `json:"status,omitempty"`
	LatencyMS int64  `json:"latency_ms,omitempty"`
	Error     string `json:"error,omitempty"`
}

type AccountState struct {
	Name            string     `json:"name"`
	Project         string     `json:"project,omitempty"`
	ListenPort      int        `json:"listen_port"`
	LockedRegion    string     `json:"locked_region"`
	StrictRegion    bool       `json:"strict_region,omitempty"`
	FallbackRegions []string   `json:"fallback_regions,omitempty"`
	CurrentNode     string     `json:"current_node,omitempty"`
	FixedProxy      *ProxyRef  `json:"fixed_proxy,omitempty"`
	FixedProxies    []ProxyRef `json:"fixed_proxies,omitempty"`
	UpdatedAt       time.Time  `json:"updated_at"`
}

type StateFile struct {
	Accounts map[string]*AccountState `json:"accounts"`
}

type Router struct {
	cfgPath string
	cfg     Config

	mu       sync.RWMutex
	nodes    map[string]Node
	health   map[string]*HealthState
	accounts map[string]*AccountState

	listeners        map[int]net.Listener
	listenerAccounts map[int]string
	syncMu           sync.Mutex
	singBoxCmd       *exec.Cmd
	cancel           context.CancelFunc
}

func main() {
	configPath := flag.String("config", "/app/config.yaml", "config file path")
	flag.Parse()

	cfg, err := loadConfig(*configPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	router := &Router{
		cfgPath:          *configPath,
		cfg:              cfg,
		nodes:            map[string]Node{},
		health:           map[string]*HealthState{},
		accounts:         map[string]*AccountState{},
		listeners:        map[int]net.Listener{},
		listenerAccounts: map[int]string{},
		cancel:           cancel,
	}

	if err := router.loadState(); err != nil {
		log.Printf("state load warning: %v", err)
	}
	if err := router.reloadNodesAndAssignments(context.Background()); err != nil {
		log.Printf("initial reload warning: %v", err)
	}

	go router.reloadLoop(ctx)
	go router.healthLoop(ctx)
	go router.httpStatusServer(ctx)

	router.wait(ctx)
}

func loadConfig(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return Config{}, err
	}
	if cfg.ListenHost == "" {
		cfg.ListenHost = "0.0.0.0"
	}
	cfg.NodeSource = strings.ToLower(strings.TrimSpace(cfg.NodeSource))
	if cfg.NodeSource == "" {
		cfg.NodeSource = "sub2socks5"
	}
	if cfg.Sub2Socks5Host == "" {
		cfg.Sub2Socks5Host = "sub2socks5"
	}
	if cfg.UpstreamSocksHost == "" {
		cfg.UpstreamSocksHost = cfg.Sub2Socks5Host
	}
	if cfg.SingBoxConfigPath == "" {
		cfg.SingBoxConfigPath = "/sub2socks5/runtime/sing-box.json"
	}
	if cfg.AppConfigPath == "" {
		cfg.AppConfigPath = "/sub2socks5/data/app-config.json"
	}
	if cfg.SubscriptionStatePath == "" {
		cfg.SubscriptionStatePath = "/sub2socks5/data/subscription-state.json"
	}
	if cfg.ManagedSingBox.BinaryPath == "" {
		cfg.ManagedSingBox.BinaryPath = "/usr/local/bin/sing-box"
	}
	if cfg.ManagedSingBox.ConfigPath == "" {
		cfg.ManagedSingBox.ConfigPath = "/app/data/managed-sing-box.json"
	}
	if cfg.ManagedSingBox.WorkDir == "" {
		cfg.ManagedSingBox.WorkDir = "/app/data"
	}
	if cfg.ManagedSingBox.ListenHost == "" {
		cfg.ManagedSingBox.ListenHost = "127.0.0.1"
	}
	if cfg.ManagedSingBox.NodePortStart <= 0 {
		cfg.ManagedSingBox.NodePortStart = 19200
	}
	if cfg.ManagedSingBox.NodePortEnd <= 0 {
		cfg.ManagedSingBox.NodePortEnd = 19399
	}
	if cfg.StatePath == "" {
		cfg.StatePath = "/app/data/state.json"
	}
	if cfg.ReloadInterval <= 0 {
		cfg.ReloadInterval = 15 * time.Second
	}
	if cfg.HealthInterval <= 0 {
		cfg.HealthInterval = 30 * time.Second
	}
	if cfg.HealthTimeout <= 0 {
		cfg.HealthTimeout = 8 * time.Second
	}
	if cfg.HealthURL == "" {
		cfg.HealthURL = "https://www.gstatic.com/generate_204"
	}
	if len(cfg.HealthTargets) == 0 {
		cfg.HealthTargets = []HealthTarget{
			{Name: "gstatic", URL: cfg.HealthURL, ExpectStatuses: []int{204}},
			{Name: "openai", URL: "https://api.openai.com/", ExpectStatuses: []int{200, 401, 403, 404}},
			{Name: "gemini", URL: "https://generativelanguage.googleapis.com/", ExpectStatuses: []int{200, 400, 401, 403, 404}},
		}
	}
	for i := range cfg.HealthTargets {
		if strings.TrimSpace(cfg.HealthTargets[i].Name) == "" {
			cfg.HealthTargets[i].Name = fmt.Sprintf("target-%d", i+1)
		}
		if strings.TrimSpace(cfg.HealthTargets[i].URL) == "" {
			cfg.HealthTargets[i].URL = cfg.HealthURL
		}
	}
	if cfg.HealthMinSuccesses <= 0 {
		cfg.HealthMinSuccesses = 2
	}
	if cfg.HealthMinSuccesses > len(cfg.HealthTargets) {
		cfg.HealthMinSuccesses = len(cfg.HealthTargets)
	}
	if cfg.HealthyAfterSuccesses <= 0 {
		cfg.HealthyAfterSuccesses = 1
	}
	if cfg.UnhealthyAfterFailures <= 0 {
		cfg.UnhealthyAfterFailures = 2
	}
	if len(cfg.RegionFallbacks) == 0 {
		cfg.RegionFallbacks = []string{"JPN", "USA", "SGP", "TWN", "KOR", "GBR"}
	}
	for i := range cfg.RegionFallbacks {
		cfg.RegionFallbacks[i] = normaliseRegion(cfg.RegionFallbacks[i], cfg.RegionAliases)
	}
	if cfg.ClientHandshakeTimeout <= 0 {
		cfg.ClientHandshakeTimeout = 10 * time.Second
	}
	if cfg.UpstreamDialTimeout <= 0 {
		cfg.UpstreamDialTimeout = 20 * time.Second
	}
	if cfg.MaxHealthConcurrency <= 0 {
		cfg.MaxHealthConcurrency = 1
	}
	if cfg.SyncTimeout <= 0 {
		cfg.SyncTimeout = 45 * time.Second
	}
	if cfg.SpeedWeightCap <= 0 {
		cfg.SpeedWeightCap = 1.8
	}
	if cfg.MinNodeWeight <= 0 {
		cfg.MinNodeWeight = 0.5
	}
	if cfg.MaxNodeWeight <= 0 {
		cfg.MaxNodeWeight = 2.5
	}
	if cfg.MinNodeWeight > cfg.MaxNodeWeight {
		cfg.MinNodeWeight, cfg.MaxNodeWeight = cfg.MaxNodeWeight, cfg.MinNodeWeight
	}
	if cfg.AutoSync.Interval <= 0 {
		cfg.AutoSync.Interval = cfg.ReloadInterval
	}
	if cfg.AutoSync.ListenPortStart <= 0 {
		cfg.AutoSync.ListenPortStart = 19100
	}
	if cfg.AutoSync.ListenPortEnd <= 0 {
		cfg.AutoSync.ListenPortEnd = 19199
	}
	if cfg.AutoSync.ListenPortStart > cfg.AutoSync.ListenPortEnd {
		cfg.AutoSync.ListenPortStart, cfg.AutoSync.ListenPortEnd = cfg.AutoSync.ListenPortEnd, cfg.AutoSync.ListenPortStart
	}
	if cfg.AutoSync.ProxyHost == "" {
		cfg.AutoSync.ProxyHost = "proxy-account-router"
	}
	if cfg.AutoSync.CLIProxyAPI.AuthDir == "" {
		cfg.AutoSync.CLIProxyAPI.AuthDir = "/cliproxyapi/auths"
	}
	if cfg.AutoSync.Sub2API.Host == "" {
		cfg.AutoSync.Sub2API.Host = "sub2api-postgres"
	}
	if cfg.AutoSync.Sub2API.Port <= 0 {
		cfg.AutoSync.Sub2API.Port = 5432
	}
	if cfg.AutoSync.Sub2API.EnvFile == "" {
		cfg.AutoSync.Sub2API.EnvFile = "/sub2api/.env"
	}
	if values, err := readSimpleEnvFile(cfg.AutoSync.Sub2API.EnvFile); err == nil {
		if cfg.AutoSync.Sub2API.User == "" {
			cfg.AutoSync.Sub2API.User = values["POSTGRES_USER"]
		}
		if cfg.AutoSync.Sub2API.Password == "" {
			cfg.AutoSync.Sub2API.Password = values["POSTGRES_PASSWORD"]
		}
		if cfg.AutoSync.Sub2API.DBName == "" {
			cfg.AutoSync.Sub2API.DBName = values["POSTGRES_DB"]
		}
	}
	if cfg.AutoSync.Sub2API.User == "" {
		cfg.AutoSync.Sub2API.User = "sub2api"
	}
	if cfg.AutoSync.Sub2API.DBName == "" {
		cfg.AutoSync.Sub2API.DBName = "sub2api"
	}
	if cfg.AutoSync.Sub2API.SSLMode == "" {
		cfg.AutoSync.Sub2API.SSLMode = "disable"
	}
	cfg.RegionAliases = normaliseAliases(cfg.RegionAliases)
	cfg.BlockedRegions = normaliseBlockedRegions(cfg.BlockedRegions, cfg.RegionAliases)
	for i := range cfg.Accounts {
		cfg.Accounts[i].Region = normaliseRegion(cfg.Accounts[i].Region, cfg.RegionAliases)
		cfg.Accounts[i].StrictRegion = cfg.Accounts[i].StrictRegion || cfg.Accounts[i].Region != ""
		fixed, fixedList, err := normaliseProxyRefs(cfg.Accounts[i].FixedProxy, cfg.Accounts[i].FixedProxies)
		if err != nil {
			return Config{}, fmt.Errorf("account %s fixed_proxy: %w", cfg.Accounts[i].Name, err)
		}
		cfg.Accounts[i].FixedProxy = fixed
		cfg.Accounts[i].FixedProxies = fixedList
		for j := range cfg.Accounts[i].PreferredRegions {
			cfg.Accounts[i].PreferredRegions[j] = normaliseRegion(cfg.Accounts[i].PreferredRegions[j], cfg.RegionAliases)
		}
		for j := range cfg.Accounts[i].FallbackRegions {
			cfg.Accounts[i].FallbackRegions[j] = normaliseRegion(cfg.Accounts[i].FallbackRegions[j], cfg.RegionAliases)
		}
	}
	return cfg, nil
}

func normaliseProxyRefs(single *ProxyRef, many []ProxyRef) (*ProxyRef, []ProxyRef, error) {
	raw := append([]ProxyRef(nil), many...)
	if single != nil {
		raw = append([]ProxyRef{*single}, raw...)
	}
	proxies := make([]ProxyRef, 0, len(raw))
	for _, item := range raw {
		proxy, err := normaliseProxyRef(item)
		if err != nil {
			return nil, nil, err
		}
		duplicate := false
		for _, existing := range proxies {
			if existing == proxy {
				duplicate = true
				break
			}
		}
		if !duplicate {
			proxies = append(proxies, proxy)
		}
	}
	if len(proxies) == 0 {
		return nil, nil, nil
	}
	if len(proxies) > 1 {
		return nil, nil, fmt.Errorf("multiple fixed proxies are not supported yet")
	}
	return &proxies[0], []ProxyRef{proxies[0]}, nil
}

func normaliseProxyRef(proxy ProxyRef) (ProxyRef, error) {
	proxy.Type = strings.ToLower(strings.TrimSpace(proxy.Type))
	proxy.URL = strings.TrimSpace(proxy.URL)
	if proxy.URL != "" {
		parsed, err := url.Parse(proxy.URL)
		if err != nil {
			return ProxyRef{}, err
		}
		scheme := strings.ToLower(parsed.Scheme)
		if scheme != "socks5" && scheme != "socks5h" {
			return ProxyRef{}, fmt.Errorf("unsupported proxy url scheme %q", parsed.Scheme)
		}
		if proxy.Type != "" && proxy.Type != "socks5" {
			return ProxyRef{}, fmt.Errorf("proxy type %q does not match url scheme %q", proxy.Type, parsed.Scheme)
		}
		proxy.Type = "socks5"
		proxy.Host = parsed.Hostname()
		if parsed.Port() != "" {
			port, err := strconv.Atoi(parsed.Port())
			if err != nil {
				return ProxyRef{}, fmt.Errorf("invalid url port")
			}
			proxy.Port = port
		}
		if parsed.User != nil {
			proxy.Username = parsed.User.Username()
			proxy.Password, _ = parsed.User.Password()
		}
	}
	if proxy.Type == "" {
		proxy.Type = "socks5"
	}
	if proxy.Type != "socks5" {
		return ProxyRef{}, fmt.Errorf("unsupported proxy type %q", proxy.Type)
	}
	proxy.Host = strings.TrimSpace(proxy.Host)
	proxy.Username = strings.TrimSpace(proxy.Username)
	if proxy.Host == "" {
		return ProxyRef{}, fmt.Errorf("host required")
	}
	if proxy.Port < 1 || proxy.Port > 65535 {
		return ProxyRef{}, fmt.Errorf("port must be 1-65535")
	}
	return proxy, nil
}

func proxyRefSlicesEqual(a, b []ProxyRef) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func stateHasFixedProxy(state *AccountState) bool {
	return primaryFixedProxyFromState(state) != nil
}

func primaryFixedProxyFromState(state *AccountState) *ProxyRef {
	if state == nil {
		return nil
	}
	if len(state.FixedProxies) > 0 {
		cp := state.FixedProxies[0]
		return &cp
	}
	if state.FixedProxy != nil {
		cp := *state.FixedProxy
		return &cp
	}
	return nil
}

func fixedProxiesFromState(state *AccountState) []ProxyRef {
	if state == nil {
		return nil
	}
	if len(state.FixedProxies) > 0 {
		return append([]ProxyRef(nil), state.FixedProxies...)
	}
	if state.FixedProxy != nil {
		return []ProxyRef{*state.FixedProxy}
	}
	return nil
}

func setFixedProxiesOnState(state *AccountState, proxies []ProxyRef) {
	if len(proxies) == 0 {
		state.FixedProxy = nil
		state.FixedProxies = nil
		return
	}
	state.FixedProxies = append([]ProxyRef(nil), proxies...)
	fixed := state.FixedProxies[0]
	state.FixedProxy = &fixed
}

func readSimpleEnvFile(path string) (map[string]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	out := map[string]string{}
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		value = strings.TrimSpace(value)
		value = strings.Trim(value, `"'`)
		out[strings.TrimSpace(key)] = value
	}
	return out, nil
}

func normaliseBlockedRegions(in []string, aliases map[string]string) []string {
	defaults := []string{
		"HKG", "RUS", "CHN", "IRN", "PRK", "SYR", "CUB", "VEN", "BLR", "AFG",
	}
	set := map[string]struct{}{}
	for _, region := range append(defaults, in...) {
		normalised := normaliseRegion(region, aliases)
		if normalised != "" {
			set[normalised] = struct{}{}
		}
	}
	out := make([]string, 0, len(set))
	for region := range set {
		out = append(out, region)
	}
	sort.Strings(out)
	return out
}

func normaliseAliases(in map[string]string) map[string]string {
	out := map[string]string{
		"US":  "USA",
		"USA": "USA",
		"RU":  "RUS",
		"RUS": "RUS",
		"CN":  "CHN",
		"CHN": "CHN",
		"JP":  "JPN",
		"JPN": "JPN",
		"KO":  "KOR",
		"KR":  "KOR",
		"KOR": "KOR",
		"SG":  "SGP",
		"SGP": "SGP",
		"TW":  "TWN",
		"TWN": "TWN",
		"HK":  "HKG",
		"HKG": "HKG",
		"UK":  "GBR",
		"GB":  "GBR",
		"GBR": "GBR",
	}
	for k, v := range in {
		out[strings.ToUpper(strings.TrimSpace(k))] = strings.ToUpper(strings.TrimSpace(v))
	}
	return out
}

func normaliseRegion(region string, aliases map[string]string) string {
	region = strings.ToUpper(strings.TrimSpace(region))
	if region == "" {
		return ""
	}
	if mapped := aliases[region]; mapped != "" {
		return mapped
	}
	return region
}

func regionBlocked(region string, blocked []string) bool {
	for _, blockedRegion := range blocked {
		if region == blockedRegion {
			return true
		}
	}
	return false
}

func (r *Router) loadState() error {
	data, err := os.ReadFile(r.cfg.StatePath)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var state StateFile
	if err := json.Unmarshal(data, &state); err != nil {
		return err
	}
	if state.Accounts == nil {
		return nil
	}
	for name, account := range state.Accounts {
		if account == nil {
			continue
		}
		fixed, fixedList, err := normaliseProxyRefs(account.FixedProxy, account.FixedProxies)
		if err != nil {
			return fmt.Errorf("account %s fixed_proxy: %w", name, err)
		}
		account.FixedProxy = fixed
		account.FixedProxies = fixedList
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.accounts = state.Accounts
	return nil
}

func (r *Router) saveStateLocked() error {
	state := StateFile{Accounts: r.accounts}
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepathDir(r.cfg.StatePath), 0o755); err != nil {
		return err
	}
	tmp := r.cfg.StatePath + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, r.cfg.StatePath)
}

func filepathDir(path string) string {
	idx := strings.LastIndexAny(path, `/\`)
	if idx < 0 {
		return "."
	}
	return path[:idx]
}

func (r *Router) reloadLoop(ctx context.Context) {
	ticker := time.NewTicker(r.cfg.ReloadInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if cfg, err := loadConfig(r.cfgPath); err == nil {
				r.mu.Lock()
				r.cfg = cfg
				r.mu.Unlock()
			} else {
				log.Printf("config reload warning: %v", err)
			}
			if err := r.reloadNodesAndAssignments(ctx); err != nil {
				log.Printf("node reload warning: %v", err)
			}
		}
	}
}

func (r *Router) accountConfigs(ctx context.Context, cfg Config) []AccountConfig {
	accounts := make([]AccountConfig, 0, len(cfg.Accounts))
	accounts = append(accounts, cfg.Accounts...)
	if !cfg.AutoSync.Enabled {
		return accounts
	}

	discovered, err := r.discoverPlatformAccounts(ctx, cfg)
	if err != nil {
		log.Printf("account sync discovery warning: %v", err)
		return accounts
	}
	usedPorts := map[int]struct{}{}
	for _, acct := range accounts {
		if acct.ListenPort > 0 {
			usedPorts[acct.ListenPort] = struct{}{}
		}
	}
	r.mu.RLock()
	for _, state := range r.accounts {
		if state.ListenPort > 0 {
			usedPorts[state.ListenPort] = struct{}{}
		}
	}
	r.mu.RUnlock()

	for _, acct := range discovered {
		if acct.ListenPort <= 0 {
			if state := r.accountStateSnapshot(acct.Name); state != nil {
				acct.ListenPort = state.ListenPort
			}
		}
		if acct.ListenPort <= 0 {
			port, ok := nextFreePort(cfg.AutoSync.ListenPortStart, cfg.AutoSync.ListenPortEnd, usedPorts)
			if !ok {
				log.Printf("no free account router port remains for %s", acct.Name)
				continue
			}
			acct.ListenPort = port
		}
		usedPorts[acct.ListenPort] = struct{}{}
		accounts = append(accounts, acct)
	}
	return dedupeAccountConfigs(accounts)
}

func (r *Router) accountStateSnapshot(name string) *AccountState {
	r.mu.RLock()
	defer r.mu.RUnlock()
	state := r.accounts[name]
	if state == nil {
		return nil
	}
	cp := *state
	return &cp
}

func nextFreePort(start, end int, used map[int]struct{}) (int, bool) {
	for port := start; port <= end; port++ {
		if _, ok := used[port]; !ok {
			return port, true
		}
	}
	return 0, false
}

func dedupeAccountConfigs(in []AccountConfig) []AccountConfig {
	seen := map[string]struct{}{}
	out := make([]AccountConfig, 0, len(in))
	for _, acct := range in {
		acct.Name = strings.TrimSpace(acct.Name)
		if acct.Name == "" {
			continue
		}
		if _, ok := seen[acct.Name]; ok {
			continue
		}
		seen[acct.Name] = struct{}{}
		out = append(out, acct)
	}
	return out
}

func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func (r *Router) reloadNodesAndAssignments(ctx context.Context) error {
	r.mu.RLock()
	cfg := r.cfg
	r.mu.RUnlock()
	accounts := r.accountConfigs(ctx, cfg)

	nodes, err := discoverNodes(cfg)
	if err != nil {
		return err
	}
	if len(nodes) == 0 {
		return fmt.Errorf("no region-tagged nodes found")
	}
	if cfg.NodeSource == "clash" && cfg.ManagedSingBox.Enabled {
		if err := r.restartManagedSingBox(ctx, cfg, nodes); err != nil {
			return fmt.Errorf("restart managed sing-box: %w", err)
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	oldCount := len(r.nodes)
	r.nodes = nodes
	for id := range r.health {
		if _, ok := nodes[id]; !ok {
			delete(r.health, id)
		}
	}
	for id := range nodes {
		if _, ok := r.health[id]; !ok {
			r.health[id] = &HealthState{Healthy: false}
		}
	}

	changed := false
	configured := map[string]struct{}{}
	for _, acct := range accounts {
		if strings.TrimSpace(acct.Name) == "" || acct.ListenPort <= 0 {
			continue
		}
		configured[acct.Name] = struct{}{}
		state, ok := r.accounts[acct.Name]
		isNewAccount := !ok
		if !ok {
			state = &AccountState{Name: acct.Name}
			r.accounts[acct.Name] = state
			changed = true
		}
		if state.Project != acct.Project || state.ListenPort != acct.ListenPort {
			state.Project = acct.Project
			state.ListenPort = acct.ListenPort
			state.UpdatedAt = time.Now().UTC()
			changed = true
		}
		if state.StrictRegion != acct.StrictRegion {
			state.StrictRegion = acct.StrictRegion
			state.UpdatedAt = time.Now().UTC()
			changed = true
		}
		if !stringSlicesEqual(state.FallbackRegions, acct.FallbackRegions) {
			state.FallbackRegions = append([]string(nil), acct.FallbackRegions...)
			state.UpdatedAt = time.Now().UTC()
			changed = true
		}
		if len(acct.FixedProxies) > 0 && !proxyRefSlicesEqual(fixedProxiesFromState(state), acct.FixedProxies) {
			setFixedProxiesOnState(state, acct.FixedProxies)
			state.CurrentNode = ""
			state.UpdatedAt = time.Now().UTC()
			changed = true
		}
		if acct.Region != "" && state.LockedRegion != acct.Region {
			state.LockedRegion = acct.Region
			state.CurrentNode = ""
			state.UpdatedAt = time.Now().UTC()
			changed = true
		}
		if state.LockedRegion == "" {
			state.LockedRegion = r.pickInitialRegionLocked(acct)
			state.UpdatedAt = time.Now().UTC()
			changed = true
		}
		if stateHasFixedProxy(state) {
			continue
		}
		if state.CurrentNode != "" && !r.nodeBelongsToCandidateRegionLocked(state.CurrentNode, state.LockedRegion, state.Name) {
			log.Printf("account %s clears missing or disallowed-region node %s for locked region %s", state.Name, state.CurrentNode, state.LockedRegion)
			state.CurrentNode = ""
			state.UpdatedAt = time.Now().UTC()
			changed = true
		}
		if isNewAccount && state.CurrentNode == "" && acct.Region == "" && state.LockedRegion != "" && !r.regionHasNodesLocked(state.LockedRegion, true) {
			if region := r.pickInitialHealthyRegionLocked(acct); region != "" && region != state.LockedRegion {
				log.Printf("account %s initial region %s has no healthy nodes; re-locking to %s before first successful assignment", state.Name, state.LockedRegion, region)
				state.LockedRegion = region
				state.UpdatedAt = time.Now().UTC()
				changed = true
			}
		}
		if state.CurrentNode == "" {
			if nodeID := r.pickNodeLocked(state.LockedRegion, state.CurrentNode, state.Name); nodeID != "" {
				if state.CurrentNode != nodeID {
					log.Printf("account %s locked to %s uses node %s", state.Name, state.LockedRegion, nodeID)
					state.CurrentNode = nodeID
					state.UpdatedAt = time.Now().UTC()
					changed = true
				}
			}
		}
	}
	for name := range r.accounts {
		if _, ok := configured[name]; !ok {
			delete(r.accounts, name)
			changed = true
		}
	}
	if changed {
		if err := r.saveStateLocked(); err != nil {
			log.Printf("save state warning: %v", err)
		}
	}
	if oldCount != len(nodes) {
		log.Printf("nodes reloaded: %d -> %d", oldCount, len(nodes))
	}
	r.syncListenersLocked(ctx, cfg, accounts)
	if cfg.AutoSync.Enabled {
		go r.syncPlatformConfigsOnce(cfg)
	}
	return nil
}

func (r *Router) syncListenersLocked(ctx context.Context, cfg Config, accounts []AccountConfig) {
	desired := map[int]string{}
	for _, acct := range accounts {
		if strings.TrimSpace(acct.Name) == "" || acct.ListenPort <= 0 {
			continue
		}
		if existing, ok := desired[acct.ListenPort]; ok && existing != acct.Name {
			log.Printf("listen port %d is duplicated by %s and %s; keeping %s", acct.ListenPort, existing, acct.Name, existing)
			continue
		}
		desired[acct.ListenPort] = acct.Name
	}

	for port, ln := range r.listeners {
		account := r.listenerAccounts[port]
		if desired[port] == account {
			continue
		}
		_ = ln.Close()
		delete(r.listeners, port)
		delete(r.listenerAccounts, port)
		log.Printf("closed listener on port %d for account %s", port, account)
	}

	for port, account := range desired {
		if _, ok := r.listeners[port]; ok {
			continue
		}
		addr := net.JoinHostPort(cfg.ListenHost, strconv.Itoa(port))
		ln, err := net.Listen("tcp", addr)
		if err != nil {
			log.Printf("open listener %s for account %s failed: %v", addr, account, err)
			continue
		}
		r.listeners[port] = ln
		r.listenerAccounts[port] = account
		log.Printf("account %s listens on %s", account, addr)
		go r.acceptLoop(ctx, ln, account)
	}
}

func (r *Router) discoverPlatformAccounts(ctx context.Context, cfg Config) ([]AccountConfig, error) {
	var accounts []AccountConfig
	if cfg.AutoSync.CLIProxyAPI.Enabled {
		cliAccounts, err := discoverCLIProxyAPIAccounts(cfg)
		if err != nil {
			log.Printf("cliproxyapi discovery warning: %v", err)
		} else {
			accounts = append(accounts, cliAccounts...)
		}
	}
	if cfg.AutoSync.Sub2API.Enabled {
		subAccounts, err := discoverSub2APIAccounts(ctx, cfg)
		if err != nil {
			log.Printf("sub2api discovery warning: %v", err)
		} else {
			accounts = append(accounts, subAccounts...)
		}
	}
	return accounts, nil
}

func discoverCLIProxyAPIAccounts(cfg Config) ([]AccountConfig, error) {
	entries, err := os.ReadDir(cfg.AutoSync.CLIProxyAPI.AuthDir)
	if err != nil {
		return nil, err
	}
	accounts := make([]AccountConfig, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		base := strings.TrimSuffix(entry.Name(), ".json")
		identity := strings.Replace(base, "-", ":", 1)
		accounts = append(accounts, AccountConfig{
			Name:    "cliproxyapi:" + identity,
			Project: "cliproxyapi",
		})
	}
	sort.Slice(accounts, func(i, j int) bool { return accounts[i].Name < accounts[j].Name })
	return accounts, nil
}

func discoverSub2APIAccounts(ctx context.Context, cfg Config) ([]AccountConfig, error) {
	db, err := openSub2APIDB(cfg)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	rows, err := db.QueryContext(ctx, `
		select id, platform, type, name
		from accounts
		where deleted_at is null and status = 'active'
		order by id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var accounts []AccountConfig
	for rows.Next() {
		var id int64
		var platform, accountType, name string
		if err := rows.Scan(&id, &platform, &accountType, &name); err != nil {
			return nil, err
		}
		accounts = append(accounts, AccountConfig{
			Name:    fmt.Sprintf("sub2api:%d", id),
			Project: "sub2api:" + platform + ":" + accountType + ":" + name,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return accounts, nil
}

func openSub2APIDB(cfg Config) (*sql.DB, error) {
	pg := cfg.AutoSync.Sub2API
	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s connect_timeout=5",
		pg.Host, pg.Port, pg.User, pg.Password, pg.DBName, pg.SSLMode)
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(5 * time.Minute)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func discoverNodes(cfg Config) (map[string]Node, error) {
	if cfg.NodeSource == "clash" {
		return discoverFromClashProfiles(cfg)
	}
	nodes := map[string]Node{}
	if appNodes, err := discoverFromAppConfig(cfg); err == nil {
		for id, node := range appNodes {
			nodes[id] = node
		}
	}
	if len(nodes) > 0 {
		return nodes, nil
	}
	return discoverFromSingBox(cfg)
}

type clashProxy map[string]any

func discoverFromClashProfiles(cfg Config) (map[string]Node, error) {
	if len(cfg.ClashProfilePaths) == 0 {
		return nil, fmt.Errorf("clash_profile_paths is empty")
	}
	nodes := map[string]Node{}
	usedTags := map[string]int{}
	port := cfg.ManagedSingBox.NodePortStart
	for _, path := range cfg.ClashProfilePaths {
		path = strings.TrimSpace(path)
		if path == "" {
			continue
		}
		proxies, err := readClashProxies(path)
		if err != nil {
			return nil, err
		}
		for _, proxy := range proxies {
			if port > cfg.ManagedSingBox.NodePortEnd {
				return nil, fmt.Errorf("managed sing-box node port range exhausted at %d", port)
			}
			name := strings.TrimSpace(anyString(proxy["name"]))
			proxyType := strings.ToLower(strings.TrimSpace(anyString(proxy["type"])))
			if name == "" || proxyType == "" {
				continue
			}
			if !supportedClashProxyType(proxyType) {
				continue
			}
			if nodeTagBlocked(name, proxyType) {
				continue
			}
			region := extractRegion(name, cfg.RegionAliases)
			if region == "" || regionBlocked(region, cfg.BlockedRegions) {
				continue
			}
			tag := uniqueSingBoxTag(name, usedTags)
			if _, err := clashProxyToSingBoxOutbound(proxy, tag); err != nil {
				log.Printf("skip clash proxy %s: %v", name, err)
				continue
			}
			id := strconv.Itoa(port)
			nodes[id] = Node{
				ID:     id,
				Port:   port,
				Tag:    tag,
				Target: name,
				Region: region,
				Type:   proxyType,
			}
			port++
		}
	}
	return nodes, nil
}

func readClashProxies(path string) ([]clashProxy, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var raw struct {
		Proxies []map[string]any `yaml:"proxies"`
	}
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return nil, err
	}
	out := make([]clashProxy, 0, len(raw.Proxies))
	for _, proxy := range raw.Proxies {
		if proxy == nil {
			continue
		}
		out = append(out, clashProxy(proxy))
	}
	return out, nil
}

func supportedClashProxyType(proxyType string) bool {
	switch strings.ToLower(proxyType) {
	case "trojan", "ss", "shadowsocks":
		return true
	default:
		return false
	}
}

func uniqueSingBoxTag(name string, used map[string]int) string {
	base := strings.TrimSpace(name)
	used[base]++
	if used[base] == 1 {
		return base
	}
	return fmt.Sprintf("%s #%d", base, used[base])
}

func managedSingBoxConfig(cfg Config) (map[string]any, error) {
	if len(cfg.ClashProfilePaths) == 0 {
		return nil, fmt.Errorf("clash_profile_paths is empty")
	}
	inbounds := make([]map[string]any, 0)
	outbounds := make([]map[string]any, 0)
	rules := make([]map[string]any, 0)
	usedTags := map[string]int{}
	port := cfg.ManagedSingBox.NodePortStart

	for _, path := range cfg.ClashProfilePaths {
		proxies, err := readClashProxies(path)
		if err != nil {
			return nil, err
		}
		for _, proxy := range proxies {
			if port > cfg.ManagedSingBox.NodePortEnd {
				return nil, fmt.Errorf("managed sing-box node port range exhausted at %d", port)
			}
			name := strings.TrimSpace(anyString(proxy["name"]))
			proxyType := strings.ToLower(strings.TrimSpace(anyString(proxy["type"])))
			if name == "" || proxyType == "" || !supportedClashProxyType(proxyType) {
				continue
			}
			if nodeTagBlocked(name, proxyType) {
				continue
			}
			region := extractRegion(name, cfg.RegionAliases)
			if region == "" || regionBlocked(region, cfg.BlockedRegions) {
				continue
			}
			tag := uniqueSingBoxTag(name, usedTags)
			outbound, err := clashProxyToSingBoxOutbound(proxy, tag)
			if err != nil {
				log.Printf("skip clash proxy %s: %v", name, err)
				continue
			}
			inTag := fmt.Sprintf("node-%d-%s", port, safeTag(tag))
			inbounds = append(inbounds, map[string]any{
				"type":        "socks",
				"tag":         inTag,
				"listen":      cfg.ManagedSingBox.ListenHost,
				"listen_port": port,
			})
			outbounds = append(outbounds, outbound)
			rules = append(rules, map[string]any{
				"inbound":  []string{inTag},
				"outbound": tag,
			})
			port++
		}
	}
	if len(outbounds) == 0 {
		return nil, fmt.Errorf("no supported clash proxies found")
	}
	outbounds = append(outbounds,
		map[string]any{"type": "direct", "tag": "direct"},
		map[string]any{"type": "block", "tag": "block"},
	)
	return map[string]any{
		"log": map[string]any{
			"level":     "info",
			"timestamp": true,
		},
		"dns": map[string]any{
			"strategy": "prefer_ipv4",
			"servers": []map[string]any{
				{"tag": "dns-bootstrap", "type": "udp", "server": "1.1.1.1", "server_port": 53},
				{"tag": "dns-direct", "type": "local"},
			},
			"final": "dns-bootstrap",
		},
		"inbounds":  inbounds,
		"outbounds": outbounds,
		"route": map[string]any{
			"default_domain_resolver": map[string]any{"server": "dns-bootstrap", "strategy": "prefer_ipv4"},
			"rules":                   rules,
			"final":                   "direct",
		},
	}, nil
}

func clashProxyToSingBoxOutbound(proxy clashProxy, tag string) (map[string]any, error) {
	proxyType := strings.ToLower(strings.TrimSpace(anyString(proxy["type"])))
	switch proxyType {
	case "trojan":
		server := strings.TrimSpace(anyString(proxy["server"]))
		password := strings.TrimSpace(anyString(proxy["password"]))
		port, ok := anyInt(proxy["port"])
		if server == "" || password == "" || !ok || port <= 0 || port > 65535 {
			return nil, fmt.Errorf("invalid trojan server/password/port")
		}
		out := map[string]any{
			"type":            "trojan",
			"tag":             tag,
			"server":          server,
			"server_port":     port,
			"password":        password,
			"domain_resolver": map[string]any{"server": "dns-bootstrap", "strategy": "prefer_ipv4"},
		}
		tls := map[string]any{"enabled": true}
		if sni := firstNonEmpty(anyString(proxy["sni"]), anyString(proxy["servername"])); sni != "" {
			tls["server_name"] = sni
		}
		if insecure, ok := anyBool(proxy["skip-cert-verify"]); ok {
			tls["insecure"] = insecure
		}
		out["tls"] = tls
		return out, nil
	case "ss", "shadowsocks":
		server := strings.TrimSpace(anyString(proxy["server"]))
		password := strings.TrimSpace(anyString(proxy["password"]))
		method := strings.TrimSpace(anyString(proxy["cipher"]))
		port, ok := anyInt(proxy["port"])
		if server == "" || password == "" || method == "" || !ok || port <= 0 || port > 65535 {
			return nil, fmt.Errorf("invalid shadowsocks server/password/cipher/port")
		}
		return map[string]any{
			"type":            "shadowsocks",
			"tag":             tag,
			"server":          server,
			"server_port":     port,
			"method":          method,
			"password":        password,
			"domain_resolver": map[string]any{"server": "dns-bootstrap", "strategy": "prefer_ipv4"},
		}, nil
	default:
		return nil, fmt.Errorf("unsupported type %s", proxyType)
	}
}

func anyString(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case fmt.Stringer:
		return x.String()
	case int:
		return strconv.Itoa(x)
	case int64:
		return strconv.FormatInt(x, 10)
	case float64:
		if x == float64(int64(x)) {
			return strconv.FormatInt(int64(x), 10)
		}
		return fmt.Sprintf("%v", x)
	default:
		return ""
	}
}

func anyInt(v any) (int, bool) {
	switch x := v.(type) {
	case int:
		return x, true
	case int64:
		return int(x), true
	case float64:
		if x == float64(int(x)) {
			return int(x), true
		}
	case string:
		n, err := strconv.Atoi(strings.TrimSpace(x))
		return n, err == nil
	}
	return 0, false
}

func anyBool(v any) (bool, bool) {
	switch x := v.(type) {
	case bool:
		return x, true
	case string:
		b, err := strconv.ParseBool(strings.TrimSpace(x))
		return b, err == nil
	default:
		return false, false
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func safeTag(tag string) string {
	tag = strings.TrimSpace(tag)
	var b strings.Builder
	for _, r := range tag {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '-' || r == '_':
			b.WriteRune(r)
		default:
			b.WriteByte('-')
		}
	}
	out := strings.Trim(b.String(), "-")
	if out == "" {
		return "proxy"
	}
	if len(out) > 48 {
		return out[:48]
	}
	return out
}

func discoverFromAppConfig(cfg Config) (map[string]Node, error) {
	data, err := os.ReadFile(cfg.AppConfigPath)
	if err != nil {
		return nil, err
	}
	outboundTypes := singBoxOutboundTypes(cfg.SingBoxConfigPath)
	var raw struct {
		Ports []struct {
			Port   int    `json:"port"`
			Tag    string `json:"tag"`
			Target string `json:"target"`
		} `json:"ports"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, err
	}
	nodes := map[string]Node{}
	for _, p := range raw.Ports {
		if p.Port <= 0 {
			continue
		}
		text := p.Tag + " " + p.Target
		if nodeTagBlocked(text, outboundTypes[p.Target]) {
			continue
		}
		region := extractRegion(text, cfg.RegionAliases)
		if region == "" || regionBlocked(region, cfg.BlockedRegions) {
			continue
		}
		id := strconv.Itoa(p.Port)
		nodes[id] = Node{ID: id, Port: p.Port, Tag: p.Tag, Target: p.Target, Region: region}
	}
	return nodes, nil
}

func discoverFromSingBox(cfg Config) (map[string]Node, error) {
	data, err := os.ReadFile(cfg.SingBoxConfigPath)
	if err != nil {
		return nil, err
	}
	var raw struct {
		Inbounds []struct {
			ListenPort int    `json:"listen_port"`
			Tag        string `json:"tag"`
			Type       string `json:"type"`
		} `json:"inbounds"`
		Route struct {
			Rules []struct {
				Inbound  []string `json:"inbound"`
				Outbound string   `json:"outbound"`
			} `json:"rules"`
		} `json:"route"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, err
	}
	targets := map[string]string{}
	for _, rule := range raw.Route.Rules {
		for _, inbound := range rule.Inbound {
			targets[inbound] = rule.Outbound
		}
	}
	nodes := map[string]Node{}
	for _, inbound := range raw.Inbounds {
		if inbound.Type != "socks" || inbound.ListenPort <= 0 {
			continue
		}
		target := targets[inbound.Tag]
		text := inbound.Tag + " " + target
		if nodeTagBlocked(text, "") {
			continue
		}
		region := extractRegion(text, cfg.RegionAliases)
		if region == "" || regionBlocked(region, cfg.BlockedRegions) {
			continue
		}
		id := strconv.Itoa(inbound.ListenPort)
		nodes[id] = Node{ID: id, Port: inbound.ListenPort, Tag: inbound.Tag, Target: target, Region: region}
	}
	return nodes, nil
}

var regionRe = regexp.MustCompile(`(?i)(?:^|[^A-Z0-9])(AFG|ARE|BLR|BRA|CHN|CN|CUB|DEU|FRA|GBR|HKG|IDN|IRN|JPN|KOR|MYS|PHL|PRK|RUS|RU|SGP|SYR|THA|TUR|TWN|USA|VEN|VNM|US|JP|KO|KR|SG|TW|HK|GB|UK)(?:[^A-Z0-9]|$)`)

func singBoxOutboundTypes(path string) map[string]string {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var raw struct {
		Outbounds []struct {
			Tag  string `json:"tag"`
			Type string `json:"type"`
		} `json:"outbounds"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil
	}
	out := map[string]string{}
	for _, outbound := range raw.Outbounds {
		if outbound.Tag != "" {
			out[outbound.Tag] = strings.ToLower(outbound.Type)
		}
	}
	return out
}

func nodeTagBlocked(text, outboundType string) bool {
	lower := strings.ToLower(text)
	if strings.EqualFold(outboundType, "vmess") {
		return true
	}
	for _, marker := range []string{
		"剩余流量", "套餐到期", "下次重置", "永久地址", "telegram", "v2ny", "www.", "欢迎加入",
		"官方", "群组", "大陆访问", "节点失效", "更新订阅", "traffic:", "expire:",
	} {
		if strings.Contains(lower, strings.ToLower(marker)) {
			return true
		}
	}
	return false
}

func extractRegion(text string, aliases map[string]string) string {
	for marker, region := range map[string]string{
		"美国":    "USA",
		"日本":    "JPN",
		"新加坡":   "SGP",
		"台湾":    "TWN",
		"韩国":    "KOR",
		"英国":    "GBR",
		"德国":    "DEU",
		"法国":    "FRA",
		"马来西亚":  "MYS",
		"泰国":    "THA",
		"土耳其":   "TUR",
		"菲律宾":   "PHL",
		"印度尼西亚": "IDN",
		"印尼":    "IDN",
		"越南":    "VNM",
		"巴西":    "BRA",
		"阿联酋":   "ARE",
		"香港":    "HKG",
	} {
		if strings.Contains(text, marker) {
			return normaliseRegion(region, aliases)
		}
	}
	matches := regionRe.FindAllStringSubmatch(text, -1)
	for _, match := range matches {
		if len(match) < 2 {
			continue
		}
		region := normaliseRegion(match[1], aliases)
		if region != "" {
			return region
		}
	}
	return ""
}

func (r *Router) pickInitialRegionLocked(acct AccountConfig) string {
	if region := r.pickInitialHealthyRegionLocked(acct); region != "" {
		return region
	}
	return r.pickInitialRegionByCapacityLocked(acct, false)
}

func (r *Router) pickInitialHealthyRegionLocked(acct AccountConfig) string {
	return r.pickInitialRegionByCapacityLocked(acct, true)
}

func (r *Router) pickInitialRegionByCapacityLocked(acct AccountConfig, healthyOnly bool) string {
	regionCounts := map[string]int{}
	for _, state := range r.accounts {
		if state.LockedRegion != "" {
			regionCounts[state.LockedRegion]++
		}
	}
	for _, region := range acct.PreferredRegions {
		if r.regionHasNodesLocked(region, healthyOnly) {
			return region
		}
	}
	type regionScore struct {
		region string
		score  float64
	}
	scores := make([]regionScore, 0)
	for _, region := range r.availableRegionsLocked(healthyOnly) {
		capacity := r.regionCapacityLocked(region, healthyOnly)
		if capacity <= 0 {
			continue
		}
		assigned := float64(regionCounts[region])
		scores = append(scores, regionScore{
			region: region,
			score:  (assigned + 1) / capacity,
		})
	}
	if len(scores) == 0 {
		return ""
	}
	sort.Slice(scores, func(i, j int) bool {
		if scores[i].score == scores[j].score {
			return scores[i].region < scores[j].region
		}
		return scores[i].score < scores[j].score
	})
	return scores[0].region
}

func (r *Router) availableRegionsLocked(healthyOnly bool) []string {
	set := map[string]struct{}{}
	for id, node := range r.nodes {
		if healthyOnly && !r.isHealthyLocked(id) {
			continue
		}
		set[node.Region] = struct{}{}
	}
	regions := make([]string, 0, len(set))
	for region := range set {
		regions = append(regions, region)
	}
	sort.Strings(regions)
	return regions
}

func (r *Router) regionHasNodesLocked(region string, healthyOnly bool) bool {
	for id, node := range r.nodes {
		if node.Region != region {
			continue
		}
		if healthyOnly && !r.isHealthyLocked(id) {
			continue
		}
		return true
	}
	return false
}

func (r *Router) nodeUsableForAccountLocked(nodeID, region, account string) bool {
	if !r.nodeBelongsToCandidateRegionLocked(nodeID, region, account) {
		return false
	}
	return r.isHealthyLocked(nodeID)
}

func (r *Router) nodeBelongsToRegionLocked(nodeID, region string) bool {
	node, ok := r.nodes[nodeID]
	return ok && node.Region == region
}

func (r *Router) nodeBelongsToCandidateRegionLocked(nodeID, region, account string) bool {
	node, ok := r.nodes[nodeID]
	if !ok {
		return false
	}
	for _, candidate := range r.candidateRegionsLocked(region, account) {
		if node.Region == candidate {
			return true
		}
	}
	return false
}

func (r *Router) isHealthyLocked(nodeID string) bool {
	h := r.health[nodeID]
	return h == nil || h.Healthy
}

func (r *Router) pickNodeLocked(region, avoid, account string) string {
	for _, candidateRegion := range r.candidateRegionsLocked(region, account) {
		if nodeID := r.pickNodeInRegionLocked(candidateRegion, avoid, account); nodeID != "" {
			return nodeID
		}
	}
	return ""
}

func (r *Router) candidateRegionsLocked(region, account string) []string {
	out := []string{}
	add := func(candidate string) {
		candidate = normaliseRegion(candidate, r.cfg.RegionAliases)
		if candidate == "" || regionBlocked(candidate, r.cfg.BlockedRegions) {
			return
		}
		for _, existing := range out {
			if existing == candidate {
				return
			}
		}
		out = append(out, candidate)
	}
	add(region)
	state := r.accounts[account]
	if state != nil && state.StrictRegion {
		return out
	}
	if !r.cfg.AllowRegionFallback {
		return out
	}
	if state != nil {
		for _, fallback := range state.FallbackRegions {
			add(fallback)
		}
		if state.LockedRegion != "" {
			return out
		}
	}
	for _, fallback := range r.cfg.RegionFallbacks {
		add(fallback)
	}
	return out
}

func (r *Router) pickNodeInRegionLocked(region, avoid, account string) string {
	type nodeScore struct {
		id    string
		score float64
	}
	counts := r.assignedNodeCountsLocked()
	candidates := make([]nodeScore, 0)
	for id, node := range r.nodes {
		if node.Region == region && r.isHealthyLocked(id) && id != avoid {
			weight := r.nodeWeightLocked(id)
			if weight <= 0 {
				continue
			}
			assigned := float64(counts[id])
			if state := r.accounts[account]; state != nil && state.CurrentNode == id {
				assigned--
			}
			if assigned < 0 {
				assigned = 0
			}
			candidates = append(candidates, nodeScore{id: id, score: (assigned + 1) / weight})
		}
	}
	if len(candidates) == 0 && avoid != "" {
		if node, ok := r.nodes[avoid]; ok && node.Region == region && r.isHealthyLocked(avoid) {
			return avoid
		}
	}
	if len(candidates) == 0 {
		return ""
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].score == candidates[j].score {
			return candidates[i].id < candidates[j].id
		}
		return candidates[i].score < candidates[j].score
	})
	return candidates[0].id
}

func (r *Router) assignedNodeCountsLocked() map[string]int {
	counts := map[string]int{}
	for _, state := range r.accounts {
		if state.CurrentNode != "" {
			counts[state.CurrentNode]++
		}
	}
	return counts
}

func (r *Router) regionCapacityLocked(region string, healthyOnly bool) float64 {
	total := 0.0
	for id, node := range r.nodes {
		if node.Region == region && r.isHealthyLocked(id) {
			total += r.nodeWeightLocked(id)
		}
	}
	if total > 0 || healthyOnly {
		return total
	}
	for _, node := range r.nodes {
		if node.Region == region {
			total += 1
		}
	}
	return total
}

func (r *Router) nodeWeightLocked(nodeID string) float64 {
	h := r.health[nodeID]
	if h == nil || h.LatencyEWMAms <= 0 {
		return 1
	}
	weight := 1000.0 / h.LatencyEWMAms
	if weight > r.cfg.SpeedWeightCap {
		weight = r.cfg.SpeedWeightCap
	}
	if weight < r.cfg.MinNodeWeight {
		weight = r.cfg.MinNodeWeight
	}
	if weight > r.cfg.MaxNodeWeight {
		weight = r.cfg.MaxNodeWeight
	}
	return weight
}

func (r *Router) healthLoop(ctx context.Context) {
	r.runHealthCheck(ctx)
	ticker := time.NewTicker(r.cfg.HealthInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.runHealthCheck(ctx)
		}
	}
}

func (r *Router) runHealthCheck(ctx context.Context) {
	r.mu.RLock()
	cfg := r.cfg
	nodes := make([]Node, 0, len(r.nodes))
	for _, node := range r.nodes {
		nodes = append(nodes, node)
	}
	r.mu.RUnlock()
	rand.New(rand.NewSource(time.Now().UnixNano())).Shuffle(len(nodes), func(i, j int) {
		nodes[i], nodes[j] = nodes[j], nodes[i]
	})

	var wg sync.WaitGroup
	sem := make(chan struct{}, cfg.MaxHealthConcurrency)
	for _, node := range nodes {
		node := node
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				return
			}
			latency, results, err := checkNode(ctx, cfg, node)
			r.updateHealth(node.ID, latency, results, err)
		}()
	}
	wg.Wait()
	r.reconcileAssignments()
}

func checkNode(ctx context.Context, cfg Config, node Node) (time.Duration, []TargetHealthResult, error) {
	transport := &http.Transport{
		Proxy: nil,
		DialContext: func(dialCtx context.Context, network, addr string) (net.Conn, error) {
			if network != "tcp" && network != "tcp4" && network != "tcp6" {
				return nil, fmt.Errorf("unsupported network %s", network)
			}
			return dialViaSocks5(dialCtx, net.JoinHostPort(cfg.UpstreamSocksHost, strconv.Itoa(node.Port)), addr)
		},
	}
	defer transport.CloseIdleConnections()
	client := &http.Client{
		Transport: transport,
		Timeout:   cfg.HealthTimeout,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 3 {
				return http.ErrUseLastResponse
			}
			return nil
		},
	}

	successes := 0
	var totalLatency time.Duration
	results := make([]TargetHealthResult, 0, len(cfg.HealthTargets))
	for _, target := range cfg.HealthTargets {
		result := TargetHealthResult{Name: target.Name, URL: target.URL}
		targetURL, err := url.Parse(target.URL)
		if err != nil {
			result.Error = err.Error()
			results = append(results, result)
			continue
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL.String(), nil)
		if err != nil {
			result.Error = err.Error()
			results = append(results, result)
			continue
		}
		req.Header.Set("User-Agent", "proxy-account-router/0.1")
		start := time.Now()
		resp, err := client.Do(req)
		if err != nil {
			result.Error = err.Error()
			results = append(results, result)
			continue
		}
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		_ = resp.Body.Close()
		result.Status = resp.StatusCode
		result.LatencyMS = time.Since(start).Milliseconds()
		if statusAccepted(resp.StatusCode, target.ExpectStatuses) {
			result.OK = true
			successes++
			totalLatency += time.Duration(result.LatencyMS) * time.Millisecond
			results = append(results, result)
			if successes >= cfg.HealthMinSuccesses {
				return totalLatency / time.Duration(successes), results, nil
			}
		} else {
			result.Error = "unexpected status " + resp.Status
			results = append(results, result)
		}
	}
	if successes < cfg.HealthMinSuccesses {
		return 0, results, fmt.Errorf("health targets passed %d/%d, need %d", successes, len(cfg.HealthTargets), cfg.HealthMinSuccesses)
	}
	return totalLatency / time.Duration(successes), results, nil
}

func statusAccepted(status int, expected []int) bool {
	if len(expected) == 0 {
		return status >= 200 && status < 500
	}
	for _, want := range expected {
		if status == want {
			return true
		}
	}
	return false
}

func (r *Router) updateHealth(nodeID string, latency time.Duration, results []TargetHealthResult, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	h := r.health[nodeID]
	if h == nil {
		h = &HealthState{Healthy: true}
		r.health[nodeID] = h
	}
	h.CheckedAt = time.Now().UTC()
	h.Targets = results
	if err != nil {
		h.Failures++
		h.Successes = 0
		h.LastError = err.Error()
		if h.Failures >= r.cfg.UnhealthyAfterFailures && h.Healthy {
			h.Healthy = false
			h.LastUnhealthyAt = h.CheckedAt
			log.Printf("node %s marked unhealthy: %v", nodeID, err)
		}
		return
	}
	h.Successes++
	h.Failures = 0
	h.LastError = ""
	if latency > 0 {
		h.LatencyMS = latency.Milliseconds()
		if h.LatencyEWMAms <= 0 {
			h.LatencyEWMAms = float64(h.LatencyMS)
		} else {
			h.LatencyEWMAms = h.LatencyEWMAms*0.8 + float64(h.LatencyMS)*0.2
		}
	}
	h.LastHealthyAt = h.CheckedAt
	if h.Successes >= r.cfg.HealthyAfterSuccesses && !h.Healthy {
		h.Healthy = true
		log.Printf("node %s recovered", nodeID)
	}
}

func (r *Router) reconcileAssignments() {
	r.mu.Lock()
	defer r.mu.Unlock()
	changed := false
	for _, state := range r.accounts {
		if stateHasFixedProxy(state) {
			continue
		}
		if state.LockedRegion == "" {
			continue
		}
		if r.nodeUsableForAccountLocked(state.CurrentNode, state.LockedRegion, state.Name) {
			continue
		}
		next := r.pickNodeLocked(state.LockedRegion, state.CurrentNode, state.Name)
		if next == "" {
			if state.CurrentNode != "" && !r.nodeBelongsToRegionLocked(state.CurrentNode, state.LockedRegion) {
				log.Printf("account %s clears cross-region node %s for locked region %s", state.Name, state.CurrentNode, state.LockedRegion)
				state.CurrentNode = ""
				state.UpdatedAt = time.Now().UTC()
				changed = true
			}
			log.Printf("account %s has no healthy node in locked region %s", state.Name, state.LockedRegion)
			continue
		}
		log.Printf("account %s switches within %s: %s -> %s", state.Name, state.LockedRegion, state.CurrentNode, next)
		state.CurrentNode = next
		state.UpdatedAt = time.Now().UTC()
		changed = true
	}
	if changed {
		if err := r.saveStateLocked(); err != nil {
			log.Printf("save state warning: %v", err)
		}
	}
}

func (r *Router) syncPlatformConfigsOnce(cfg Config) {
	if !r.syncMu.TryLock() {
		log.Printf("platform sync skipped because previous sync is still running")
		return
	}
	defer r.syncMu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), cfg.SyncTimeout)
	defer cancel()
	r.syncPlatformConfigs(ctx, cfg)
}

func (r *Router) syncPlatformConfigs(ctx context.Context, cfg Config) {
	states := r.accountSnapshots()
	if cfg.AutoSync.CLIProxyAPI.Enabled {
		if err := syncCLIProxyAPI(ctx, cfg, states); err != nil {
			log.Printf("cliproxyapi sync warning: %v", err)
		}
	}
	if cfg.AutoSync.Sub2API.Enabled {
		if err := syncSub2API(ctx, cfg, states, r); err != nil {
			log.Printf("sub2api sync warning: %v", err)
		}
	}
}

func (r *Router) accountSnapshots() map[string]AccountState {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make(map[string]AccountState, len(r.accounts))
	for name, state := range r.accounts {
		if state == nil {
			continue
		}
		cp := *state
		if state.FallbackRegions != nil {
			cp.FallbackRegions = append([]string(nil), state.FallbackRegions...)
		}
		cp.FixedProxies = fixedProxiesFromState(state)
		cp.FixedProxy = primaryFixedProxyFromState(state)
		out[name] = cp
	}
	return out
}

func syncCLIProxyAPI(ctx context.Context, cfg Config, states map[string]AccountState) error {
	entries, err := os.ReadDir(cfg.AutoSync.CLIProxyAPI.AuthDir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		base := strings.TrimSuffix(entry.Name(), ".json")
		name := "cliproxyapi:" + strings.Replace(base, "-", ":", 1)
		state, ok := states[name]
		if !ok || state.ListenPort <= 0 {
			continue
		}
		path := cfg.AutoSync.CLIProxyAPI.AuthDir + "/" + entry.Name()
		if err := setJSONProxyURL(path, proxyURL(cfg, state.ListenPort)); err != nil {
			log.Printf("cliproxyapi %s proxy sync warning: %v", entry.Name(), err)
		}
	}
	return nil
}

func setJSONProxyURL(path, proxy string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	if current, _ := raw["proxy_url"].(string); current == proxy {
		return nil
	}
	raw["proxy_url"] = proxy
	out, err := json.MarshalIndent(raw, "", "  ")
	if err != nil {
		return err
	}
	out = append(out, '\n')
	tmp := path + ".tmp"
	info, statErr := os.Stat(path)
	mode := os.FileMode(0o600)
	if statErr == nil {
		mode = info.Mode().Perm()
	}
	if err := os.WriteFile(tmp, out, mode); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	log.Printf("cliproxyapi proxy_url updated for %s -> %s", path, proxy)
	return nil
}

func syncSub2API(ctx context.Context, cfg Config, states map[string]AccountState, router *Router) error {
	db, err := openSub2APIDB(cfg)
	if err != nil {
		return err
	}
	defer db.Close()
	for name, state := range states {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !strings.HasPrefix(name, "sub2api:") || state.ListenPort <= 0 {
			continue
		}
		id, err := strconv.ParseInt(strings.TrimPrefix(name, "sub2api:"), 10, 64)
		if err != nil {
			continue
		}
		proxyID, err := ensureSub2APIProxy(ctx, db, cfg, state)
		if err != nil {
			log.Printf("sub2api proxy ensure warning for account %d: %v", id, err)
			continue
		}
		if _, err := db.ExecContext(ctx, `
			update accounts
			set proxy_id = $1, updated_at = now()
			where id = $2 and deleted_at is null and (proxy_id is distinct from $1)`,
			proxyID, id); err != nil {
			log.Printf("sub2api account %d proxy sync warning: %v", id, err)
		}
	}
	if sub2APIProxyTempUnschedulableRecoveryEnabled(cfg) {
		if err := recoverSub2APIProxyTempUnschedulable(ctx, db, cfg, router); err != nil {
			log.Printf("sub2api proxy temp-unschedulable recovery warning: %v", err)
		}
	}
	return nil
}

func sub2APIProxyTempUnschedulableRecoveryEnabled(cfg Config) bool {
	enabled := cfg.AutoSync.Sub2API.RecoverProxyTempUnschedulable
	return enabled == nil || *enabled
}

func recoverSub2APIProxyTempUnschedulable(ctx context.Context, db *sql.DB, cfg Config, router *Router) error {
	if router == nil {
		return nil
	}
	rows, err := db.QueryContext(ctx, `
		select a.id, a.name, p.port
		from accounts a
		join proxies p on p.id = a.proxy_id
		where a.deleted_at is null
			and p.deleted_at is null
			and a.temp_unschedulable_until > now()
			and a.temp_unschedulable_reason like 'upstream transport error (proxy/network):%'
			and p.host = $1
			and p.name like 'proxy-account-router:sub2api:%'`,
		cfg.AutoSync.ProxyHost)
	if err != nil {
		return err
	}
	defer rows.Close()

	type tempAccount struct {
		id   int64
		name string
		port int
	}
	accounts := make([]tempAccount, 0)
	for rows.Next() {
		var a tempAccount
		if err := rows.Scan(&a.id, &a.name, &a.port); err != nil {
			return err
		}
		accounts = append(accounts, a)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if len(accounts) == 0 {
		return nil
	}

	cleared := 0
	for _, account := range accounts {
		if err := router.refreshAccountRecoveryHealth(account.port); err != nil {
			log.Printf("sub2api temp-unschedulable recovery health refresh skip for %s: %v", account.name, err)
			continue
		}
		recovered, err := router.recoverAccountProxyByPort(account.port)
		if err != nil {
			log.Printf("sub2api temp-unschedulable recovery skip for %s: %v", account.name, err)
			continue
		}
		if recovered {
			clearCtx, cancel := context.WithTimeout(context.Background(), sub2APIRecoveryDBTimeout(cfg))
			ok, err := clearSub2APIProxyTempUnschedulable(clearCtx, db, account.id)
			cancel()
			if err != nil {
				log.Printf("sub2api temp-unschedulable recovery clear skip for %s: %v", account.name, err)
				continue
			}
			if ok {
				cleared++
			}
		}
	}
	if cleared > 0 {
		log.Printf("sub2api cleared %d proxy/network temp-unschedulable account(s)", cleared)
	}
	return nil
}

func sub2APIRecoveryTimeout(cfg Config) time.Duration {
	timeout := cfg.SyncTimeout
	if timeout <= 0 {
		timeout = 45 * time.Second
	}
	minTimeout := cfg.HealthTimeout + 5*time.Second
	if minTimeout < 15*time.Second {
		minTimeout = 15 * time.Second
	}
	if timeout < minTimeout {
		timeout = minTimeout
	}
	return timeout
}

func sub2APIRecoveryDBTimeout(cfg Config) time.Duration {
	timeout := cfg.HealthTimeout
	if timeout <= 0 {
		timeout = 8 * time.Second
	}
	if timeout < 5*time.Second {
		timeout = 5 * time.Second
	}
	if timeout > 15*time.Second {
		timeout = 15 * time.Second
	}
	return timeout
}

func clearSub2APIProxyTempUnschedulable(ctx context.Context, db *sql.DB, accountID int64) (bool, error) {
	result, err := db.ExecContext(ctx, `
		update accounts
		set temp_unschedulable_until = null,
			temp_unschedulable_reason = null,
			updated_at = now()
		where id = $1
			and deleted_at is null
			and temp_unschedulable_until > now()
			and temp_unschedulable_reason like 'upstream transport error (proxy/network):%'`,
		accountID)
	if err != nil {
		return false, err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return rows > 0, nil
}

func (r *Router) refreshAccountRecoveryHealth(port int) error {
	if port <= 0 {
		return fmt.Errorf("invalid port")
	}
	r.mu.RLock()
	cfg := r.cfg
	nodes := r.recoveryHealthNodesForPortLocked(port)
	r.mu.RUnlock()
	if len(nodes) == 0 {
		return fmt.Errorf("account for port %d not found or has no candidate nodes", port)
	}

	ctx, cancel := context.WithTimeout(context.Background(), sub2APIRecoveryTimeout(cfg))
	defer cancel()

	var wg sync.WaitGroup
	concurrency := cfg.MaxHealthConcurrency
	if concurrency <= 0 {
		concurrency = 1
	}
	if concurrency > len(nodes) {
		concurrency = len(nodes)
	}
	sem := make(chan struct{}, concurrency)
	var completedMu sync.Mutex
	completed := 0
	for _, node := range nodes {
		node := node
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				return
			}
			latency, results, err := checkNode(ctx, cfg, node)
			if ctx.Err() != nil && err != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
				return
			}
			r.updateHealth(node.ID, latency, results, err)
			completedMu.Lock()
			completed++
			completedMu.Unlock()
		}()
	}
	wg.Wait()
	if ctx.Err() != nil {
		completedMu.Lock()
		defer completedMu.Unlock()
		if completed == 0 {
			return ctx.Err()
		}
	}
	return nil
}

func (r *Router) recoveryHealthNodesForPortLocked(port int) []Node {
	var state *AccountState
	for _, account := range r.accounts {
		if account != nil && account.ListenPort == port {
			state = account
			break
		}
	}
	if state == nil {
		return nil
	}
	if stateHasFixedProxy(state) {
		return nil
	}

	seen := map[string]struct{}{}
	nodes := make([]Node, 0)
	add := func(id string) {
		if id == "" {
			return
		}
		if _, ok := seen[id]; ok {
			return
		}
		node, ok := r.nodes[id]
		if !ok {
			return
		}
		seen[id] = struct{}{}
		nodes = append(nodes, node)
	}
	add(state.CurrentNode)
	for _, region := range r.candidateRegionsLocked(state.LockedRegion, state.Name) {
		for id, node := range r.nodes {
			if node.Region == region {
				add(id)
			}
		}
	}
	sort.Slice(nodes, func(i, j int) bool {
		if nodes[i].Region == nodes[j].Region {
			return nodes[i].ID < nodes[j].ID
		}
		return nodes[i].Region < nodes[j].Region
	})
	return nodes
}

func (r *Router) recoverAccountProxyByPort(port int) (bool, error) {
	if port <= 0 {
		return false, fmt.Errorf("invalid port")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, state := range r.accounts {
		if state != nil && state.ListenPort == port {
			if stateHasFixedProxy(state) {
				return false, nil
			}
			if r.nodeUsableForAccountLocked(state.CurrentNode, state.LockedRegion, state.Name) {
				return true, nil
			}
			next := r.pickNodeLocked(state.LockedRegion, state.CurrentNode, state.Name)
			if next == "" {
				return false, nil
			}
			log.Printf("account %s recovers proxy temp-unschedulable by switching %s -> %s", state.Name, state.CurrentNode, next)
			state.CurrentNode = next
			state.UpdatedAt = time.Now().UTC()
			if err := r.saveStateLocked(); err != nil {
				return false, err
			}
			return r.nodeUsableForAccountLocked(state.CurrentNode, state.LockedRegion, state.Name), nil
		}
	}
	return false, fmt.Errorf("account for port %d not found", port)
}

func ensureSub2APIProxy(ctx context.Context, db *sql.DB, cfg Config, state AccountState) (int64, error) {
	name := "proxy-account-router:" + state.Name
	var id int64
	err := db.QueryRowContext(ctx, `select id from proxies where name = $1 and deleted_at is null order by id limit 1`, name).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		err = db.QueryRowContext(ctx, `
			insert into proxies (name, protocol, host, port, status, created_at, updated_at)
			values ($1, 'socks5', $2, $3, 'active', now(), now())
			returning id`,
			name, cfg.AutoSync.ProxyHost, state.ListenPort).Scan(&id)
		return id, err
	}
	if err != nil {
		return 0, err
	}
	_, err = db.ExecContext(ctx, `
		update proxies
		set protocol = 'socks5', host = $1, port = $2, status = 'active', updated_at = now()
		where id = $3`,
		cfg.AutoSync.ProxyHost, state.ListenPort, id)
	return id, err
}

func proxyURL(cfg Config, port int) string {
	return "socks5://" + net.JoinHostPort(cfg.AutoSync.ProxyHost, strconv.Itoa(port))
}

func (r *Router) restartManagedSingBox(ctx context.Context, cfg Config, nodes map[string]Node) error {
	if len(nodes) == 0 {
		return fmt.Errorf("no nodes for managed sing-box")
	}
	managed, err := managedSingBoxConfig(cfg)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(cfg.ManagedSingBox.ConfigPath), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(managed, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if existing, err := os.ReadFile(cfg.ManagedSingBox.ConfigPath); err == nil && string(existing) == string(data) {
		r.mu.RLock()
		running := r.singBoxCmd != nil && r.singBoxCmd.Process != nil && r.singBoxCmd.ProcessState == nil
		r.mu.RUnlock()
		if running {
			return nil
		}
	}
	tmp := cfg.ManagedSingBox.ConfigPath + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	checkCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	check := exec.CommandContext(checkCtx, cfg.ManagedSingBox.BinaryPath, "check", "-c", tmp)
	check.Dir = cfg.ManagedSingBox.WorkDir
	checkOut, err := check.CombinedOutput()
	if err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("sing-box check failed: %w: %s", err, strings.TrimSpace(string(checkOut)))
	}
	if err := os.Rename(tmp, cfg.ManagedSingBox.ConfigPath); err != nil {
		_ = os.Remove(tmp)
		return err
	}

	cmd := exec.Command(cfg.ManagedSingBox.BinaryPath, "run", "-c", cfg.ManagedSingBox.ConfigPath)
	cmd.Dir = cfg.ManagedSingBox.WorkDir
	cmd.Stdout = logWriter("sing-box stdout: ")
	cmd.Stderr = logWriter("sing-box stderr: ")
	if err := cmd.Start(); err != nil {
		return err
	}

	r.mu.Lock()
	old := r.singBoxCmd
	r.singBoxCmd = cmd
	r.mu.Unlock()

	if old != nil && old.Process != nil {
		_ = old.Process.Kill()
		_, _ = old.Process.Wait()
	}
	go func() {
		err := cmd.Wait()
		if err != nil {
			log.Printf("managed sing-box exited: %v", err)
		} else {
			log.Printf("managed sing-box exited")
		}
	}()
	log.Printf("managed sing-box restarted with %d nodes on %s:%d-%d",
		len(nodes), cfg.ManagedSingBox.ListenHost, cfg.ManagedSingBox.NodePortStart, cfg.ManagedSingBox.NodePortStart+len(nodes)-1)
	return nil
}

type logWriter string

func (w logWriter) Write(p []byte) (int, error) {
	text := strings.TrimSpace(string(p))
	if text != "" {
		log.Print(string(w) + text)
	}
	return len(p), nil
}

func (r *Router) wait(ctx context.Context) {
	<-ctx.Done()
	r.mu.Lock()
	cmd := r.singBoxCmd
	r.singBoxCmd = nil
	r.mu.Unlock()
	if cmd != nil && cmd.Process != nil {
		_ = cmd.Process.Kill()
	}
}

func (r *Router) acceptLoop(ctx context.Context, ln net.Listener, account string) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
				if errors.Is(err, net.ErrClosed) {
					return
				}
				log.Printf("accept %s: %v", account, err)
				time.Sleep(250 * time.Millisecond)
				continue
			}
		}
		go r.handleClient(ctx, account, conn)
	}
}

func (r *Router) handleClient(ctx context.Context, account string, client net.Conn) {
	defer client.Close()
	handshakeTimeout := r.currentClientHandshakeTimeout()
	if handshakeTimeout > 0 {
		_ = client.SetReadDeadline(time.Now().Add(handshakeTimeout))
	}
	target, err := readSocks5Connect(client)
	if err != nil {
		return
	}
	_ = client.SetReadDeadline(time.Time{})

	upstream, err := r.dialAccountTarget(ctx, account, target)
	if err != nil {
		writeSocks5Reply(client, 0x05)
		log.Printf("route %s to %s failed: %v", account, target, err)
		return
	}
	defer upstream.Close()

	if err := writeSocks5Reply(client, 0x00); err != nil {
		return
	}
	proxyBoth(client, upstream)
}

func (r *Router) dialAccountTarget(ctx context.Context, account, target string) (net.Conn, error) {
	if fixed := r.currentFixedProxyForAccount(account); fixed != nil {
		dialCtx := ctx
		cancel := func() {}
		if timeout := r.currentUpstreamDialTimeout(); timeout > 0 {
			dialCtx, cancel = context.WithTimeout(ctx, timeout)
		}
		defer cancel()
		return dialViaSocks5Proxy(dialCtx, *fixed, target)
	}

	var lastErr error
	tried := map[string]struct{}{}
	for attempt := 0; attempt < 3; attempt++ {
		node, err := r.currentNodeForAccount(account)
		if err != nil {
			return nil, err
		}
		if _, seen := tried[node.ID]; seen {
			return nil, fmt.Errorf("no untried healthy node remains in %s: %w", node.Region, lastErr)
		}
		tried[node.ID] = struct{}{}
		upstreamAddr := net.JoinHostPort(r.currentUpstreamSocksHost(), strconv.Itoa(node.Port))
		dialCtx := ctx
		cancel := func() {}
		if timeout := r.currentUpstreamDialTimeout(); timeout > 0 {
			dialCtx, cancel = context.WithTimeout(ctx, timeout)
		}
		upstream, err := dialViaSocks5(dialCtx, upstreamAddr, target)
		cancel()
		if err == nil {
			return upstream, nil
		}
		lastErr = err
		r.markNodeFailureAndSwitch(account, node.ID, err)
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("no route attempt made")
	}
	return nil, lastErr
}

func (r *Router) currentFixedProxyForAccount(account string) *ProxyRef {
	r.mu.RLock()
	defer r.mu.RUnlock()
	state := r.accounts[account]
	return primaryFixedProxyFromState(state)
}

func (r *Router) currentUpstreamSocksHost() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.cfg.UpstreamSocksHost
}

func (r *Router) currentClientHandshakeTimeout() time.Duration {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.cfg.ClientHandshakeTimeout
}

func (r *Router) currentUpstreamDialTimeout() time.Duration {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.cfg.UpstreamDialTimeout
}

func (r *Router) markNodeFailureAndSwitch(account, nodeID string, err error) {
	r.updateHealth(nodeID, 0, nil, err)
	r.mu.Lock()
	defer r.mu.Unlock()
	state := r.accounts[account]
	if state == nil || state.CurrentNode != nodeID {
		return
	}
	next := r.pickNodeLocked(state.LockedRegion, nodeID, account)
	if next == "" {
		return
	}
	nextNode := r.nodes[next]
	log.Printf("account %s immediate switch for %s: %s -> %s (%s)", account, state.LockedRegion, nodeID, next, nextNode.Region)
	state.CurrentNode = next
	state.UpdatedAt = time.Now().UTC()
	if err := r.saveStateLocked(); err != nil {
		log.Printf("save state warning: %v", err)
	}
}

func (r *Router) currentNodeForAccount(account string) (Node, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	state := r.accounts[account]
	if state == nil {
		return Node{}, fmt.Errorf("unknown account")
	}
	node, ok := r.nodes[state.CurrentNode]
	if ok && r.nodeBelongsToCandidateRegionLocked(node.ID, state.LockedRegion, account) && r.isHealthyLocked(node.ID) {
		return node, nil
	}
	next := r.pickNodeLocked(state.LockedRegion, state.CurrentNode, account)
	if next == "" {
		if !ok {
			return Node{}, fmt.Errorf("assigned node missing and no healthy fallback in %s", state.LockedRegion)
		}
		if !r.nodeBelongsToCandidateRegionLocked(node.ID, state.LockedRegion, account) {
			return Node{}, fmt.Errorf("assigned node region mismatch and no healthy fallback for %s", state.LockedRegion)
		}
		return Node{}, fmt.Errorf("assigned node unhealthy and no healthy fallback for %s", state.LockedRegion)
	}
	node = r.nodes[next]
	log.Printf("account %s lazy switches for %s: %s -> %s (%s)", account, state.LockedRegion, state.CurrentNode, next, node.Region)
	state.CurrentNode = next
	state.UpdatedAt = time.Now().UTC()
	if err := r.saveStateLocked(); err != nil {
		log.Printf("save state warning: %v", err)
	}
	return node, nil
}

func readSocks5Connect(conn net.Conn) (string, error) {
	header := make([]byte, 2)
	if _, err := io.ReadFull(conn, header); err != nil {
		return "", err
	}
	if header[0] != 0x05 {
		return "", fmt.Errorf("unsupported socks version")
	}
	methods := make([]byte, int(header[1]))
	if _, err := io.ReadFull(conn, methods); err != nil {
		return "", err
	}
	if _, err := conn.Write([]byte{0x05, 0x00}); err != nil {
		return "", err
	}

	req := make([]byte, 4)
	if _, err := io.ReadFull(conn, req); err != nil {
		return "", err
	}
	if req[0] != 0x05 || req[1] != 0x01 {
		return "", fmt.Errorf("only CONNECT is supported")
	}
	host, err := readSocksAddr(conn, req[3])
	if err != nil {
		return "", err
	}
	portBytes := make([]byte, 2)
	if _, err := io.ReadFull(conn, portBytes); err != nil {
		return "", err
	}
	port := binary.BigEndian.Uint16(portBytes)
	return net.JoinHostPort(host, strconv.Itoa(int(port))), nil
}

func readSocksAddr(r io.Reader, atyp byte) (string, error) {
	switch atyp {
	case 0x01:
		buf := make([]byte, 4)
		if _, err := io.ReadFull(r, buf); err != nil {
			return "", err
		}
		return net.IP(buf).String(), nil
	case 0x03:
		lenBuf := make([]byte, 1)
		if _, err := io.ReadFull(r, lenBuf); err != nil {
			return "", err
		}
		buf := make([]byte, int(lenBuf[0]))
		if _, err := io.ReadFull(r, buf); err != nil {
			return "", err
		}
		return string(buf), nil
	case 0x04:
		buf := make([]byte, 16)
		if _, err := io.ReadFull(r, buf); err != nil {
			return "", err
		}
		return net.IP(buf).String(), nil
	default:
		return "", fmt.Errorf("unsupported address type %d", atyp)
	}
}

func writeSocks5Reply(conn net.Conn, code byte) error {
	_, err := conn.Write([]byte{0x05, code, 0x00, 0x01, 0, 0, 0, 0, 0, 0})
	return err
}

func dialViaSocks5(ctx context.Context, socksAddr, target string) (net.Conn, error) {
	host, portStr, err := net.SplitHostPort(socksAddr)
	if err != nil {
		return nil, err
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, err
	}
	return dialViaSocks5Proxy(ctx, ProxyRef{Type: "socks5", Host: host, Port: port}, target)
}

func dialViaSocks5Proxy(ctx context.Context, proxy ProxyRef, target string) (net.Conn, error) {
	switch proxy.Type {
	case "", "socks5":
	default:
		return nil, fmt.Errorf("unsupported fixed proxy type %q", proxy.Type)
	}
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", net.JoinHostPort(proxy.Host, strconv.Itoa(proxy.Port)))
	if err != nil {
		return nil, err
	}
	ok := false
	defer func() {
		if !ok {
			_ = conn.Close()
		}
	}()
	if deadline, okDeadline := ctx.Deadline(); okDeadline {
		_ = conn.SetDeadline(deadline)
		defer conn.SetDeadline(time.Time{})
	}
	methods := []byte{0x00}
	if proxy.Username != "" || proxy.Password != "" {
		methods = append(methods, 0x02)
	}
	hello := []byte{0x05, byte(len(methods))}
	hello = append(hello, methods...)
	if _, err := conn.Write(hello); err != nil {
		return nil, err
	}
	resp := make([]byte, 2)
	if _, err := io.ReadFull(conn, resp); err != nil {
		return nil, err
	}
	if resp[0] != 0x05 {
		return nil, fmt.Errorf("upstream socks version rejected")
	}
	switch resp[1] {
	case 0x00:
	case 0x02:
		if err := writeSocks5UserPassAuth(conn, proxy.Username, proxy.Password); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("upstream socks auth rejected")
	}

	host, portStr, err := net.SplitHostPort(target)
	if err != nil {
		return nil, err
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port < 1 || port > 65535 {
		return nil, fmt.Errorf("invalid target port")
	}
	req := []byte{0x05, 0x01, 0x00}
	if ip := net.ParseIP(host); ip != nil {
		if v4 := ip.To4(); v4 != nil {
			req = append(req, 0x01)
			req = append(req, v4...)
		} else {
			req = append(req, 0x04)
			req = append(req, ip.To16()...)
		}
	} else {
		if len(host) > 255 {
			return nil, fmt.Errorf("hostname too long")
		}
		req = append(req, 0x03, byte(len(host)))
		req = append(req, []byte(host)...)
	}
	portBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(portBytes, uint16(port))
	req = append(req, portBytes...)
	if _, err := conn.Write(req); err != nil {
		return nil, err
	}
	head := make([]byte, 4)
	if _, err := io.ReadFull(conn, head); err != nil {
		return nil, err
	}
	if head[1] != 0x00 {
		return nil, fmt.Errorf("upstream socks connect failed: %d", head[1])
	}
	if _, err := readSocksAddr(conn, head[3]); err != nil {
		return nil, err
	}
	if _, err := io.ReadFull(conn, make([]byte, 2)); err != nil {
		return nil, err
	}
	ok = true
	return conn, nil
}

func writeSocks5UserPassAuth(conn net.Conn, username, password string) error {
	if len(username) > 255 || len(password) > 255 {
		return fmt.Errorf("socks username/password too long")
	}
	req := []byte{0x01, byte(len(username))}
	req = append(req, []byte(username)...)
	req = append(req, byte(len(password)))
	req = append(req, []byte(password)...)
	if _, err := conn.Write(req); err != nil {
		return err
	}
	resp := make([]byte, 2)
	if _, err := io.ReadFull(conn, resp); err != nil {
		return err
	}
	if resp[0] != 0x01 || resp[1] != 0x00 {
		return fmt.Errorf("upstream socks username/password rejected")
	}
	return nil
}

func proxyBoth(a, b net.Conn) {
	done := make(chan struct{}, 2)
	go func() {
		_, _ = io.Copy(a, b)
		_ = a.SetDeadline(time.Now())
		done <- struct{}{}
	}()
	go func() {
		_, _ = io.Copy(b, a)
		_ = b.SetDeadline(time.Now())
		done <- struct{}{}
	}()
	<-done
}

type StatusSnapshot struct {
	Nodes    map[string]Node          `json:"nodes"`
	Health   map[string]*HealthState  `json:"health"`
	Accounts map[string]*AccountState `json:"accounts"`
}

type SummarySnapshot struct {
	NodeCount          int                     `json:"node_count"`
	HealthyNodeCount   int                     `json:"healthy_node_count"`
	Regions            map[string]RegionStatus `json:"regions"`
	AccountCount       int                     `json:"account_count"`
	PortPool           *PortPoolStatus         `json:"port_pool,omitempty"`
	Subscription       *SubscriptionSummary    `json:"subscription,omitempty"`
	UnroutableAccounts []AccountRouteIssue     `json:"unroutable_accounts,omitempty"`
	Alerts             []Alert                 `json:"alerts,omitempty"`
	Warnings           []string                `json:"warnings,omitempty"`
}

type RegionStatus struct {
	Nodes   int `json:"nodes"`
	Healthy int `json:"healthy"`
}

type AccountRouteIssue struct {
	Name         string `json:"name"`
	ListenPort   int    `json:"listen_port"`
	LockedRegion string `json:"locked_region"`
	StrictRegion bool   `json:"strict_region,omitempty"`
	CurrentNode  string `json:"current_node,omitempty"`
	Reason       string `json:"reason"`
}

type Alert struct {
	Severity string `json:"severity"`
	Type     string `json:"type"`
	Message  string `json:"message"`
}

type PortPoolStatus struct {
	Start     int  `json:"start"`
	End       int  `json:"end"`
	Capacity  int  `json:"capacity"`
	Used      int  `json:"used"`
	Remaining int  `json:"remaining"`
	Exhausted bool `json:"exhausted"`
	Low       bool `json:"low"`
}

type SubscriptionSummary struct {
	URL       string    `json:"url,omitempty"`
	URLs      []string  `json:"urls,omitempty"`
	UpdatedAt time.Time `json:"updated_at,omitempty"`
	NodeCount int       `json:"node_count,omitempty"`
	Warnings  []string  `json:"warnings,omitempty"`
}

func (r *Router) statusSnapshot() StatusSnapshot {
	r.mu.RLock()
	defer r.mu.RUnlock()
	nodes := make(map[string]Node, len(r.nodes))
	for k, v := range r.nodes {
		nodes[k] = v
	}
	health := make(map[string]*HealthState, len(r.health))
	for k, v := range r.health {
		if v == nil {
			continue
		}
		cp := *v
		if v.Targets != nil {
			cp.Targets = append([]TargetHealthResult(nil), v.Targets...)
		}
		health[k] = &cp
	}
	accounts := make(map[string]*AccountState, len(r.accounts))
	for k, v := range r.accounts {
		if v == nil {
			continue
		}
		cp := *v
		if v.FallbackRegions != nil {
			cp.FallbackRegions = append([]string(nil), v.FallbackRegions...)
		}
		cp.FixedProxies = fixedProxiesFromState(v)
		cp.FixedProxy = primaryFixedProxyFromState(v)
		accounts[k] = &cp
	}
	return StatusSnapshot{Nodes: nodes, Health: health, Accounts: accounts}
}

func (r *Router) summarySnapshot() SummarySnapshot {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := SummarySnapshot{
		NodeCount:    len(r.nodes),
		Regions:      map[string]RegionStatus{},
		AccountCount: len(r.accounts),
		PortPool:     r.portPoolStatusLocked(),
	}
	for id, node := range r.nodes {
		rs := out.Regions[node.Region]
		rs.Nodes++
		if r.isHealthyLocked(id) {
			rs.Healthy++
			out.HealthyNodeCount++
		}
		out.Regions[node.Region] = rs
	}
	for region, rs := range out.Regions {
		if rs.Nodes > 0 && rs.Healthy == 0 {
			out.addAlert("critical", "region_no_healthy_nodes", fmt.Sprintf("region %s has no healthy nodes", region))
		}
	}
	if out.PortPool != nil {
		switch {
		case out.PortPool.Exhausted:
			out.addAlert("critical", "account_port_pool_exhausted", fmt.Sprintf("account listen port pool %d-%d is exhausted", out.PortPool.Start, out.PortPool.End))
		case out.PortPool.Low:
			out.addAlert("warning", "account_port_pool_low", fmt.Sprintf("account listen port pool %d-%d has %d ports remaining", out.PortPool.Start, out.PortPool.End, out.PortPool.Remaining))
		}
	}
	for _, state := range r.accounts {
		if state == nil {
			continue
		}
		if stateHasFixedProxy(state) {
			continue
		}
		if r.nodeUsableForAccountLocked(state.CurrentNode, state.LockedRegion, state.Name) {
			continue
		}
		reason := "no healthy candidate node"
		if node, ok := r.nodes[state.CurrentNode]; ok && !r.isHealthyLocked(node.ID) {
			reason = "current node unhealthy"
		} else if state.CurrentNode == "" {
			reason = "no current node"
		}
		out.UnroutableAccounts = append(out.UnroutableAccounts, AccountRouteIssue{
			Name:         state.Name,
			ListenPort:   state.ListenPort,
			LockedRegion: state.LockedRegion,
			StrictRegion: state.StrictRegion,
			CurrentNode:  state.CurrentNode,
			Reason:       reason,
		})
		severity := "warning"
		alertType := "account_unroutable"
		if state.StrictRegion {
			severity = "critical"
			alertType = "strict_account_unroutable"
		}
		out.addAlert(severity, alertType, fmt.Sprintf("account %s on %d is not routable: %s", state.Name, state.ListenPort, reason))
	}
	if sub := readSubscriptionSummary(r.cfg.SubscriptionStatePath); sub != nil {
		out.Subscription = sub
		for _, warning := range sub.Warnings {
			out.addAlert("warning", "subscription_warning", warning)
		}
	}
	for _, alert := range out.Alerts {
		out.Warnings = append(out.Warnings, alert.Message)
	}
	sort.Slice(out.Alerts, func(i, j int) bool {
		if out.Alerts[i].Severity == out.Alerts[j].Severity {
			return out.Alerts[i].Message < out.Alerts[j].Message
		}
		return out.Alerts[i].Severity < out.Alerts[j].Severity
	})
	sort.Strings(out.Warnings)
	return out
}

func (s *SummarySnapshot) addAlert(severity, typ, message string) {
	s.Alerts = append(s.Alerts, Alert{Severity: severity, Type: typ, Message: message})
}

func (r *Router) portPoolStatusLocked() *PortPoolStatus {
	if !r.cfg.AutoSync.Enabled {
		return nil
	}
	start, end := r.cfg.AutoSync.ListenPortStart, r.cfg.AutoSync.ListenPortEnd
	if start <= 0 || end <= 0 {
		return nil
	}
	if start > end {
		start, end = end, start
	}
	used := map[int]struct{}{}
	for _, state := range r.accounts {
		if state == nil || state.ListenPort < start || state.ListenPort > end {
			continue
		}
		used[state.ListenPort] = struct{}{}
	}
	capacity := end - start + 1
	remaining := capacity - len(used)
	if remaining < 0 {
		remaining = 0
	}
	lowThreshold := capacity / 10
	if lowThreshold < 5 {
		lowThreshold = 5
	}
	return &PortPoolStatus{
		Start:     start,
		End:       end,
		Capacity:  capacity,
		Used:      len(used),
		Remaining: remaining,
		Exhausted: remaining == 0,
		Low:       remaining > 0 && remaining <= lowThreshold,
	}
}

func readSubscriptionSummary(path string) *SubscriptionSummary {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var raw struct {
		Nodes        []any `json:"nodes"`
		Subscription struct {
			URL      string   `json:"url"`
			URLs     []string `json:"urls"`
			Warnings []string `json:"warnings"`
		} `json:"subscription"`
		UpdatedAt time.Time `json:"updatedAt"`
		Warnings  []string  `json:"warnings"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return &SubscriptionSummary{Warnings: []string{"subscription-state parse failed: " + err.Error()}}
	}
	warnings := append([]string(nil), raw.Warnings...)
	warnings = append(warnings, raw.Subscription.Warnings...)
	if raw.UpdatedAt.IsZero() {
		warnings = append(warnings, "subscription updated_at is missing")
	} else if time.Since(raw.UpdatedAt) > 25*time.Hour {
		warnings = append(warnings, "subscription has not updated for more than 25h")
	}
	if len(raw.Nodes) == 0 {
		warnings = append(warnings, "subscription has zero nodes")
	}
	return &SubscriptionSummary{
		URL:       raw.Subscription.URL,
		URLs:      raw.Subscription.URLs,
		UpdatedAt: raw.UpdatedAt,
		NodeCount: len(raw.Nodes),
		Warnings:  warnings,
	}
}

func (r *Router) handleAccountAdmin(w http.ResponseWriter, req *http.Request) {
	name := strings.TrimPrefix(req.URL.Path, "/admin/accounts/")
	action := ""
	if idx := strings.IndexByte(name, '/'); idx >= 0 {
		action = strings.Trim(name[idx+1:], "/")
		name = name[:idx]
	}
	name, _ = url.PathUnescape(name)
	if strings.TrimSpace(name) == "" {
		http.Error(w, "account name required", http.StatusBadRequest)
		return
	}
	switch req.Method {
	case http.MethodGet:
		state, ok := r.accountAdminSnapshot(name)
		if !ok {
			http.Error(w, "account not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(state)
	case http.MethodPost:
		switch action {
		case "lock-region":
			r.handleLockRegion(w, req, name)
		case "set-fixed-proxy":
			r.handleSetFixedProxy(w, req, name)
		case "clear-fixed-proxy":
			if err := r.clearFixedProxy(name); err != nil {
				http.Error(w, err.Error(), http.StatusConflict)
				return
			}
			state, ok := r.accountAdminSnapshot(name)
			if !ok {
				http.Error(w, "account not found", http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(state)
		case "unlock-region":
			if err := r.unlockRegion(name); err != nil {
				http.Error(w, err.Error(), http.StatusConflict)
				return
			}
			state, ok := r.accountAdminSnapshot(name)
			if !ok {
				http.Error(w, "account not found", http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(state)
		case "switch":
			if err := r.switchAccountNow(name); err != nil {
				http.Error(w, err.Error(), http.StatusConflict)
				return
			}
			_, _ = w.Write([]byte("ok\n"))
		default:
			http.Error(w, "unknown action", http.StatusNotFound)
		}
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (r *Router) accountAdminSnapshot(name string) (map[string]any, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	state := r.accounts[name]
	if state == nil {
		return nil, false
	}
	account := *state
	if state.FallbackRegions != nil {
		account.FallbackRegions = append([]string(nil), state.FallbackRegions...)
	}
	account.FixedProxies = fixedProxiesFromState(state)
	account.FixedProxy = primaryFixedProxyFromState(state)
	out := map[string]any{"account": account}
	if node, ok := r.nodes[state.CurrentNode]; ok {
		out["node"] = node
	}
	if health := r.health[state.CurrentNode]; health != nil {
		cp := *health
		if health.Targets != nil {
			cp.Targets = append([]TargetHealthResult(nil), health.Targets...)
		}
		out["health"] = cp
	}
	out["candidate_regions"] = r.candidateRegionsLocked(state.LockedRegion, state.Name)
	return out, true
}

func (r *Router) handleLockRegion(w http.ResponseWriter, req *http.Request, name string) {
	var body struct {
		Region          string   `json:"region"`
		Strict          *bool    `json:"strict"`
		FallbackRegions []string `json:"fallback_regions"`
	}
	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	state := r.accounts[name]
	if state == nil {
		http.Error(w, "account not found", http.StatusNotFound)
		return
	}
	region := normaliseRegion(body.Region, r.cfg.RegionAliases)
	if region == "" {
		http.Error(w, "region required", http.StatusBadRequest)
		return
	}
	state.LockedRegion = region
	state.CurrentNode = ""
	setFixedProxiesOnState(state, nil)
	if body.Strict == nil {
		state.StrictRegion = true
	} else {
		state.StrictRegion = *body.Strict
	}
	state.FallbackRegions = state.FallbackRegions[:0]
	for _, fallback := range body.FallbackRegions {
		fallback = normaliseRegion(fallback, r.cfg.RegionAliases)
		if fallback != "" {
			state.FallbackRegions = append(state.FallbackRegions, fallback)
		}
	}
	if nodeID := r.pickNodeLocked(state.LockedRegion, "", state.Name); nodeID != "" {
		state.CurrentNode = nodeID
	}
	state.UpdatedAt = time.Now().UTC()
	if err := r.saveStateLocked(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_ = json.NewEncoder(w).Encode(state)
}

func (r *Router) handleSetFixedProxy(w http.ResponseWriter, req *http.Request, name string) {
	var body ProxyRef
	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	proxy, err := normaliseProxyRef(body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	state := r.accounts[name]
	if state == nil {
		http.Error(w, "account not found", http.StatusNotFound)
		return
	}
	setFixedProxiesOnState(state, []ProxyRef{proxy})
	state.CurrentNode = ""
	state.UpdatedAt = time.Now().UTC()
	if err := r.saveStateLocked(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_ = json.NewEncoder(w).Encode(state)
}

func (r *Router) clearFixedProxy(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	state := r.accounts[name]
	if state == nil {
		return fmt.Errorf("account not found")
	}
	setFixedProxiesOnState(state, nil)
	if state.LockedRegion == "" {
		state.LockedRegion = r.pickInitialRegionLocked(AccountConfig{Name: state.Name})
	}
	if nodeID := r.pickNodeLocked(state.LockedRegion, state.CurrentNode, state.Name); nodeID != "" {
		state.CurrentNode = nodeID
	}
	state.UpdatedAt = time.Now().UTC()
	return r.saveStateLocked()
}

func (r *Router) unlockRegion(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	state := r.accounts[name]
	if state == nil {
		return fmt.Errorf("account not found")
	}
	state.LockedRegion = ""
	state.StrictRegion = false
	state.FallbackRegions = nil
	setFixedProxiesOnState(state, nil)
	if nodeID := r.pickNodeLocked("", state.CurrentNode, state.Name); nodeID != "" {
		state.CurrentNode = nodeID
	} else {
		state.CurrentNode = ""
	}
	state.UpdatedAt = time.Now().UTC()
	return r.saveStateLocked()
}

func (r *Router) switchAccountNow(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	state := r.accounts[name]
	if state == nil {
		return fmt.Errorf("account not found")
	}
	if stateHasFixedProxy(state) {
		return fmt.Errorf("account has fixed proxy")
	}
	next := r.pickNodeLocked(state.LockedRegion, state.CurrentNode, state.Name)
	if next == "" {
		return fmt.Errorf("no healthy candidate node for %s", state.LockedRegion)
	}
	if next == state.CurrentNode {
		return nil
	}
	log.Printf("account %s admin switch for %s: %s -> %s", name, state.LockedRegion, state.CurrentNode, next)
	state.CurrentNode = next
	state.UpdatedAt = time.Now().UTC()
	return r.saveStateLocked()
}

func (r *Router) httpStatusServer(ctx context.Context) {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/status", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(r.statusSnapshot())
	})
	mux.HandleFunc("/summary", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(r.summarySnapshot())
	})
	mux.HandleFunc("/admin/reload", func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if err := r.reloadNodesAndAssignments(req.Context()); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/admin/health-check", func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		r.runHealthCheck(req.Context())
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/admin/accounts/", func(w http.ResponseWriter, req *http.Request) {
		r.handleAccountAdmin(w, req)
	})
	server := &http.Server{Addr: ":19080", Handler: mux}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Printf("status server: %v", err)
	}
}
