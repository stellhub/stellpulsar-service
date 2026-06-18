package config

import (
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/goccy/go-yaml"
	"github.com/stellhub/stellar"
)

const (
	defaultService                  = "stellpulsar-service"
	defaultInstanceID               = "stellpulsar-local"
	defaultHost                     = "127.0.0.1"
	defaultPriority                 = 100
	defaultWeight                   = 100
	defaultSnapshotPageSize         = 100
	defaultHTTPTimeout              = 5 * time.Second
	defaultWatchReconnectDelay      = 500 * time.Millisecond
	defaultWatchResyncInterval      = 30 * time.Second
	defaultDeltaFetchTimeout        = 3 * time.Second
	defaultFullSyncMaxRetries       = 3
	defaultWatchEventBuffer         = 64
	defaultServerLagRetryAfter      = 50 * time.Millisecond
	defaultBucketGCInterval         = 30 * time.Second
	defaultExpiredBucketGrace       = 2 * time.Minute
	defaultMaxBuckets               = 1_000_000
	defaultCompatiblePrevRevision   = 5 * time.Second
	defaultSessionHeartbeatInterval = 10 * time.Second
)

type Config struct {
	Instance      InstanceConfig
	StellOrbit    StellOrbitConfig
	Rules         RulesConfig
	GRPC          GRPCConfig
	Observability ObservabilityConfig
	Security      SecurityConfig
}

type InstanceConfig struct {
	Namespace string
	Service   string
	ID        string
	Host      string
	Port      int32
	Priority  int32
	Weight    int32
	Zone      string
	Version   string
}

type StellOrbitConfig struct {
	RuntimeEndpoint string
	HTTPTimeout     time.Duration
	AuthToken       string
}

type RulesConfig struct {
	SnapshotPageSize         int
	WatchReconnectDelay      time.Duration
	WatchResyncInterval      time.Duration
	DeltaFetchTimeout        time.Duration
	FullSyncMaxRetries       int
	WatchEventBuffer         int
	ServerLagRetryAfter      time.Duration
	BucketGCInterval         time.Duration
	ExpiredBucketGrace       time.Duration
	MaxBuckets               int
	CompatiblePrevRevision   time.Duration
	SessionHeartbeatInterval time.Duration
	AllowEmptyStartup        bool
}

type GRPCConfig struct {
	ClientName string
}

type ObservabilityConfig struct {
	TopologyMetrics bool
}

type SecurityConfig struct {
	AuthToken string
}

type fileConfig struct {
	StellPulsar fileStellPulsarConfig `yaml:"stellpulsar"`
}

type fileStellPulsarConfig struct {
	Instance      fileInstanceConfig      `yaml:"instance"`
	StellOrbit    fileStellOrbitConfig    `yaml:"stellorbit"`
	Rules         fileRulesConfig         `yaml:"rules"`
	GRPC          fileGRPCConfig          `yaml:"grpc"`
	Observability fileObservabilityConfig `yaml:"observability"`
	Security      fileSecurityConfig      `yaml:"security"`
}

type fileInstanceConfig struct {
	Priority int32 `yaml:"priority"`
	Weight   int32 `yaml:"weight"`
}

type fileStellOrbitConfig struct {
	RuntimeEndpoint string `yaml:"runtime_endpoint"`
	HTTPTimeout     string `yaml:"http_timeout"`
	AuthToken       string `yaml:"auth_token"`
}

type fileRulesConfig struct {
	SnapshotPageSize         int    `yaml:"snapshot_page_size"`
	WatchReconnectDelay      string `yaml:"watch_reconnect_delay"`
	WatchResyncInterval      string `yaml:"watch_resync_interval"`
	DeltaFetchTimeout        string `yaml:"delta_fetch_timeout"`
	FullSyncMaxRetries       int    `yaml:"full_sync_max_retries"`
	WatchEventBuffer         int    `yaml:"watch_event_buffer"`
	ServerLagRetryAfter      string `yaml:"server_lag_retry_after"`
	BucketGCInterval         string `yaml:"bucket_gc_interval"`
	ExpiredBucketGrace       string `yaml:"expired_bucket_grace"`
	MaxBuckets               int    `yaml:"max_buckets"`
	CompatiblePrevRevision   string `yaml:"compatible_previous_revision"`
	SessionHeartbeatInterval string `yaml:"session_heartbeat_interval"`
	AllowEmptyStartup        *bool  `yaml:"allow_empty_startup"`
}

type fileGRPCConfig struct {
	ClientName string `yaml:"client_name"`
}

type fileObservabilityConfig struct {
	TopologyMetrics *bool `yaml:"topology_metrics"`
}

type fileSecurityConfig struct {
	AuthToken string `yaml:"auth_token"`
}

func Load(stellarCfg stellar.Config) Config {
	stellarCfg = stellarCfg.Normalize()
	fileCfg := loadFileConfig()
	service := defaultService
	namespace := "default"
	instanceID := defaultInstanceID
	host := defaultHost
	port := int32(9090)
	zone := stellarCfg.Zone
	version := stellarCfg.Version

	if stellarCfg.Registry != nil {
		if strings.TrimSpace(stellarCfg.Registry.Namespace) != "" {
			namespace = stellarCfg.Registry.Namespace
		}
		if strings.TrimSpace(stellarCfg.Registry.Service) != "" {
			service = stellarCfg.Registry.Service
		}
		if strings.TrimSpace(stellarCfg.Registry.InstanceID) != "" {
			instanceID = stellarCfg.Registry.InstanceID
		}
		if strings.TrimSpace(stellarCfg.Registry.Zone) != "" {
			zone = stellarCfg.Registry.Zone
		}
		for _, endpoint := range stellarCfg.Registry.ServiceEndpoints {
			if endpoint.Protocol == "grpc" || endpoint.Name == "grpc" {
				host = chooseString(endpoint.Host, host)
				if endpoint.Port > 0 {
					port = int32(endpoint.Port)
				}
				break
			}
		}
	}
	if strings.TrimSpace(stellarCfg.AppName) != "" && service == defaultService {
		service = stellarCfg.AppName
	}
	if strings.TrimSpace(version) == "" {
		version = "dev"
	}

	allowEmpty := true
	if fileCfg.StellPulsar.Rules.AllowEmptyStartup != nil {
		allowEmpty = *fileCfg.StellPulsar.Rules.AllowEmptyStartup
	}
	topologyMetrics := true
	if fileCfg.StellPulsar.Observability.TopologyMetrics != nil {
		topologyMetrics = *fileCfg.StellPulsar.Observability.TopologyMetrics
	}
	return Config{
		Instance: InstanceConfig{
			Namespace: namespace,
			Service:   service,
			ID:        instanceID,
			Host:      host,
			Port:      port,
			Priority:  int32(envInt("STELLPULSAR_INSTANCE_PRIORITY", int(chooseInt32(fileCfg.StellPulsar.Instance.Priority, defaultPriority)))),
			Weight:    int32(envInt("STELLPULSAR_INSTANCE_WEIGHT", int(chooseInt32(fileCfg.StellPulsar.Instance.Weight, defaultWeight)))),
			Zone:      zone,
			Version:   version,
		},
		StellOrbit: StellOrbitConfig{
			RuntimeEndpoint: envString("STELLPULSAR_STELLORBIT_RUNTIME_ENDPOINT", fileCfg.StellPulsar.StellOrbit.RuntimeEndpoint),
			HTTPTimeout:     envDuration("STELLPULSAR_STELLORBIT_HTTP_TIMEOUT", durationFromText(fileCfg.StellPulsar.StellOrbit.HTTPTimeout, defaultHTTPTimeout)),
			AuthToken:       envString("STELLPULSAR_STELLORBIT_AUTH_TOKEN", fileCfg.StellPulsar.StellOrbit.AuthToken),
		},
		Rules: RulesConfig{
			SnapshotPageSize:         envInt("STELLPULSAR_RULES_SNAPSHOT_PAGE_SIZE", chooseInt(fileCfg.StellPulsar.Rules.SnapshotPageSize, defaultSnapshotPageSize)),
			WatchReconnectDelay:      envDuration("STELLPULSAR_RULES_WATCH_RECONNECT_DELAY", durationFromText(fileCfg.StellPulsar.Rules.WatchReconnectDelay, defaultWatchReconnectDelay)),
			WatchResyncInterval:      envDuration("STELLPULSAR_RULES_WATCH_RESYNC_INTERVAL", durationFromText(fileCfg.StellPulsar.Rules.WatchResyncInterval, defaultWatchResyncInterval)),
			DeltaFetchTimeout:        envDuration("STELLPULSAR_RULES_DELTA_FETCH_TIMEOUT", durationFromText(fileCfg.StellPulsar.Rules.DeltaFetchTimeout, defaultDeltaFetchTimeout)),
			FullSyncMaxRetries:       envInt("STELLPULSAR_RULES_FULL_SYNC_MAX_RETRIES", chooseInt(fileCfg.StellPulsar.Rules.FullSyncMaxRetries, defaultFullSyncMaxRetries)),
			WatchEventBuffer:         envInt("STELLPULSAR_RULES_WATCH_EVENT_BUFFER", chooseInt(fileCfg.StellPulsar.Rules.WatchEventBuffer, defaultWatchEventBuffer)),
			ServerLagRetryAfter:      envDuration("STELLPULSAR_RULES_SERVER_LAG_RETRY_AFTER", durationFromText(fileCfg.StellPulsar.Rules.ServerLagRetryAfter, defaultServerLagRetryAfter)),
			BucketGCInterval:         envDuration("STELLPULSAR_RULES_BUCKET_GC_INTERVAL", durationFromText(fileCfg.StellPulsar.Rules.BucketGCInterval, defaultBucketGCInterval)),
			ExpiredBucketGrace:       envDuration("STELLPULSAR_RULES_EXPIRED_BUCKET_GRACE", durationFromText(fileCfg.StellPulsar.Rules.ExpiredBucketGrace, defaultExpiredBucketGrace)),
			MaxBuckets:               envInt("STELLPULSAR_RULES_MAX_BUCKETS", chooseInt(fileCfg.StellPulsar.Rules.MaxBuckets, defaultMaxBuckets)),
			CompatiblePrevRevision:   envDuration("STELLPULSAR_RULES_COMPATIBLE_PREVIOUS_REVISION", durationFromText(fileCfg.StellPulsar.Rules.CompatiblePrevRevision, defaultCompatiblePrevRevision)),
			SessionHeartbeatInterval: envDuration("STELLPULSAR_SESSION_HEARTBEAT_INTERVAL", durationFromText(fileCfg.StellPulsar.Rules.SessionHeartbeatInterval, defaultSessionHeartbeatInterval)),
			AllowEmptyStartup:        envBool("STELLPULSAR_RULES_ALLOW_EMPTY_STARTUP", allowEmpty),
		},
		GRPC: GRPCConfig{
			ClientName: envString("STELLPULSAR_GRPC_CLIENT_NAME", fileCfg.StellPulsar.GRPC.ClientName),
		},
		Observability: ObservabilityConfig{
			TopologyMetrics: envBool("STELLPULSAR_OBSERVABILITY_TOPOLOGY_METRICS", topologyMetrics),
		},
		Security: SecurityConfig{
			AuthToken: envString("STELLPULSAR_SECURITY_AUTH_TOKEN", fileCfg.StellPulsar.Security.AuthToken),
		},
	}
}

func loadFileConfig() fileConfig {
	for _, path := range candidateConfigPaths() {
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		var cfg fileConfig
		if err := yaml.Unmarshal(data, &cfg); err != nil {
			continue
		}
		return cfg
	}
	return fileConfig{}
}

func candidateConfigPaths() []string {
	for _, key := range []string{"STELLAR_CONFIG_FILE", "STELLAR_CONFIG", "STELLAR_APPLICATION_CONFIG"} {
		if value := strings.TrimSpace(os.Getenv(key)); value != "" {
			return []string{value}
		}
	}
	dirs := make([]string, 0, 2)
	if mainDir := mainSourceDir(); mainDir != "" {
		dirs = append(dirs, mainDir)
	}
	if cwd, err := os.Getwd(); err == nil {
		dirs = append(dirs, cwd)
	}
	return configPathsInDirs(dirs...)
}

func configPathsInDirs(dirs ...string) []string {
	names := []string{"application.yml", "application.yaml"}
	seen := map[string]struct{}{}
	paths := make([]string, 0, len(dirs)*len(names))
	for _, dir := range dirs {
		if strings.TrimSpace(dir) == "" {
			continue
		}
		for _, name := range names {
			path := filepath.Join(dir, name)
			if _, ok := seen[path]; ok {
				continue
			}
			seen[path] = struct{}{}
			paths = append(paths, path)
		}
	}
	if len(paths) == 0 {
		return names
	}
	return paths
}

func mainSourceDir() string {
	pcs := make([]uintptr, 32)
	n := runtime.Callers(0, pcs)
	frames := runtime.CallersFrames(pcs[:n])
	for {
		frame, more := frames.Next()
		if frame.Function == "main.main" && frame.File != "" {
			return filepath.Dir(frame.File)
		}
		if !more {
			break
		}
	}
	return ""
}

func envString(key string, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}

func envInt(key string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func envDuration(key string, fallback time.Duration) time.Duration {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return durationFromText(value, fallback)
}

func envBool(key string, fallback bool) bool {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func durationFromText(value string, fallback time.Duration) time.Duration {
	value = strings.TrimSpace(value)
	if value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err == nil {
		return parsed
	}
	millis, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return time.Duration(millis) * time.Millisecond
}

func chooseString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func chooseInt(value int, fallback int) int {
	if value != 0 {
		return value
	}
	return fallback
}

func chooseInt32(value int32, fallback int32) int32 {
	if value != 0 {
		return value
	}
	return fallback
}
