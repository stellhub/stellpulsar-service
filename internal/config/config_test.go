package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stellhub/stellar"
)

func TestLoadReadsTopologyMetricsConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "application.yaml")
	if err := os.WriteFile(path, []byte(`
stellpulsar:
  observability:
    topology_metrics: false
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	t.Setenv("STELLAR_CONFIG_FILE", path)

	cfg := Load(stellar.Config{})
	if cfg.Observability.TopologyMetrics {
		t.Fatalf("expected topology metrics to be disabled by file config")
	}

	t.Setenv("STELLPULSAR_OBSERVABILITY_TOPOLOGY_METRICS", "true")
	cfg = Load(stellar.Config{})
	if !cfg.Observability.TopologyMetrics {
		t.Fatalf("expected topology metrics env override to enable metrics")
	}
}
