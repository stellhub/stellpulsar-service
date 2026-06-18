package topology

import (
	"context"
	"testing"

	"github.com/stellhub/stellpulsar-service/internal/registry"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestManagerRecordsTopologyMetrics(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	meterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	metrics, err := NewMetrics(meterProvider.Meter("stellpulsar-test"), MetricsConfig{
		Namespace:  "default",
		Service:    "stellpulsar-service",
		InstanceID: "pulsar-a",
	})
	if err != nil {
		t.Fatalf("NewMetrics() error = %v", err)
	}

	manager := NewManager(staticProvider{instances: []registry.Instance{
		{InstanceID: "pulsar-a", Host: "127.0.0.1", Port: 9090, Priority: 10, Weight: 100, State: "UP", Version: "test"},
		{InstanceID: "pulsar-b", Host: "127.0.0.2", Port: 9090, Priority: 10, Weight: 100, State: "DRAINING", Version: "test"},
	}}, Config{
		Namespace:      "default",
		Service:        "stellpulsar-service",
		SelfInstanceID: "pulsar-a",
		Metrics:        metrics,
	})

	current, err := manager.Current(context.Background())
	if err != nil {
		t.Fatalf("Current() error = %v", err)
	}
	_, _, _, err = manager.OwnerOf(context.Background(), ShardKey("order-service", "rule-a", "tenant-a"))
	if err != nil {
		t.Fatalf("OwnerOf() error = %v", err)
	}
	manager.RecordOwnerCheck(context.Background(), "matched", "owner_matched")

	collected := collectMetrics(t, reader)
	assertMetricExists(t, collected, "stellpulsar.topology.refresh.count")
	assertMetricExists(t, collected, "stellpulsar.topology.refresh.duration")
	assertMetricExists(t, collected, "stellpulsar.topology.cache.access.count")
	assertMetricExists(t, collected, "stellpulsar.topology.cache.age")
	assertMetricExists(t, collected, "stellpulsar.topology.cache.stale")
	assertMetricExists(t, collected, "stellpulsar.topology.instance.count")
	assertMetricExists(t, collected, "stellpulsar.topology.owner.lookup.count")
	assertMetricExists(t, collected, "stellpulsar.topology.owner.lookup.duration")
	assertMetricExists(t, collected, "stellpulsar.topology.owner.check.count")
	if current.Revision == "" {
		t.Fatalf("expected topology revision")
	}
}

type staticProvider struct {
	instances []registry.Instance
	err       error
}

func (p staticProvider) List(context.Context) ([]registry.Instance, error) {
	if p.err != nil {
		return nil, p.err
	}
	return p.instances, nil
}

func collectMetrics(t *testing.T, reader *sdkmetric.ManualReader) metricdata.ResourceMetrics {
	t.Helper()
	var metrics metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &metrics); err != nil {
		t.Fatalf("collect metrics: %v", err)
	}
	return metrics
}

func assertMetricExists(t *testing.T, metrics metricdata.ResourceMetrics, name string) {
	t.Helper()
	for _, scopeMetrics := range metrics.ScopeMetrics {
		for _, item := range scopeMetrics.Metrics {
			if item.Name == name {
				return
			}
		}
	}
	t.Fatalf("expected metric %q", name)
}
