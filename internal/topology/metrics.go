package topology

import (
	"context"
	"errors"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type MetricsConfig struct {
	Namespace  string
	Service    string
	InstanceID string
}

type Metrics struct {
	baseAttributes []attribute.KeyValue

	refreshCount       metric.Int64Counter
	refreshDuration    metric.Float64Histogram
	cacheAccessCount   metric.Int64Counter
	cacheAge           metric.Float64Gauge
	cacheStale         metric.Int64Gauge
	instanceCount      metric.Int64Gauge
	ownerLookupCount   metric.Int64Counter
	ownerLookupLatency metric.Float64Histogram
	ownerCheckCount    metric.Int64Counter
}

func NewMetrics(meter metric.Meter, config MetricsConfig) (*Metrics, error) {
	if meter == nil {
		return nil, errors.New("topology metrics meter is required")
	}
	refreshCount, err := meter.Int64Counter(
		"stellpulsar.topology.refresh.count",
		metric.WithDescription("Number of topology refresh operations."),
		metric.WithUnit("{operation}"),
	)
	if err != nil {
		return nil, err
	}
	refreshDuration, err := meter.Float64Histogram(
		"stellpulsar.topology.refresh.duration",
		metric.WithDescription("Duration of topology refresh operations."),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, err
	}
	cacheAccessCount, err := meter.Int64Counter(
		"stellpulsar.topology.cache.access.count",
		metric.WithDescription("Number of topology cache access operations."),
		metric.WithUnit("{operation}"),
	)
	if err != nil {
		return nil, err
	}
	cacheAge, err := meter.Float64Gauge(
		"stellpulsar.topology.cache.age",
		metric.WithDescription("Age of the current topology cache snapshot."),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, err
	}
	cacheStale, err := meter.Int64Gauge(
		"stellpulsar.topology.cache.stale",
		metric.WithDescription("Whether the current topology cache is stale."),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, err
	}
	instanceCount, err := meter.Int64Gauge(
		"stellpulsar.topology.instance.count",
		metric.WithDescription("Number of instances in the current topology by state."),
		metric.WithUnit("{instance}"),
	)
	if err != nil {
		return nil, err
	}
	ownerLookupCount, err := meter.Int64Counter(
		"stellpulsar.topology.owner.lookup.count",
		metric.WithDescription("Number of topology owner lookup operations."),
		metric.WithUnit("{operation}"),
	)
	if err != nil {
		return nil, err
	}
	ownerLookupLatency, err := meter.Float64Histogram(
		"stellpulsar.topology.owner.lookup.duration",
		metric.WithDescription("Duration of topology owner lookup operations."),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, err
	}
	ownerCheckCount, err := meter.Int64Counter(
		"stellpulsar.topology.owner.check.count",
		metric.WithDescription("Number of server-side owner validation decisions."),
		metric.WithUnit("{operation}"),
	)
	if err != nil {
		return nil, err
	}
	return &Metrics{
		baseAttributes: []attribute.KeyValue{
			attribute.String("stellpulsar.namespace", strings.TrimSpace(config.Namespace)),
			attribute.String("stellpulsar.service", strings.TrimSpace(config.Service)),
			attribute.String("stellpulsar.instance_id", strings.TrimSpace(config.InstanceID)),
		},
		refreshCount:       refreshCount,
		refreshDuration:    refreshDuration,
		cacheAccessCount:   cacheAccessCount,
		cacheAge:           cacheAge,
		cacheStale:         cacheStale,
		instanceCount:      instanceCount,
		ownerLookupCount:   ownerLookupCount,
		ownerLookupLatency: ownerLookupLatency,
		ownerCheckCount:    ownerCheckCount,
	}, nil
}

func (m *Metrics) RecordCacheAccess(ctx context.Context, result string, age time.Duration, stale bool) {
	if m == nil {
		return
	}
	attrs := m.attributes(attribute.String("result", normalizeAttribute(result)))
	m.cacheAccessCount.Add(ctx, 1, metric.WithAttributes(attrs...))
	m.recordCacheState(ctx, age, stale)
}

func (m *Metrics) RecordRefresh(ctx context.Context, result string, duration time.Duration, topology Topology, age time.Duration, stale bool) {
	if m == nil {
		return
	}
	attrs := m.attributes(attribute.String("result", normalizeAttribute(result)))
	m.refreshCount.Add(ctx, 1, metric.WithAttributes(attrs...))
	m.refreshDuration.Record(ctx, duration.Seconds(), metric.WithAttributes(attrs...))
	m.recordCacheState(ctx, age, stale)
	if topology.Revision != "" {
		m.recordInstances(ctx, topology)
	}
}

func (m *Metrics) RecordOwnerLookup(ctx context.Context, result string, duration time.Duration, localOwner bool) {
	if m == nil {
		return
	}
	attrs := m.attributes(
		attribute.String("result", normalizeAttribute(result)),
		attribute.Bool("local_owner", localOwner),
	)
	m.ownerLookupCount.Add(ctx, 1, metric.WithAttributes(attrs...))
	m.ownerLookupLatency.Record(ctx, duration.Seconds(), metric.WithAttributes(attrs...))
}

func (m *Metrics) RecordOwnerCheck(ctx context.Context, result string, reason string) {
	if m == nil {
		return
	}
	attrs := m.attributes(
		attribute.String("result", normalizeAttribute(result)),
		attribute.String("reason", normalizeAttribute(reason)),
	)
	m.ownerCheckCount.Add(ctx, 1, metric.WithAttributes(attrs...))
}

func (m *Metrics) recordCacheState(ctx context.Context, age time.Duration, stale bool) {
	if age < 0 {
		age = 0
	}
	staleValue := int64(0)
	if stale {
		staleValue = 1
	}
	m.cacheAge.Record(ctx, age.Seconds(), metric.WithAttributes(m.baseAttributes...))
	m.cacheStale.Record(ctx, staleValue, metric.WithAttributes(m.baseAttributes...))
}

func (m *Metrics) recordInstances(ctx context.Context, topology Topology) {
	counts := map[string]int64{
		"TOTAL":     int64(len(topology.Instances)),
		"UP":        0,
		"DRAINING":  0,
		"MIGRATING": 0,
	}
	for _, instance := range topology.Instances {
		state := normalizeAttribute(instance.State)
		if state == "" {
			state = "UP"
		}
		counts[state]++
	}
	for state, count := range counts {
		attrs := m.attributes(attribute.String("state", state))
		m.instanceCount.Record(ctx, count, metric.WithAttributes(attrs...))
	}
}

func (m *Metrics) attributes(extra ...attribute.KeyValue) []attribute.KeyValue {
	attrs := make([]attribute.KeyValue, 0, len(m.baseAttributes)+len(extra))
	attrs = append(attrs, m.baseAttributes...)
	attrs = append(attrs, extra...)
	return attrs
}

func normalizeAttribute(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "unknown"
	}
	return value
}
