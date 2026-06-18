package topology

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/stellhub/stellpulsar-service/internal/registry"
)

const (
	HashAlgorithmRendezvousV1 = "rendezvous_hash_v1"
	defaultCacheTTL           = 2 * time.Second
)

type Provider interface {
	List(context.Context) ([]registry.Instance, error)
}

type Config struct {
	Namespace      string
	Service        string
	SelfInstanceID string
	CacheTTL       time.Duration
	Metrics        *Metrics
}

type Manager struct {
	provider Provider
	config   Config
	now      func() time.Time

	mu        sync.RWMutex
	current   Topology
	expiresAt time.Time
	updatedAt time.Time
	metrics   *Metrics
}

type Topology struct {
	Namespace     string
	Service       string
	Revision      string
	HashAlgorithm string
	Instances     []registry.Instance
}

func NewManager(provider Provider, config Config) *Manager {
	if config.CacheTTL <= 0 {
		config.CacheTTL = defaultCacheTTL
	}
	return &Manager{
		provider: provider,
		config:   config,
		now:      time.Now,
		metrics:  config.Metrics,
	}
}

func (m *Manager) Current(ctx context.Context) (Topology, error) {
	if m == nil {
		return Topology{}, fmt.Errorf("topology manager is nil")
	}
	now := m.now()
	m.mu.RLock()
	if m.current.Revision != "" && now.Before(m.expiresAt) {
		current := m.current.Clone()
		updatedAt := m.updatedAt
		m.mu.RUnlock()
		m.metrics.RecordCacheAccess(ctx, "hit", ageSince(now, updatedAt), false)
		return current, nil
	}
	result := "empty"
	updatedAt := m.updatedAt
	if m.current.Revision != "" {
		result = "expired"
	}
	m.mu.RUnlock()
	m.metrics.RecordCacheAccess(ctx, result, ageSince(now, updatedAt), result == "expired")
	return m.Refresh(ctx)
}

func (m *Manager) Refresh(ctx context.Context) (Topology, error) {
	if m == nil || m.provider == nil {
		return Topology{}, fmt.Errorf("topology provider is required")
	}
	start := m.now()
	instances, err := m.provider.List(ctx)
	if err != nil {
		m.mu.RLock()
		if m.current.Revision != "" {
			current := m.current.Clone()
			updatedAt := m.updatedAt
			m.mu.RUnlock()
			now := m.now()
			m.metrics.RecordRefresh(ctx, "stale_cache", now.Sub(start), current, ageSince(now, updatedAt), true)
			return current, nil
		}
		m.mu.RUnlock()
		m.metrics.RecordRefresh(ctx, "error", m.now().Sub(start), Topology{}, 0, true)
		return Topology{}, err
	}
	topology := Build(m.config.Namespace, m.config.Service, instances)

	m.mu.Lock()
	m.current = topology.Clone()
	now := m.now()
	m.updatedAt = now
	m.expiresAt = now.Add(m.config.CacheTTL)
	m.mu.Unlock()
	m.metrics.RecordRefresh(ctx, "success", now.Sub(start), topology, 0, false)
	return topology, nil
}

func (m *Manager) OwnerOf(ctx context.Context, shardKey string) (registry.Instance, Topology, bool, error) {
	if m == nil {
		return registry.Instance{}, Topology{}, false, fmt.Errorf("topology manager is nil")
	}
	start := time.Now()
	current, err := m.Current(ctx)
	if err != nil {
		m.metrics.RecordOwnerLookup(ctx, "error", time.Since(start), false)
		return registry.Instance{}, Topology{}, false, err
	}
	owner, ok := current.OwnerOf(shardKey)
	if !ok {
		m.metrics.RecordOwnerLookup(ctx, "owner_unavailable", time.Since(start), false)
		return owner, current, ok, nil
	}
	m.metrics.RecordOwnerLookup(ctx, "success", time.Since(start), owner.InstanceID == m.SelfInstanceID())
	return owner, current, ok, nil
}

func (m *Manager) RecordOwnerCheck(ctx context.Context, result string, reason string) {
	if m == nil {
		return
	}
	m.metrics.RecordOwnerCheck(ctx, result, reason)
}

func ageSince(now, updatedAt time.Time) time.Duration {
	if updatedAt.IsZero() {
		return 0
	}
	return now.Sub(updatedAt)
}

func (m *Manager) SelfInstanceID() string {
	if m == nil {
		return ""
	}
	return strings.TrimSpace(m.config.SelfInstanceID)
}

func Build(namespace, service string, instances []registry.Instance) Topology {
	filtered := make([]registry.Instance, 0, len(instances))
	for _, instance := range instances {
		normalized, ok := normalizeInstance(instance)
		if !ok {
			continue
		}
		filtered = append(filtered, normalized)
	}
	sortInstances(filtered)
	revision := revision(namespace, service, filtered)
	return Topology{
		Namespace:     strings.TrimSpace(namespace),
		Service:       strings.TrimSpace(service),
		Revision:      revision,
		HashAlgorithm: HashAlgorithmRendezvousV1,
		Instances:     cloneInstances(filtered),
	}
}

func (t Topology) Clone() Topology {
	t.Instances = cloneInstances(t.Instances)
	return t
}

func (t Topology) OwnerOf(shardKey string) (registry.Instance, bool) {
	shardKey = strings.TrimSpace(shardKey)
	if shardKey == "" || t.Revision == "" {
		return registry.Instance{}, false
	}
	var owner registry.Instance
	bestScore := -1.0
	for _, instance := range t.Instances {
		if instance.State != "UP" {
			continue
		}
		score := weightedScore(t.Revision, shardKey, instance)
		if score > bestScore || (score == bestScore && instance.InstanceID < owner.InstanceID) {
			bestScore = score
			owner = instance
		}
	}
	if bestScore < 0 {
		return registry.Instance{}, false
	}
	return cloneInstance(owner), true
}

func ShardKey(applicationCode, ruleID, quotaKey string) string {
	return strings.TrimSpace(applicationCode) + ":" + strings.TrimSpace(ruleID) + ":" + strings.TrimSpace(quotaKey)
}

func ShardHash(revision, shardKey string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(revision) + "\n" + strings.TrimSpace(shardKey)))
	return hex.EncodeToString(sum[:])[:16]
}

func normalizeInstance(instance registry.Instance) (registry.Instance, bool) {
	instance.InstanceID = strings.TrimSpace(instance.InstanceID)
	instance.Host = strings.TrimSpace(instance.Host)
	instance.Zone = strings.TrimSpace(instance.Zone)
	instance.Version = strings.TrimSpace(instance.Version)
	instance.State = normalizeState(instance.State)
	if instance.InstanceID == "" || instance.Host == "" || instance.Port <= 0 {
		return registry.Instance{}, false
	}
	switch instance.State {
	case "UP", "DRAINING", "MIGRATING":
	default:
		return registry.Instance{}, false
	}
	if instance.Priority == 0 {
		instance.Priority = 100
	}
	if instance.Weight <= 0 {
		instance.Weight = 100
	}
	instance.Metadata = cloneMap(instance.Metadata)
	return instance, true
}

func normalizeState(state string) string {
	state = strings.ToUpper(strings.TrimSpace(state))
	if state == "" {
		return "UP"
	}
	return state
}

func revision(namespace, service string, instances []registry.Instance) string {
	var builder strings.Builder
	builder.WriteString(strings.TrimSpace(namespace))
	builder.WriteString("/")
	builder.WriteString(strings.TrimSpace(service))
	builder.WriteByte('\n')
	for _, instance := range instances {
		_, _ = fmt.Fprintf(
			&builder,
			"%s|%s|%d|%d|%s|%s|%d|%s\n",
			instance.InstanceID,
			instance.State,
			instance.Priority,
			instance.Weight,
			instance.Zone,
			instance.Host,
			instance.Port,
			instance.Version,
		)
	}
	sum := sha256.Sum256([]byte(builder.String()))
	return hex.EncodeToString(sum[:])[:16]
}

func weightedScore(revision, shardKey string, instance registry.Instance) float64 {
	sum := sha256.Sum256([]byte(strings.TrimSpace(revision) + "\n" + strings.TrimSpace(shardKey) + "\n" + instance.InstanceID))
	value := binary.BigEndian.Uint64(sum[:8])
	const uint64Range = 18446744073709551616.0
	unit := (float64(value) + 1) / uint64Range
	if unit >= 1 {
		unit = math.Nextafter(1, 0)
	}
	weight := float64(instance.Weight)
	if weight <= 0 {
		weight = 100
	}
	return weight / -math.Log(unit)
}

func sortInstances(instances []registry.Instance) {
	sort.SliceStable(instances, func(i, j int) bool {
		left := instances[i]
		right := instances[j]
		if left.Priority != right.Priority {
			return left.Priority < right.Priority
		}
		if left.Weight != right.Weight {
			return left.Weight > right.Weight
		}
		if left.InstanceID != right.InstanceID {
			return left.InstanceID < right.InstanceID
		}
		return fmt.Sprintf("%s:%d", left.Host, left.Port) < fmt.Sprintf("%s:%d", right.Host, right.Port)
	})
}

func cloneInstances(instances []registry.Instance) []registry.Instance {
	cloned := make([]registry.Instance, 0, len(instances))
	for _, instance := range instances {
		cloned = append(cloned, cloneInstance(instance))
	}
	return cloned
}

func cloneInstance(instance registry.Instance) registry.Instance {
	instance.Metadata = cloneMap(instance.Metadata)
	return instance
}

func cloneMap(values map[string]string) map[string]string {
	if values == nil {
		return map[string]string{}
	}
	cloned := make(map[string]string, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}
