package registry

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"sync"

	stellarregistry "github.com/stellhub/stellar/registry"
	"github.com/stellhub/stellpulsar-service/internal/config"
	"github.com/stellhub/stellpulsar-service/internal/rulestore"
)

type Instance struct {
	InstanceID   string
	Host         string
	Port         int32
	Priority     int32
	Weight       int32
	Zone         string
	Version      string
	RuleRevision string
	State        string
	Metadata     map[string]string
}

type Provider interface {
	Start(ctx context.Context, snapshot rulestore.Snapshot) error
	Stop(ctx context.Context) error
	List(ctx context.Context) ([]Instance, error)
	Update(ctx context.Context, snapshot rulestore.Snapshot) error
}

type ProviderConfig struct {
	Registry *stellarregistry.Registry
	Config   config.Config
}

func NewProvider(cfg ProviderConfig) Provider {
	local := LocalProvider{
		instance: Instance{
			InstanceID: cfg.Config.Instance.ID,
			Host:       cfg.Config.Instance.Host,
			Port:       cfg.Config.Instance.Port,
			Priority:   cfg.Config.Instance.Priority,
			Weight:     cfg.Config.Instance.Weight,
			Zone:       cfg.Config.Instance.Zone,
			Version:    cfg.Config.Instance.Version,
			State:      "UP",
			Metadata: map[string]string{
				"source": "local",
			},
		},
	}
	if cfg.Registry == nil {
		return &local
	}
	return &StellarProvider{
		registry: cfg.Registry,
		cfg:      cfg.Config,
		local:    local,
	}
}

type LocalProvider struct {
	mu       sync.RWMutex
	instance Instance
}

func (p *LocalProvider) Start(ctx context.Context, snapshot rulestore.Snapshot) error {
	return p.Update(ctx, snapshot)
}

func (p *LocalProvider) Update(_ context.Context, snapshot rulestore.Snapshot) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.instance.Metadata == nil {
		p.instance.Metadata = map[string]string{}
	}
	p.instance.RuleRevision = strconv.FormatInt(snapshot.Version, 10)
	p.instance.Metadata["rule_revision"] = p.instance.RuleRevision
	p.instance.Metadata["rule_checksum"] = snapshot.Checksum
	return nil
}

func (p *LocalProvider) Stop(context.Context) error {
	return nil
}

func (p *LocalProvider) List(context.Context) ([]Instance, error) {
	return []Instance{p.currentInstance()}, nil
}

func (p *LocalProvider) currentInstance() Instance {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return cloneInstance(p.instance)
}

type StellarProvider struct {
	registry *stellarregistry.Registry
	cfg      config.Config
	local    LocalProvider
}

func (p *StellarProvider) Start(ctx context.Context, snapshot rulestore.Snapshot) error {
	return p.local.Start(ctx, snapshot)
}

func (p *StellarProvider) Update(ctx context.Context, snapshot rulestore.Snapshot) error {
	return p.local.Update(ctx, snapshot)
}

func (p *StellarProvider) Stop(context.Context) error {
	return nil
}

func (p *StellarProvider) List(ctx context.Context) ([]Instance, error) {
	items, err := p.registry.Discover(ctx, stellarregistry.Query{
		Namespace:   p.cfg.Instance.Namespace,
		Service:     p.cfg.Instance.Service,
		PassingOnly: true,
	})
	if err != nil {
		return nil, err
	}
	instances := make([]Instance, 0, len(items))
	local := p.local.currentInstance()
	for _, item := range items {
		instance := fromStellar(item)
		if instance.InstanceID == local.InstanceID {
			instance.RuleRevision = local.RuleRevision
			instance.Priority = local.Priority
			instance.Weight = local.Weight
			instance.Metadata = mergeMap(instance.Metadata, local.Metadata)
		}
		instances = append(instances, instance)
	}
	sortInstances(instances)
	return instances, nil
}

func fromStellar(item stellarregistry.Instance) Instance {
	host, port := endpoint(item.Endpoints)
	priority := int32Value(item.Metadata, "priority", 100)
	weight := int32Value(item.Metadata, "weight", endpointWeight(item.Endpoints, 100))
	return Instance{
		InstanceID:   item.InstanceID,
		Host:         host,
		Port:         int32(port),
		Priority:     priority,
		Weight:       weight,
		Zone:         item.Zone,
		Version:      item.Metadata["version"],
		RuleRevision: item.Metadata["rule_revision"],
		State:        "UP",
		Metadata:     cloneMap(item.Metadata),
	}
}

func endpoint(endpoints []stellarregistry.Endpoint) (string, int) {
	for _, candidate := range endpoints {
		if candidate.Protocol == "grpc" || candidate.Name == "grpc" {
			return candidate.Host, candidate.Port
		}
	}
	if len(endpoints) == 0 {
		return "", 0
	}
	return endpoints[0].Host, endpoints[0].Port
}

func endpointWeight(endpoints []stellarregistry.Endpoint, fallback int32) int32 {
	for _, candidate := range endpoints {
		if candidate.Weight > 0 {
			return int32(candidate.Weight)
		}
	}
	return fallback
}

func int32Value(values map[string]string, key string, fallback int32) int32 {
	value, ok := values[key]
	if !ok {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return int32(parsed)
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

func cloneInstance(value Instance) Instance {
	value.Metadata = cloneMap(value.Metadata)
	return value
}

func mergeMap(base map[string]string, override map[string]string) map[string]string {
	merged := cloneMap(base)
	for key, value := range override {
		merged[key] = value
	}
	return merged
}

func sortInstances(instances []Instance) {
	sort.SliceStable(instances, func(i, j int) bool {
		left := instances[i]
		right := instances[j]
		if left.Priority != right.Priority {
			return left.Priority < right.Priority
		}
		if left.Weight != right.Weight {
			return left.Weight > right.Weight
		}
		return fmt.Sprintf("%s:%d", left.Host, left.Port) < fmt.Sprintf("%s:%d", right.Host, right.Port)
	})
}
