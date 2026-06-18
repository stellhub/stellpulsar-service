package grpcapi

import (
	"context"
	"fmt"
	"testing"
	"time"

	pb "github.com/stellhub/stellpulsar-service/api/stellpulsar/v1"
	"github.com/stellhub/stellpulsar-service/internal/quota"
	"github.com/stellhub/stellpulsar-service/internal/registry"
	"github.com/stellhub/stellpulsar-service/internal/rulestore"
	"github.com/stellhub/stellpulsar-service/internal/topology"
)

func TestServerListInstancesAndGetRuleSnapshot(t *testing.T) {
	store := rulestore.NewStore(testSnapshot(t))
	provider := &fakeProvider{
		instances: []registry.Instance{
			{
				InstanceID:   "pulsar-a",
				Host:         "127.0.0.1",
				Port:         9090,
				Priority:     10,
				Weight:       100,
				Version:      "test",
				RuleRevision: "3",
				State:        "UP",
				Metadata:     map[string]string{"rule_revision": "3"},
			},
		},
	}
	server := NewServer(store, provider, quota.NewEngine(store, nil, quota.Config{}), Config{InstanceID: "pulsar-a"})

	instances, err := server.ListInstances(context.Background(), &pb.ListInstancesRequest{})
	if err != nil {
		t.Fatalf("ListInstances() error = %v", err)
	}
	if len(instances.GetInstances()) != 1 || instances.GetInstances()[0].GetInstanceId() != "pulsar-a" {
		t.Fatalf("unexpected instances: %#v", instances)
	}
	if instances.GetInstanceRevision() == "" || instances.GetHashAlgorithm() != topology.HashAlgorithmRendezvousV1 {
		t.Fatalf("expected topology metadata in list response, got %#v", instances)
	}

	snapshot, err := server.GetRuleSnapshot(context.Background(), &pb.GetRuleSnapshotRequest{
		ApplicationCode: "order-service",
	})
	if err != nil {
		t.Fatalf("GetRuleSnapshot() error = %v", err)
	}
	if snapshot.GetRevision() != "3" || len(snapshot.GetRules()) != 1 {
		t.Fatalf("unexpected snapshot: %#v", snapshot)
	}
}

type fakeProvider struct {
	instances []registry.Instance
}

func (p *fakeProvider) Start(context.Context, rulestore.Snapshot) error {
	return nil
}

func (p *fakeProvider) Stop(context.Context) error {
	return nil
}

func (p *fakeProvider) List(context.Context) ([]registry.Instance, error) {
	return p.instances, nil
}

func (p *fakeProvider) Update(context.Context, rulestore.Snapshot) error {
	return nil
}

func TestServerChangedDigestsIncludesRulesMissingOnClient(t *testing.T) {
	store := rulestore.NewStore(testSnapshot(t))
	server := NewServer(store, &fakeProvider{}, quota.NewEngine(store, nil, quota.Config{}), Config{InstanceID: "pulsar-a"})

	changed := server.changedDigests("order-service", nil)
	if len(changed) != 1 || changed[0].GetRuleId() != "rule-a" {
		t.Fatalf("expected server rule to be reported as changed, got %#v", changed)
	}
}

func TestServerAcquireQuotaReturnsNotOwner(t *testing.T) {
	store := rulestore.NewStore(testSnapshot(t))
	instances := []registry.Instance{
		{InstanceID: "pulsar-a", Host: "127.0.0.1", Port: 9090, Priority: 10, Weight: 100, State: "UP", Version: "test"},
		{InstanceID: "pulsar-b", Host: "127.0.0.2", Port: 9090, Priority: 10, Weight: 100, State: "UP", Version: "test"},
	}
	provider := &fakeProvider{instances: instances}
	server := NewServer(store, provider, quota.NewEngine(store, nil, quota.Config{}), Config{
		InstanceID: "pulsar-a",
		Namespace:  "default",
		Service:    "stellpulsar-service",
	})
	current := topology.Build("default", "stellpulsar-service", instances)
	quotaKey := quotaKeyOwnedBy(t, current, "pulsar-b")

	response, err := server.AcquireQuota(context.Background(), &pb.AcquireQuotaRequest{
		RequestId:        "r1",
		ApplicationCode:  "order-service",
		RuleId:           "rule-a",
		QuotaKey:         quotaKey,
		Cost:             1,
		RuleRevision:     "3",
		RuleChecksum:     "rule-a-checksum",
		TopologyRevision: current.Revision,
		TargetInstanceId: "pulsar-b",
	})
	if err != nil {
		t.Fatalf("AcquireQuota() error = %v", err)
	}
	if response.GetDecision() != pb.QuotaDecision_QUOTA_DECISION_NOT_OWNER {
		t.Fatalf("expected NOT_OWNER, got %#v", response)
	}
	if response.GetOwnerInstance().GetInstanceId() != "pulsar-b" {
		t.Fatalf("unexpected owner instance: %#v", response.GetOwnerInstance())
	}
	if response.GetTopologyRevision() != current.Revision {
		t.Fatalf("unexpected topology revision: %s", response.GetTopologyRevision())
	}
}

func TestServerAcquireQuotaRequiresTopologyFields(t *testing.T) {
	store := rulestore.NewStore(testSnapshot(t))
	provider := &fakeProvider{instances: []registry.Instance{
		{InstanceID: "pulsar-a", Host: "127.0.0.1", Port: 9090, Priority: 10, Weight: 100, State: "UP", Version: "test"},
	}}
	server := NewServer(store, provider, quota.NewEngine(store, nil, quota.Config{}), Config{InstanceID: "pulsar-a"})

	response, err := server.AcquireQuota(context.Background(), &pb.AcquireQuotaRequest{
		RequestId:       "r1",
		ApplicationCode: "order-service",
		RuleId:          "rule-a",
		QuotaKey:        "tenant-a",
		Cost:            1,
		RuleRevision:    "3",
		RuleChecksum:    "rule-a-checksum",
	})
	if err != nil {
		t.Fatalf("AcquireQuota() error = %v", err)
	}
	if response.GetDecision() != pb.QuotaDecision_QUOTA_DECISION_INVALID_REQUEST || response.GetReason() != "topology_revision_required" {
		t.Fatalf("unexpected response: %#v", response)
	}
}

func quotaKeyOwnedBy(t *testing.T, current topology.Topology, instanceID string) string {
	t.Helper()
	for index := 0; index < 1000; index++ {
		quotaKey := fmt.Sprintf("tenant-%d", index)
		owner, ok := current.OwnerOf(topology.ShardKey("order-service", "rule-a", quotaKey))
		if ok && owner.InstanceID == instanceID {
			return quotaKey
		}
	}
	t.Fatalf("could not find quota key owned by %s", instanceID)
	return ""
}

func testSnapshot(t *testing.T) rulestore.Snapshot {
	t.Helper()
	snapshot, err := rulestore.BuildSnapshot(3, "s3", time.Unix(10, 0), []rulestore.PublishedConfig{
		{
			ApplicationCode: "order-service",
			ConfigID:        "config-a",
			ContentChecksum: "rule-a-checksum",
			Content:         `{"rules":[{"ruleId":"rule-a","quota":2,"windowSeconds":60}]}`,
		},
	})
	if err != nil {
		t.Fatalf("BuildSnapshot() error = %v", err)
	}
	return snapshot
}
