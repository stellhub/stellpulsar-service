package topology

import (
	"testing"

	"github.com/stellhub/stellpulsar-service/internal/registry"
)

func TestBuildCreatesStableRevisionAndFiltersInstances(t *testing.T) {
	left := []registry.Instance{
		{InstanceID: "pulsar-b", Host: "127.0.0.2", Port: 9090, Priority: 10, Weight: 100, State: "UP", Version: "v1"},
		{InstanceID: "pulsar-down", Host: "127.0.0.3", Port: 9090, Priority: 10, Weight: 100, State: "DOWN", Version: "v1"},
		{InstanceID: "pulsar-a", Host: "127.0.0.1", Port: 9090, Priority: 10, Weight: 100, State: "UP", Version: "v1"},
	}
	right := []registry.Instance{
		{InstanceID: "pulsar-a", Host: "127.0.0.1", Port: 9090, Priority: 10, Weight: 100, State: "UP", Version: "v1"},
		{InstanceID: "pulsar-b", Host: "127.0.0.2", Port: 9090, Priority: 10, Weight: 100, State: "UP", Version: "v1"},
		{InstanceID: "invalid", Host: "", Port: 0, Priority: 10, Weight: 100, State: "UP", Version: "v1"},
	}

	first := Build("default", "stellpulsar-service", left)
	second := Build("default", "stellpulsar-service", right)
	if first.Revision == "" || first.Revision != second.Revision {
		t.Fatalf("expected stable revision, got %q and %q", first.Revision, second.Revision)
	}
	if len(first.Instances) != 2 {
		t.Fatalf("expected filtered topology to contain two instances, got %#v", first.Instances)
	}
	if first.HashAlgorithm != HashAlgorithmRendezvousV1 {
		t.Fatalf("unexpected hash algorithm: %s", first.HashAlgorithm)
	}
}

func TestTopologyOwnerOfIsDeterministic(t *testing.T) {
	current := Build("default", "stellpulsar-service", []registry.Instance{
		{InstanceID: "pulsar-a", Host: "127.0.0.1", Port: 9090, Priority: 10, Weight: 100, State: "UP", Version: "v1"},
		{InstanceID: "pulsar-b", Host: "127.0.0.2", Port: 9090, Priority: 10, Weight: 100, State: "UP", Version: "v1"},
	})
	first, ok := current.OwnerOf(ShardKey("order-service", "rule-a", "tenant-a"))
	if !ok {
		t.Fatalf("expected owner")
	}
	second, ok := current.OwnerOf(ShardKey("order-service", "rule-a", "tenant-a"))
	if !ok {
		t.Fatalf("expected owner on second lookup")
	}
	if first.InstanceID != second.InstanceID {
		t.Fatalf("expected deterministic owner, got %s and %s", first.InstanceID, second.InstanceID)
	}
}
