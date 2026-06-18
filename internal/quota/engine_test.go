package quota

import (
	"context"
	"testing"
	"time"

	pb "github.com/stellhub/stellpulsar-service/api/stellpulsar/v1"
	"github.com/stellhub/stellpulsar-service/internal/rulestore"
)

func TestEngineAcquireQuota(t *testing.T) {
	now := time.Unix(100, 0)
	store := rulestore.NewStore(snapshotForQuota(t))
	engine := NewEngine(store, func() time.Time { return now }, Config{})

	request := &pb.AcquireQuotaRequest{
		RequestId:       "r1",
		ApplicationCode: "order-service",
		RuleId:          "rule-a",
		QuotaKey:        "tenant-a",
		Cost:            1,
		RuleRevision:    "3",
		RuleChecksum:    "rule-a-checksum",
	}

	first := engine.Acquire(context.Background(), request)
	if first.GetDecision() != pb.QuotaDecision_QUOTA_DECISION_ALLOWED || first.GetRemaining() != 1 {
		t.Fatalf("unexpected first decision: %#v", first)
	}
	second := engine.Acquire(context.Background(), request)
	if second.GetDecision() != pb.QuotaDecision_QUOTA_DECISION_ALLOWED || second.GetRemaining() != 0 {
		t.Fatalf("unexpected second decision: %#v", second)
	}
	third := engine.Acquire(context.Background(), request)
	if third.GetDecision() != pb.QuotaDecision_QUOTA_DECISION_DENIED {
		t.Fatalf("unexpected third decision: %#v", third)
	}
}

func TestEngineDetectsRuleVersionMismatch(t *testing.T) {
	store := rulestore.NewStore(snapshotForQuota(t))
	engine := NewEngine(store, nil, Config{})

	stale := engine.Acquire(context.Background(), &pb.AcquireQuotaRequest{
		ApplicationCode: "order-service",
		RuleId:          "rule-a",
		QuotaKey:        "tenant-a",
		Cost:            1,
		RuleRevision:    "2",
		RuleChecksum:    "rule-a-checksum",
	})
	if stale.GetDecision() != pb.QuotaDecision_QUOTA_DECISION_RULE_STALE {
		t.Fatalf("unexpected stale decision: %#v", stale)
	}

	lag := engine.Acquire(context.Background(), &pb.AcquireQuotaRequest{
		ApplicationCode: "order-service",
		RuleId:          "rule-a",
		QuotaKey:        "tenant-a",
		Cost:            1,
		RuleRevision:    "4",
		RuleChecksum:    "rule-a-checksum",
	})
	if lag.GetDecision() != pb.QuotaDecision_QUOTA_DECISION_SERVER_RULE_LAG {
		t.Fatalf("unexpected lag decision: %#v", lag)
	}
}

func TestEngineRejectsMissingRuleDigest(t *testing.T) {
	store := rulestore.NewStore(snapshotForQuota(t))
	engine := NewEngine(store, nil, Config{})

	response := engine.Acquire(context.Background(), &pb.AcquireQuotaRequest{
		ApplicationCode: "order-service",
		RuleId:          "rule-a",
		QuotaKey:        "tenant-a",
		Cost:            1,
	})
	if response.GetDecision() != pb.QuotaDecision_QUOTA_DECISION_INVALID_REQUEST {
		t.Fatalf("unexpected decision: %#v", response)
	}
	if response.GetReason() != "rule_revision_required" {
		t.Fatalf("unexpected reason: %s", response.GetReason())
	}
}

func TestEngineCleansExpiredBuckets(t *testing.T) {
	now := time.Unix(100, 0)
	store := rulestore.NewStore(snapshotForQuota(t))
	engine := NewEngine(store, func() time.Time { return now }, Config{
		BucketGCInterval:   time.Millisecond,
		ExpiredBucketGrace: time.Millisecond,
		MaxBuckets:         10,
	})

	request := &pb.AcquireQuotaRequest{
		ApplicationCode: "order-service",
		RuleId:          "rule-a",
		QuotaKey:        "tenant-a",
		Cost:            1,
		RuleRevision:    "3",
		RuleChecksum:    "rule-a-checksum",
	}
	if engine.Acquire(context.Background(), request).GetDecision() != pb.QuotaDecision_QUOTA_DECISION_ALLOWED {
		t.Fatalf("expected first request to be allowed")
	}
	now = now.Add(2 * time.Minute)
	request.QuotaKey = "tenant-b"
	if engine.Acquire(context.Background(), request).GetDecision() != pb.QuotaDecision_QUOTA_DECISION_ALLOWED {
		t.Fatalf("expected second request to be allowed")
	}
	if len(engine.buckets) != 1 {
		t.Fatalf("expected expired bucket to be cleaned, got %d buckets", len(engine.buckets))
	}
}

func snapshotForQuota(t *testing.T) rulestore.Snapshot {
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
