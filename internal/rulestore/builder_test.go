package rulestore

import (
	"testing"
	"time"
)

func TestBuildSnapshotParsesPublishedDistributedRateLimitRules(t *testing.T) {
	snapshot, err := BuildSnapshot(7, "snapshot-checksum", time.Unix(100, 0), []PublishedConfig{
		{
			ApplicationCode:   "order-service",
			ConfigID:          "stellorbit.order-service",
			RuleName:          "order limit",
			ContentChecksum:   "content-checksum",
			AggregateChecksum: "aggregate-checksum",
			RuleCount:         1,
			Content: `{
				"schemaVersion": "stellorbit.governance.aggregate.v1",
				"rules": [
					{
						"ruleId": "orders-per-tenant",
						"name": "tenant order limit",
						"algorithm": "fixed_window",
						"quota": 2,
						"windowSeconds": 60,
						"burst": 1,
						"dimensions": ["tenant"],
						"failPolicy": "fail_closed",
						"attributes": {"tier": "gold"}
					}
				]
			}`,
		},
	})
	if err != nil {
		t.Fatalf("BuildSnapshot() error = %v", err)
	}

	rule := snapshot.Apps["order-service"].Rules["orders-per-tenant"]
	if rule.RuleID != "orders-per-tenant" {
		t.Fatalf("unexpected rule id: %s", rule.RuleID)
	}
	if rule.Quota != 2 || rule.WindowSeconds != 60 {
		t.Fatalf("unexpected quota/window: %d/%d", rule.Quota, rule.WindowSeconds)
	}
	if rule.Checksum != "content-checksum" {
		t.Fatalf("unexpected checksum: %s", rule.Checksum)
	}
	if rule.Attributes["tier"] != "gold" {
		t.Fatalf("unexpected attributes: %#v", rule.Attributes)
	}
}

func TestApplyChangesDeletesConfigRules(t *testing.T) {
	current, err := BuildSnapshot(1, "c1", time.Unix(1, 0), []PublishedConfig{
		{
			ApplicationCode: "order-service",
			ConfigID:        "config-a",
			ContentChecksum: "ra",
			Content:         `{"rules":[{"ruleId":"rule-a","quota":1,"windowSeconds":1}]}`,
		},
	})
	if err != nil {
		t.Fatalf("BuildSnapshot() error = %v", err)
	}

	next, err := ApplyChanges(current, 2, "c2", time.Unix(2, 0), []PublishedChange{
		{
			Operation:       "DELETE",
			ApplicationCode: "order-service",
			ConfigID:        "config-a",
		},
	})
	if err != nil {
		t.Fatalf("ApplyChanges() error = %v", err)
	}
	if _, ok := next.Apps["order-service"]; ok {
		t.Fatalf("expected order-service app to be deleted")
	}
}

func TestBuildSnapshotRejectsConfigWithoutRules(t *testing.T) {
	_, err := BuildSnapshot(1, "c1", time.Unix(1, 0), []PublishedConfig{
		{
			ApplicationCode: "order-service",
			ConfigID:        "config-a",
			RuleCount:       1,
			Content:         `{}`,
		},
	})
	if err == nil {
		t.Fatalf("expected config without rules to be rejected")
	}
}

func TestApplyChangesValidatesChecksums(t *testing.T) {
	current, err := BuildSnapshot(1, "c1", time.Unix(1, 0), []PublishedConfig{
		{
			ApplicationCode: "order-service",
			ConfigID:        "config-a",
			ContentChecksum: "ra",
			Content:         `{"rules":[{"ruleId":"rule-a","quota":1,"windowSeconds":1}]}`,
		},
	})
	if err != nil {
		t.Fatalf("BuildSnapshot() error = %v", err)
	}

	_, err = ApplyChanges(current, 2, "c2", time.Unix(2, 0), []PublishedChange{
		{
			Operation:        "DELETE",
			ApplicationCode:  "order-service",
			ConfigID:         "config-a",
			PreviousChecksum: "unexpected",
		},
	})
	if err == nil {
		t.Fatalf("expected previous checksum mismatch")
	}

	_, err = ApplyChanges(current, 2, "c2", time.Unix(2, 0), []PublishedChange{
		{
			Operation:       "UPDATE",
			ApplicationCode: "order-service",
			ConfigID:        "config-a",
			CurrentChecksum: "expected-current",
			Config: &PublishedConfig{
				ApplicationCode: "order-service",
				ConfigID:        "config-a",
				ContentChecksum: "actual-current",
				Content:         `{"rules":[{"ruleId":"rule-a","quota":1,"windowSeconds":1}]}`,
			},
		},
	})
	if err == nil {
		t.Fatalf("expected current checksum mismatch")
	}
}
