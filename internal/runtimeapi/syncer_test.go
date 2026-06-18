package runtimeapi

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stellhub/stellpulsar-service/internal/rulestore"
)

func TestSyncerBootstrapUsesCachedSnapshotWhenRuntimeFails(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "stellorbit unavailable", http.StatusServiceUnavailable)
	}))
	defer server.Close()

	store := rulestore.NewStore(syncerSnapshot(t, 3, "s3"))
	syncer := NewSyncer(NewClient(server.URL, server.Client()), store, nil, SyncerConfig{
		AllowEmptyStartup: true,
	})

	if err := syncer.Bootstrap(context.Background()); err != nil {
		t.Fatalf("Bootstrap() error = %v", err)
	}
	if snapshot := store.Snapshot(); snapshot.Version != 3 || snapshot.Checksum != "s3" {
		t.Fatalf("expected cached snapshot to be kept, got %#v", snapshot)
	}
}

func TestSyncerRetriesFullSnapshotWhenPagingVersionChanges(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != runtimePath+"/snapshot" {
			http.NotFound(w, r)
			return
		}
		call := calls.Add(1)
		page, _ := strconv.Atoi(r.URL.Query().Get("page"))
		switch call {
		case 1:
			writeSnapshotPage(w, 1, "s1", page, 2, "config-a", "rule-a")
		case 2:
			writeSnapshotPage(w, 2, "s2", page, 2, "config-b", "rule-b")
		case 3:
			writeSnapshotPage(w, 3, "s3", page, 2, "config-a", "rule-a")
		case 4:
			writeSnapshotPage(w, 3, "s3", page, 2, "config-b", "rule-b")
		default:
			t.Fatalf("unexpected snapshot request call %d", call)
		}
	}))
	defer server.Close()

	store := rulestore.NewEmptyStore(time.Now)
	syncer := NewSyncer(NewClient(server.URL, server.Client()), store, nil, SyncerConfig{
		SnapshotPageSize:   1,
		FullSyncMaxRetries: 2,
	})

	if err := syncer.Bootstrap(context.Background()); err != nil {
		t.Fatalf("Bootstrap() error = %v", err)
	}
	snapshot := store.Snapshot()
	if snapshot.Version != 3 || len(snapshot.Apps["order-service"].Rules) != 2 {
		t.Fatalf("unexpected snapshot after retry: %#v", snapshot)
	}
}

func writeSnapshotPage(w http.ResponseWriter, version int64, checksum string, page int, totalPages int, configID string, ruleID string) {
	w.Header().Set("Content-Type", "application/json")
	content := fmt.Sprintf(`{"rules":[{"ruleId":%q,"quota":1,"windowSeconds":1}]}`, ruleID)
	config := fmt.Sprintf(`{
		"applicationCode": "order-service",
		"configId": %q,
		"ruleName": %q,
		"content": %q,
		"checksum": %q,
		"ruleCount": 1
	}`, configID, ruleID, content, ruleID+"-checksum")
	_, _ = fmt.Fprintf(w, `{
		"snapshotVersion": %d,
		"checksum": %q,
		"generatedAt": "2026-06-18T00:00:00Z",
		"totalApplications": 1,
		"totalRules": 1,
		"configs": {
			"content": [%s],
			"page": %d,
			"size": 1,
			"totalElements": 2,
			"totalPages": %d
		}
	}`, version, checksum, config, page, totalPages)
}

func syncerSnapshot(t *testing.T, version int64, checksum string) rulestore.Snapshot {
	t.Helper()
	snapshot, err := rulestore.BuildSnapshot(version, checksum, time.Unix(10, 0), []rulestore.PublishedConfig{
		{
			ApplicationCode: "order-service",
			ConfigID:        "config-a",
			ContentChecksum: "rule-a-checksum",
			Content:         `{"rules":[{"ruleId":"rule-a","quota":1,"windowSeconds":1}]}`,
		},
	})
	if err != nil {
		t.Fatalf("BuildSnapshot() error = %v", err)
	}
	return snapshot
}
