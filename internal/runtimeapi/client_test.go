package runtimeapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestClientReadsSnapshotAndChanges(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case runtimePath + "/snapshot":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{
				"snapshotVersion": 3,
				"checksum": "s3",
				"generatedAt": "2026-06-18T00:00:00Z",
				"totalApplications": 1,
				"totalRules": 1,
				"configs": {
					"content": [
						{
							"applicationCode": "order-service",
							"configId": "config-a",
							"ruleName": "rule",
							"content": "{\"rules\":[{\"ruleId\":\"rule-a\",\"quota\":1,\"windowSeconds\":1}]}",
							"checksum": "rule-a-checksum",
							"aggregateChecksum": "agg-a",
							"ruleCount": 1
						}
					],
					"page": 0,
					"size": 100,
					"totalElements": 1,
					"totalPages": 1
				}
			}`))
		case runtimePath + "/changes":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{
				"fromSnapshotVersion": 3,
				"toSnapshotVersion": 4,
				"fromChecksum": "s3",
				"toChecksum": "s4",
				"generatedAt": "2026-06-18T00:00:01Z",
				"changeCount": 1,
				"changes": [
					{
						"operation": "DELETE",
						"fromSnapshotVersion": 3,
						"toSnapshotVersion": 4,
						"applicationCode": "order-service",
						"configId": "config-a",
						"previousChecksum": "rule-a-checksum"
					}
				]
			}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client := NewClient(server.URL, server.Client())
	page, err := client.Snapshot(context.Background(), 0, 100)
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	if page.SnapshotVersion != 3 || len(page.Configs.Content) != 1 {
		t.Fatalf("unexpected snapshot page: %#v", page)
	}

	delta, err := client.Changes(context.Background(), 3, "s3")
	if err != nil {
		t.Fatalf("Changes() error = %v", err)
	}
	if delta.ToSnapshotVersion != 4 || len(delta.Changes) != 1 {
		t.Fatalf("unexpected delta: %#v", delta)
	}
}

func TestClientParsesWatchEvents(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != runtimePath+"/watch" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("event: heartbeat\n"))
		_, _ = w.Write([]byte(`data: {"eventId":"e1","eventType":"WATCH_HEARTBEAT","currentSnapshotVersion":1,"latestSnapshotVersion":1,"latestChecksum":"c1","generatedAt":"2026-06-18T00:00:00Z"}` + "\n\n"))
	}))
	defer server.Close()

	client := NewClient(server.URL, server.Client())
	var seen []WatchEvent
	if err := client.Watch(context.Background(), 1, "c1", func(event WatchEvent) error {
		seen = append(seen, event)
		return nil
	}); err != nil {
		t.Fatalf("Watch() error = %v", err)
	}
	if len(seen) != 1 || seen[0].EventType != "WATCH_HEARTBEAT" {
		t.Fatalf("unexpected watch events: %#v", seen)
	}
}
