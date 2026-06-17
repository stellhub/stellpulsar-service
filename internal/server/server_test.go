package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stellhub/stellpulsar-service/internal/limiter"
)

func TestHealth(t *testing.T) {
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/health", nil)

	NewHandler(limiter.NewMemoryLimiter(time.Now)).ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, recorder.Code)
	}
}

func TestCheck(t *testing.T) {
	body := []byte(`{"key":"tenant-a:/api/orders","limit":2,"windowSeconds":60,"cost":1}`)
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/api/stellpulsar/v1/limits/check", bytes.NewReader(body))

	NewHandler(limiter.NewMemoryLimiter(time.Now)).ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, recorder.Code)
	}

	var response limiter.Decision
	if err := json.NewDecoder(recorder.Body).Decode(&response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !response.Allowed {
		t.Fatal("expected allowed decision")
	}
}

func TestCheckRejectsInvalidPayload(t *testing.T) {
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/api/stellpulsar/v1/limits/check", bytes.NewReader([]byte(`{`)))

	NewHandler(limiter.NewMemoryLimiter(time.Now)).ServeHTTP(recorder, request)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, recorder.Code)
	}
}
