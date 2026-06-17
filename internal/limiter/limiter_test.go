package limiter

import (
	"errors"
	"testing"
	"time"
)

func TestMemoryLimiterAllowsWithinLimit(t *testing.T) {
	now := time.Date(2026, 6, 17, 0, 0, 0, 0, time.UTC)
	limiter := NewMemoryLimiter(func() time.Time { return now })

	decision, err := limiter.Check(CheckRequest{
		Key:           "tenant-a:/api/orders",
		Limit:         2,
		WindowSeconds: 60,
		Cost:          1,
	})
	if err != nil {
		t.Fatalf("check limit: %v", err)
	}
	if !decision.Allowed {
		t.Fatal("expected request to be allowed")
	}
	if decision.Remaining != 1 {
		t.Fatalf("expected remaining 1, got %d", decision.Remaining)
	}
}

func TestMemoryLimiterRejectsAfterLimit(t *testing.T) {
	now := time.Date(2026, 6, 17, 0, 0, 0, 0, time.UTC)
	limiter := NewMemoryLimiter(func() time.Time { return now })
	request := CheckRequest{Key: "tenant-a", Limit: 1, WindowSeconds: 60, Cost: 1}

	if _, err := limiter.Check(request); err != nil {
		t.Fatalf("first check: %v", err)
	}
	decision, err := limiter.Check(request)
	if err != nil {
		t.Fatalf("second check: %v", err)
	}
	if decision.Allowed {
		t.Fatal("expected request to be rate limited")
	}
	if decision.Decision != "rate_limited" {
		t.Fatalf("expected rate_limited decision, got %s", decision.Decision)
	}
}

func TestMemoryLimiterResetsWindow(t *testing.T) {
	now := time.Date(2026, 6, 17, 0, 0, 0, 0, time.UTC)
	limiter := NewMemoryLimiter(func() time.Time { return now })
	request := CheckRequest{Key: "tenant-a", Limit: 1, WindowSeconds: 60, Cost: 1}

	_, _ = limiter.Check(request)
	now = now.Add(61 * time.Second)

	decision, err := limiter.Check(request)
	if err != nil {
		t.Fatalf("check after reset: %v", err)
	}
	if !decision.Allowed {
		t.Fatal("expected request after reset to be allowed")
	}
}

func TestCheckRequestValidation(t *testing.T) {
	limiter := NewMemoryLimiter(time.Now)

	_, err := limiter.Check(CheckRequest{Limit: 1, WindowSeconds: 60, Cost: 1})
	if !errors.Is(err, ErrKeyRequired) {
		t.Fatalf("expected ErrKeyRequired, got %v", err)
	}
}
