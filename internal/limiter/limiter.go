package limiter

import (
	"errors"
	"sync"
	"time"
)

var (
	ErrKeyRequired      = errors.New("limit key is required")
	ErrLimitInvalid     = errors.New("limit must be greater than zero")
	ErrWindowInvalid    = errors.New("windowSeconds must be greater than zero")
	ErrCostInvalid      = errors.New("cost must be greater than zero")
	ErrCostExceedsLimit = errors.New("cost must not exceed limit")
)

type CheckRequest struct {
	Key           string `json:"key"`
	Limit         int    `json:"limit"`
	WindowSeconds int    `json:"windowSeconds"`
	Cost          int    `json:"cost"`
}

type Decision struct {
	Allowed   bool      `json:"allowed"`
	Key       string    `json:"key"`
	Remaining int       `json:"remaining"`
	ResetAt   time.Time `json:"resetAt"`
	Decision  string    `json:"decision"`
}

type MemoryLimiter struct {
	now     func() time.Time
	mu      sync.Mutex
	buckets map[string]*bucket
}

type bucket struct {
	tokens  int
	resetAt time.Time
}

func NewMemoryLimiter(now func() time.Time) *MemoryLimiter {
	if now == nil {
		now = time.Now
	}
	return &MemoryLimiter{
		now:     now,
		buckets: make(map[string]*bucket),
	}
}

func (l *MemoryLimiter) Check(request CheckRequest) (Decision, error) {
	if err := request.validate(); err != nil {
		return Decision{}, err
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	current := l.now().UTC()
	window := time.Duration(request.WindowSeconds) * time.Second
	state := l.buckets[request.Key]
	if state == nil || !current.Before(state.resetAt) {
		state = &bucket{
			tokens:  request.Limit,
			resetAt: current.Add(window),
		}
		l.buckets[request.Key] = state
	}

	if state.tokens < request.Cost {
		return Decision{
			Allowed:   false,
			Key:       request.Key,
			Remaining: state.tokens,
			ResetAt:   state.resetAt,
			Decision:  "rate_limited",
		}, nil
	}

	state.tokens -= request.Cost
	return Decision{
		Allowed:   true,
		Key:       request.Key,
		Remaining: state.tokens,
		ResetAt:   state.resetAt,
		Decision:  "allowed",
	}, nil
}

func (r CheckRequest) validate() error {
	if r.Key == "" {
		return ErrKeyRequired
	}
	if r.Limit <= 0 {
		return ErrLimitInvalid
	}
	if r.WindowSeconds <= 0 {
		return ErrWindowInvalid
	}
	if r.Cost <= 0 {
		return ErrCostInvalid
	}
	if r.Cost > r.Limit {
		return ErrCostExceedsLimit
	}
	return nil
}
