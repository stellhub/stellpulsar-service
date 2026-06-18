package quota

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"time"

	pb "github.com/stellhub/stellpulsar-service/api/stellpulsar/v1"
	"github.com/stellhub/stellpulsar-service/internal/rulestore"
)

type Engine struct {
	store               *rulestore.Store
	now                 func() time.Time
	serverLagRetryAfter time.Duration
	bucketGCInterval    time.Duration
	expiredBucketGrace  time.Duration
	maxBuckets          int
	nextCleanupAt       time.Time
	mu                  sync.Mutex
	buckets             map[string]*bucket
}

type Config struct {
	ServerLagRetryAfter time.Duration
	BucketGCInterval    time.Duration
	ExpiredBucketGrace  time.Duration
	MaxBuckets          int
}

type bucket struct {
	remaining int64
	resetAt   time.Time
}

func NewEngine(store *rulestore.Store, now func() time.Time, config Config) *Engine {
	if now == nil {
		now = time.Now
	}
	if config.ServerLagRetryAfter <= 0 {
		config.ServerLagRetryAfter = 50 * time.Millisecond
	}
	if config.BucketGCInterval <= 0 {
		config.BucketGCInterval = 30 * time.Second
	}
	if config.ExpiredBucketGrace <= 0 {
		config.ExpiredBucketGrace = 2 * time.Minute
	}
	if config.MaxBuckets <= 0 {
		config.MaxBuckets = 1_000_000
	}
	return &Engine{
		store:               store,
		now:                 now,
		serverLagRetryAfter: config.ServerLagRetryAfter,
		bucketGCInterval:    config.BucketGCInterval,
		expiredBucketGrace:  config.ExpiredBucketGrace,
		maxBuckets:          config.MaxBuckets,
		buckets:             map[string]*bucket{},
	}
}

func (e *Engine) Acquire(_ context.Context, request *pb.AcquireQuotaRequest) *pb.AcquireQuotaResponse {
	response := e.Validate(request)
	if response.GetDecision() != pb.QuotaDecision_QUOTA_DECISION_UNSPECIFIED {
		return response
	}
	if request.GetCost() <= 0 {
		request.Cost = 1
	}

	rule, _ := e.store.FindRule(request.GetApplicationCode(), request.GetRuleId())
	key := request.GetApplicationCode() + ":" + request.GetRuleId() + ":" + request.GetQuotaKey()
	current := e.now().UTC()
	window := time.Duration(rule.WindowSeconds) * time.Second

	e.mu.Lock()
	defer e.mu.Unlock()
	e.cleanupExpiredLocked(current)

	state := e.buckets[key]
	if state == nil && len(e.buckets) >= e.maxBuckets {
		response.Decision = pb.QuotaDecision_QUOTA_DECISION_DENIED
		response.Reason = "bucket_capacity_exceeded"
		response.RetryAfterMs = e.bucketGCInterval.Milliseconds()
		return response
	}

	if state == nil || !current.Before(state.resetAt) {
		state = &bucket{
			remaining: rule.Quota,
			resetAt:   current.Add(window),
		}
		e.buckets[key] = state
	}
	if state.remaining < request.GetCost() {
		response.Decision = pb.QuotaDecision_QUOTA_DECISION_DENIED
		response.Remaining = state.remaining
		response.ResetAtUnixMs = state.resetAt.UnixMilli()
		response.RetryAfterMs = maxInt64(0, state.resetAt.Sub(current).Milliseconds())
		response.Reason = "rate_limited"
		return response
	}

	state.remaining -= request.GetCost()
	response.Decision = pb.QuotaDecision_QUOTA_DECISION_ALLOWED
	response.Remaining = state.remaining
	response.ResetAtUnixMs = state.resetAt.UnixMilli()
	response.Reason = "allowed"
	return response
}

func (e *Engine) Validate(request *pb.AcquireQuotaRequest) *pb.AcquireQuotaResponse {
	response := &pb.AcquireQuotaResponse{
		RequestId: request.GetRequestId(),
		RuleId:    request.GetRuleId(),
	}
	if reason := validateRequest(request); reason != "" {
		response.Decision = pb.QuotaDecision_QUOTA_DECISION_INVALID_REQUEST
		response.Reason = reason
		return response
	}

	rule, ok := e.store.FindRule(request.GetApplicationCode(), request.GetRuleId())
	if !ok {
		response.Decision = pb.QuotaDecision_QUOTA_DECISION_RULE_NOT_FOUND
		response.Reason = "rule_not_found"
		return response
	}
	response.RuleRevision = rule.Revision()
	response.RuleChecksum = rule.Checksum

	if decision, reason := e.compareRuleVersion(rule, request.GetRuleRevision(), request.GetRuleChecksum()); decision != pb.QuotaDecision_QUOTA_DECISION_UNSPECIFIED {
		response.Decision = decision
		response.Reason = reason
		if decision == pb.QuotaDecision_QUOTA_DECISION_SERVER_RULE_LAG {
			response.RetryAfterMs = e.serverLagRetryAfter.Milliseconds()
		}
		return response
	}
	if rule.Quota <= 0 || rule.WindowSeconds <= 0 {
		response.Decision = pb.QuotaDecision_QUOTA_DECISION_DENIED
		response.Reason = "invalid_rule_quota"
		return response
	}
	return response
}

func validateRequest(request *pb.AcquireQuotaRequest) string {
	switch {
	case request == nil:
		return "request_required"
	case strings.TrimSpace(request.GetApplicationCode()) == "":
		return "application_code_required"
	case strings.TrimSpace(request.GetRuleId()) == "":
		return "rule_id_required"
	case strings.TrimSpace(request.GetQuotaKey()) == "":
		return "quota_key_required"
	case strings.TrimSpace(request.GetRuleRevision()) == "":
		return "rule_revision_required"
	case strings.TrimSpace(request.GetRuleChecksum()) == "":
		return "rule_checksum_required"
	}
	return ""
}

func (e *Engine) compareRuleVersion(rule rulestore.Rule, clientRevision, clientChecksum string) (pb.QuotaDecision, string) {
	if clientRevision == "" && clientChecksum == "" {
		return pb.QuotaDecision_QUOTA_DECISION_UNSPECIFIED, ""
	}
	if clientRevision != "" {
		clientVersion, err := strconv.ParseInt(clientRevision, 10, 64)
		if err == nil {
			switch {
			case clientVersion > rule.SnapshotVersion:
				return pb.QuotaDecision_QUOTA_DECISION_SERVER_RULE_LAG, "server_rule_lag"
			case clientVersion < rule.SnapshotVersion:
				return pb.QuotaDecision_QUOTA_DECISION_RULE_STALE, "client_rule_stale"
			}
		} else if clientRevision != rule.Revision() {
			return pb.QuotaDecision_QUOTA_DECISION_RULE_CONFLICT, "rule_revision_conflict"
		}
	}
	if clientChecksum != "" && clientChecksum != rule.Checksum {
		return pb.QuotaDecision_QUOTA_DECISION_RULE_CONFLICT, "rule_checksum_conflict"
	}
	return pb.QuotaDecision_QUOTA_DECISION_UNSPECIFIED, ""
}

func maxInt64(left, right int64) int64 {
	if left > right {
		return left
	}
	return right
}

func (e *Engine) cleanupExpiredLocked(current time.Time) {
	if !e.nextCleanupAt.IsZero() && current.Before(e.nextCleanupAt) && len(e.buckets) < e.maxBuckets {
		return
	}
	for key, state := range e.buckets {
		if state == nil || !current.Before(state.resetAt.Add(e.expiredBucketGrace)) {
			delete(e.buckets, key)
		}
	}
	e.nextCleanupAt = current.Add(e.bucketGCInterval)
}
