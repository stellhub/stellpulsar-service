package grpcapi

import (
	"context"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	pb "github.com/stellhub/stellpulsar-service/api/stellpulsar/v1"
	"github.com/stellhub/stellpulsar-service/internal/quota"
	"github.com/stellhub/stellpulsar-service/internal/registry"
	"github.com/stellhub/stellpulsar-service/internal/rulestore"
	"github.com/stellhub/stellpulsar-service/internal/topology"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const protocolVersion = "stellpulsar.v1"

type Server struct {
	pb.UnimplementedStellPulsarDiscoveryServiceServer
	pb.UnimplementedStellPulsarRuntimeServiceServer

	instanceID        string
	heartbeatInterval time.Duration
	store             *rulestore.Store
	registry          registry.Provider
	topology          *topology.Manager
	quota             *quota.Engine
	authToken         string
}

type Config struct {
	InstanceID        string
	Namespace         string
	Service           string
	HeartbeatInterval time.Duration
	TopologyCacheTTL  time.Duration
	TopologyMetrics   *topology.Metrics
	AuthToken         string
}

func NewServer(store *rulestore.Store, registry registry.Provider, quotaEngine *quota.Engine, config Config) *Server {
	if config.HeartbeatInterval <= 0 {
		config.HeartbeatInterval = 10 * time.Second
	}
	return &Server{
		instanceID:        config.InstanceID,
		heartbeatInterval: config.HeartbeatInterval,
		store:             store,
		registry:          registry,
		topology: topology.NewManager(registry, topology.Config{
			Namespace:      config.Namespace,
			Service:        config.Service,
			SelfInstanceID: config.InstanceID,
			CacheTTL:       config.TopologyCacheTTL,
			Metrics:        config.TopologyMetrics,
		}),
		quota:     quotaEngine,
		authToken: strings.TrimSpace(config.AuthToken),
	}
}

func (s *Server) ListInstances(ctx context.Context, _ *pb.ListInstancesRequest) (*pb.ListInstancesResponse, error) {
	if err := s.authorize(ctx); err != nil {
		return nil, err
	}
	current, err := s.topology.Current(ctx)
	if err != nil {
		return nil, err
	}
	response := &pb.ListInstancesResponse{
		ProtocolVersion:  protocolVersion,
		InstanceRevision: current.Revision,
		ExpiresAtUnixMs:  time.Now().Add(30 * time.Second).UnixMilli(),
		Instances:        make([]*pb.PulsarInstance, 0, len(current.Instances)),
		HashAlgorithm:    current.HashAlgorithm,
	}
	for _, item := range current.Instances {
		response.Instances = append(response.Instances, toProtoInstance(item))
	}
	return response, nil
}

func (s *Server) OpenSession(stream pb.StellPulsarRuntimeService_OpenSessionServer) error {
	if err := s.authorize(stream.Context()); err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	updates, unsubscribe := s.store.Subscribe(ctx, 1)
	defer unsubscribe()

	session := &streamSession{}
	outbound := make(chan *pb.ServerFrame, 16)
	recvDone := make(chan error, 1)
	go s.receiveFrames(ctx, stream, session, outbound, recvDone)

	ticker := time.NewTicker(s.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-recvDone:
			if err == io.EOF {
				return nil
			}
			return err
		case frame := <-outbound:
			if err := stream.Send(frame); err != nil {
				return err
			}
		case snapshot, ok := <-updates:
			if !ok {
				return nil
			}
			if frame := s.ruleChangedFrameForSession(session, snapshot); frame != nil {
				if err := stream.Send(frame); err != nil {
					return err
				}
			}
		case <-ticker.C:
			if sessionID, _ := session.snapshot(); sessionID != "" {
				if err := stream.Send(s.heartbeatFrame(sessionID)); err != nil {
					return err
				}
			}
		}
	}
}

func (s *Server) AcquireQuota(ctx context.Context, request *pb.AcquireQuotaRequest) (*pb.AcquireQuotaResponse, error) {
	if err := s.authorize(ctx); err != nil {
		return nil, err
	}
	if response := s.quota.Validate(request); response.GetDecision() != pb.QuotaDecision_QUOTA_DECISION_UNSPECIFIED {
		return s.decorateTopology(ctx, request, response), nil
	}
	if response, handled := s.validateOwner(ctx, request); handled {
		return response, nil
	}
	return s.decorateTopology(ctx, request, s.quota.Acquire(ctx, request)), nil
}

func (s *Server) GetRuleSnapshot(ctx context.Context, request *pb.GetRuleSnapshotRequest) (*pb.GetRuleSnapshotResponse, error) {
	if err := s.authorize(ctx); err != nil {
		return nil, err
	}
	snapshot := s.store.Snapshot()
	response := &pb.GetRuleSnapshotResponse{
		ApplicationCode: request.GetApplicationCode(),
		Revision:        strconv.FormatInt(snapshot.Version, 10),
		Checksum:        snapshot.Checksum,
	}
	for _, rule := range s.store.Rules(request.GetApplicationCode(), request.GetRuleIds()) {
		response.Rules = append(response.Rules, toProtoRule(rule))
	}
	return response, nil
}

func (s *Server) ValidateRuleSnapshot(ctx context.Context, request *pb.ValidateRuleSnapshotRequest) (*pb.ValidateRuleSnapshotResponse, error) {
	if err := s.authorize(ctx); err != nil {
		return nil, err
	}
	response := &pb.ValidateRuleSnapshotResponse{
		ApplicationCode: request.GetApplicationCode(),
		Status:          pb.RuleValidationStatus_RULE_VALIDATION_STATUS_MATCHED,
	}
	for _, digest := range request.GetRuleDigests() {
		result := s.validateDigest(digest)
		response.Results = append(response.Results, result)
		response.Status = mergeStatus(response.Status, result.GetStatus())
	}
	return response, nil
}

type streamSession struct {
	mu           sync.RWMutex
	sessionID    string
	application  string
	lastRevision string
}

func (s *streamSession) set(sessionID, application string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sessionID = sessionID
	if strings.TrimSpace(application) != "" {
		s.application = strings.TrimSpace(application)
	}
}

func (s *streamSession) setRevision(revision string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastRevision = revision
}

func (s *streamSession) snapshot() (string, string) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.sessionID, s.application
}

func (s *streamSession) revision() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastRevision
}

func (s *Server) receiveFrames(ctx context.Context, stream pb.StellPulsarRuntimeService_OpenSessionServer, session *streamSession, outbound chan<- *pb.ServerFrame, done chan<- error) {
	for {
		frame, err := stream.Recv()
		if err != nil {
			done <- err
			return
		}
		sessionID := frame.GetSessionId()
		applicationCode := frame.GetApplicationCode()
		switch payload := frame.GetPayload().(type) {
		case *pb.ClientFrame_Hello:
			applicationCode = chooseString(applicationCode, payload.Hello.GetApplicationCode())
			session.set(sessionID, applicationCode)
			hello := s.helloFrame(sessionID, applicationCode)
			session.setRevision(hello.GetHello().GetGlobalRuleRevision())
			if !enqueueFrame(ctx, outbound, hello) {
				done <- ctx.Err()
				return
			}
			if changed := s.changedDigests(applicationCode, payload.Hello.GetRuleDigests()); len(changed) > 0 {
				if !enqueueFrame(ctx, outbound, s.ruleChangedFrame(sessionID, strconv.FormatInt(s.store.Snapshot().Version, 10), changed)) {
					done <- ctx.Err()
					return
				}
			}
		case *pb.ClientFrame_Heartbeat:
			session.set(sessionID, applicationCode)
			if !enqueueFrame(ctx, outbound, s.heartbeatFrame(sessionID)) {
				done <- ctx.Err()
				return
			}
		case *pb.ClientFrame_RuleDigest:
			session.set(sessionID, applicationCode)
			changed := s.changedDigests(applicationCode, payload.RuleDigest.GetRuleDigests())
			if len(changed) == 0 {
				if !enqueueFrame(ctx, outbound, s.heartbeatFrame(sessionID)) {
					done <- ctx.Err()
					return
				}
				continue
			}
			if !enqueueFrame(ctx, outbound, s.ruleChangedFrame(sessionID, strconv.FormatInt(s.store.Snapshot().Version, 10), changed)) {
				done <- ctx.Err()
				return
			}
		case *pb.ClientFrame_Ack:
			if payload.Ack.GetRevision() != "" {
				session.setRevision(payload.Ack.GetRevision())
			}
			continue
		default:
			if !enqueueFrame(ctx, outbound, &pb.ServerFrame{
				SessionId:        sessionID,
				ServerInstanceId: s.instanceID,
				Payload: &pb.ServerFrame_Error{
					Error: &pb.ServerError{Code: "unsupported_frame", Message: "unsupported client frame"},
				},
			}) {
				done <- ctx.Err()
				return
			}
		}
	}
}

func enqueueFrame(ctx context.Context, outbound chan<- *pb.ServerFrame, frame *pb.ServerFrame) bool {
	select {
	case <-ctx.Done():
		return false
	case outbound <- frame:
		return true
	}
}

func (s *Server) helloFrame(sessionID, applicationCode string) *pb.ServerFrame {
	snapshot := s.store.Snapshot()
	return &pb.ServerFrame{
		SessionId:        sessionID,
		ServerInstanceId: s.instanceID,
		Payload: &pb.ServerFrame_Hello{
			Hello: &pb.ServerHello{
				ProtocolVersion:     protocolVersion,
				ServerInstanceId:    s.instanceID,
				GlobalRuleRevision:  strconv.FormatInt(snapshot.Version, 10),
				RuleDigests:         s.digests(applicationCode),
				HeartbeatIntervalMs: s.heartbeatInterval.Milliseconds(),
			},
		},
	}
}

func (s *Server) heartbeatFrame(sessionID string) *pb.ServerFrame {
	return &pb.ServerFrame{
		SessionId:        sessionID,
		ServerInstanceId: s.instanceID,
		Payload: &pb.ServerFrame_Heartbeat{
			Heartbeat: &pb.ServerHeartbeat{
				SentAtUnixMs:       time.Now().UnixMilli(),
				GlobalRuleRevision: strconv.FormatInt(s.store.Snapshot().Version, 10),
			},
		},
	}
}

func (s *Server) ruleChangedFrame(sessionID string, revision string, changed []*pb.RuleDigest) *pb.ServerFrame {
	return &pb.ServerFrame{
		SessionId:        sessionID,
		ServerInstanceId: s.instanceID,
		Payload: &pb.ServerFrame_RuleChanged{
			RuleChanged: &pb.ServerRuleChanged{
				GlobalRuleRevision:   revision,
				ChangedRules:         changed,
				RequiresClientResync: true,
			},
		},
	}
}

func (s *Server) ruleChangedFrameForSession(session *streamSession, snapshot rulestore.Snapshot) *pb.ServerFrame {
	sessionID, applicationCode := session.snapshot()
	if sessionID == "" || applicationCode == "" {
		return nil
	}
	revision := strconv.FormatInt(snapshot.Version, 10)
	if revision == session.revision() {
		return nil
	}
	session.setRevision(revision)
	changed := digestsFromSnapshot(snapshot, applicationCode)
	return s.ruleChangedFrame(sessionID, revision, changed)
}

func (s *Server) digests(applicationCode string) []*pb.RuleDigest {
	rules := s.store.Rules(applicationCode, nil)
	digests := make([]*pb.RuleDigest, 0, len(rules))
	for _, rule := range rules {
		digests = append(digests, toDigest(rule))
	}
	return digests
}

func (s *Server) changedDigests(applicationCode string, clientDigests []*pb.RuleDigest) []*pb.RuleDigest {
	var changed []*pb.RuleDigest
	seen := map[string]struct{}{}
	for _, clientDigest := range clientDigests {
		digestApplication := chooseString(clientDigest.GetApplicationCode(), applicationCode)
		seen[digestApplication+"\x00"+clientDigest.GetRuleId()] = struct{}{}
		rule, ok := s.store.FindRule(digestApplication, clientDigest.GetRuleId())
		if !ok {
			changed = append(changed, &pb.RuleDigest{
				ApplicationCode: digestApplication,
				RuleId:          clientDigest.GetRuleId(),
			})
			continue
		}
		if clientDigest.GetRevision() != rule.Revision() || clientDigest.GetChecksum() != rule.Checksum {
			changed = append(changed, toDigest(rule))
		}
	}
	for _, rule := range s.store.Rules(applicationCode, nil) {
		key := rule.ApplicationCode + "\x00" + rule.RuleID
		if _, ok := seen[key]; ok {
			continue
		}
		changed = append(changed, toDigest(rule))
	}
	return changed
}

func (s *Server) validateDigest(clientDigest *pb.RuleDigest) *pb.RuleValidationResult {
	result := &pb.RuleValidationResult{
		ClientDigest: clientDigest,
	}
	rule, ok := s.store.FindRule(clientDigest.GetApplicationCode(), clientDigest.GetRuleId())
	if !ok {
		result.Status = pb.RuleValidationStatus_RULE_VALIDATION_STATUS_NOT_FOUND
		result.Message = "rule_not_found"
		return result
	}
	serverDigest := toDigest(rule)
	result.ServerDigest = serverDigest

	clientVersion, err := strconv.ParseInt(clientDigest.GetRevision(), 10, 64)
	switch {
	case err == nil && clientVersion < rule.SnapshotVersion:
		result.Status = pb.RuleValidationStatus_RULE_VALIDATION_STATUS_CLIENT_STALE
		result.Message = "client_rule_stale"
	case err == nil && clientVersion > rule.SnapshotVersion:
		result.Status = pb.RuleValidationStatus_RULE_VALIDATION_STATUS_SERVER_STALE
		result.Message = "server_rule_stale"
	case clientDigest.GetChecksum() != "" && clientDigest.GetChecksum() != rule.Checksum:
		result.Status = pb.RuleValidationStatus_RULE_VALIDATION_STATUS_CONFLICT
		result.Message = "rule_checksum_conflict"
	default:
		result.Status = pb.RuleValidationStatus_RULE_VALIDATION_STATUS_MATCHED
		result.Message = "matched"
	}
	return result
}

func toDigest(rule rulestore.Rule) *pb.RuleDigest {
	return &pb.RuleDigest{
		ApplicationCode: rule.ApplicationCode,
		RuleId:          rule.RuleID,
		Revision:        rule.Revision(),
		Checksum:        rule.Checksum,
		SchemaVersion:   rule.SchemaVersion,
	}
}

func toProtoRule(rule rulestore.Rule) *pb.RateLimitRule {
	return &pb.RateLimitRule{
		ApplicationCode: rule.ApplicationCode,
		RuleId:          rule.RuleID,
		Name:            rule.Name,
		Revision:        rule.Revision(),
		Checksum:        rule.Checksum,
		SchemaVersion:   rule.SchemaVersion,
		Algorithm:       rule.Algorithm,
		Quota:           rule.Quota,
		WindowSeconds:   rule.WindowSeconds,
		Burst:           rule.Burst,
		Dimensions:      append([]string(nil), rule.Dimensions...),
		FailPolicy:      rule.FailPolicy,
		Attributes:      cloneMap(rule.Attributes),
	}
}

func mergeStatus(current, next pb.RuleValidationStatus) pb.RuleValidationStatus {
	if current == pb.RuleValidationStatus_RULE_VALIDATION_STATUS_UNSPECIFIED {
		return next
	}
	if next == pb.RuleValidationStatus_RULE_VALIDATION_STATUS_MATCHED {
		return current
	}
	if current == pb.RuleValidationStatus_RULE_VALIDATION_STATUS_MATCHED {
		return next
	}
	if next == pb.RuleValidationStatus_RULE_VALIDATION_STATUS_CONFLICT || current == pb.RuleValidationStatus_RULE_VALIDATION_STATUS_CONFLICT {
		return pb.RuleValidationStatus_RULE_VALIDATION_STATUS_CONFLICT
	}
	return current
}

func cloneMap(values map[string]string) map[string]string {
	if values == nil {
		return map[string]string{}
	}
	cloned := make(map[string]string, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

func (s *Server) validateOwner(ctx context.Context, request *pb.AcquireQuotaRequest) (*pb.AcquireQuotaResponse, bool) {
	if strings.TrimSpace(request.GetTopologyRevision()) == "" {
		s.recordOwnerCheck(ctx, "invalid_request", "topology_revision_required")
		return s.decorateTopology(ctx, request, &pb.AcquireQuotaResponse{
			RequestId: request.GetRequestId(),
			RuleId:    request.GetRuleId(),
			Decision:  pb.QuotaDecision_QUOTA_DECISION_INVALID_REQUEST,
			Reason:    "topology_revision_required",
		}), true
	}
	if strings.TrimSpace(request.GetTargetInstanceId()) == "" {
		s.recordOwnerCheck(ctx, "invalid_request", "target_instance_id_required")
		return s.decorateTopology(ctx, request, &pb.AcquireQuotaResponse{
			RequestId: request.GetRequestId(),
			RuleId:    request.GetRuleId(),
			Decision:  pb.QuotaDecision_QUOTA_DECISION_INVALID_REQUEST,
			Reason:    "target_instance_id_required",
		}), true
	}

	shardKey := topology.ShardKey(request.GetApplicationCode(), request.GetRuleId(), request.GetQuotaKey())
	owner, current, ok, err := s.topology.OwnerOf(ctx, shardKey)
	if err != nil {
		s.recordOwnerCheck(ctx, "topology_unavailable", "topology_unavailable")
		return &pb.AcquireQuotaResponse{
			RequestId: request.GetRequestId(),
			RuleId:    request.GetRuleId(),
			Decision:  pb.QuotaDecision_QUOTA_DECISION_DENIED,
			Reason:    "topology_unavailable",
		}, true
	}
	if !ok {
		s.recordOwnerCheck(ctx, "shard_migrating", "topology_owner_unavailable")
		return &pb.AcquireQuotaResponse{
			RequestId:        request.GetRequestId(),
			RuleId:           request.GetRuleId(),
			Decision:         pb.QuotaDecision_QUOTA_DECISION_SHARD_MIGRATING,
			Reason:           "topology_owner_unavailable",
			RetryAfterMs:     50,
			TopologyRevision: current.Revision,
			HashAlgorithm:    current.HashAlgorithm,
			OwnerInstance:    nil,
		}, true
	}
	if request.GetTopologyRevision() != "" && request.GetTopologyRevision() != current.Revision {
		s.recordOwnerCheck(ctx, "not_owner", "topology_revision_mismatch")
		return s.notOwnerResponse(request, current, owner, "topology_revision_mismatch"), true
	}
	if request.GetTargetInstanceId() != "" && request.GetTargetInstanceId() != owner.InstanceID {
		s.recordOwnerCheck(ctx, "not_owner", "target_instance_mismatch")
		return s.notOwnerResponse(request, current, owner, "target_instance_mismatch"), true
	}
	if owner.InstanceID != s.instanceID {
		s.recordOwnerCheck(ctx, "not_owner", "not_owner")
		return s.notOwnerResponse(request, current, owner, "not_owner"), true
	}
	s.recordOwnerCheck(ctx, "matched", "owner_matched")
	return nil, false
}

func (s *Server) recordOwnerCheck(ctx context.Context, result string, reason string) {
	if s == nil || s.topology == nil {
		return
	}
	s.topology.RecordOwnerCheck(ctx, result, reason)
}

func (s *Server) notOwnerResponse(request *pb.AcquireQuotaRequest, current topology.Topology, owner registry.Instance, reason string) *pb.AcquireQuotaResponse {
	return &pb.AcquireQuotaResponse{
		RequestId:        request.GetRequestId(),
		RuleId:           request.GetRuleId(),
		Decision:         pb.QuotaDecision_QUOTA_DECISION_NOT_OWNER,
		Reason:           reason,
		RetryAfterMs:     10,
		TopologyRevision: current.Revision,
		OwnerInstance:    toProtoInstance(owner),
		HashAlgorithm:    current.HashAlgorithm,
	}
}

func (s *Server) decorateTopology(ctx context.Context, request *pb.AcquireQuotaRequest, response *pb.AcquireQuotaResponse) *pb.AcquireQuotaResponse {
	if response == nil || request == nil {
		return response
	}
	shardKey := topology.ShardKey(request.GetApplicationCode(), request.GetRuleId(), request.GetQuotaKey())
	owner, current, ok, err := s.topology.OwnerOf(ctx, shardKey)
	if err != nil {
		return response
	}
	response.TopologyRevision = current.Revision
	response.HashAlgorithm = current.HashAlgorithm
	if ok {
		response.OwnerInstance = toProtoInstance(owner)
	}
	return response
}

func toProtoInstance(item registry.Instance) *pb.PulsarInstance {
	return &pb.PulsarInstance{
		InstanceId:   item.InstanceID,
		Host:         item.Host,
		Port:         item.Port,
		Priority:     item.Priority,
		Weight:       item.Weight,
		Zone:         item.Zone,
		Version:      item.Version,
		RuleRevision: item.RuleRevision,
		State:        item.State,
		Metadata:     cloneMap(item.Metadata),
	}
}

func digestsFromSnapshot(snapshot rulestore.Snapshot, applicationCode string) []*pb.RuleDigest {
	app, ok := snapshot.Apps[applicationCode]
	if !ok {
		return nil
	}
	digests := make([]*pb.RuleDigest, 0, len(app.Rules))
	for _, rule := range app.Rules {
		digests = append(digests, toDigest(rule))
	}
	return digests
}

func (s *Server) authorize(ctx context.Context) error {
	if s.authToken == "" {
		return nil
	}
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "stellpulsar authorization metadata is required")
	}
	for _, key := range []string{"authorization", "x-stellpulsar-token"} {
		for _, value := range md.Get(key) {
			if tokenMatches(value, s.authToken) {
				return nil
			}
		}
	}
	return status.Error(codes.Unauthenticated, "invalid stellpulsar authorization token")
}

func tokenMatches(value string, expected string) bool {
	value = strings.TrimSpace(value)
	expected = strings.TrimSpace(expected)
	if strings.HasPrefix(strings.ToLower(value), "bearer ") {
		value = strings.TrimSpace(value[len("bearer "):])
	}
	return value == expected
}

func chooseString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
