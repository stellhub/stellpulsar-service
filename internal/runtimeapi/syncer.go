package runtimeapi

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/stellhub/stellpulsar-service/internal/rulestore"
)

type SyncerConfig struct {
	SnapshotPageSize    int
	WatchReconnectDelay time.Duration
	WatchResyncInterval time.Duration
	DeltaFetchTimeout   time.Duration
	AllowEmptyStartup   bool
	FullSyncMaxRetries  int
	WatchEventBuffer    int
}

type Syncer struct {
	client *Client
	store  *rulestore.Store
	logger *slog.Logger
	config SyncerConfig
	now    func() time.Time
	mu     sync.Mutex
}

type snapshotChangedWhilePagingError struct {
	firstVersion    int64
	firstChecksum   string
	currentVersion  int64
	currentChecksum string
}

func (e snapshotChangedWhilePagingError) Error() string {
	return fmt.Sprintf("snapshot changed while paging: first=%d/%s current=%d/%s", e.firstVersion, e.firstChecksum, e.currentVersion, e.currentChecksum)
}

func NewSyncer(client *Client, store *rulestore.Store, logger *slog.Logger, config SyncerConfig) *Syncer {
	if logger == nil {
		logger = slog.Default()
	}
	if config.SnapshotPageSize <= 0 {
		config.SnapshotPageSize = 100
	}
	if config.WatchReconnectDelay <= 0 {
		config.WatchReconnectDelay = 500 * time.Millisecond
	}
	if config.WatchResyncInterval <= 0 {
		config.WatchResyncInterval = 30 * time.Second
	}
	if config.DeltaFetchTimeout <= 0 {
		config.DeltaFetchTimeout = 3 * time.Second
	}
	if config.FullSyncMaxRetries <= 0 {
		config.FullSyncMaxRetries = 3
	}
	if config.WatchEventBuffer <= 0 {
		config.WatchEventBuffer = 64
	}
	return &Syncer{
		client: client,
		store:  store,
		logger: logger,
		config: config,
		now:    time.Now,
	}
}

func (s *Syncer) Bootstrap(ctx context.Context) error {
	if s.client == nil || !s.client.Enabled() {
		if s.config.AllowEmptyStartup {
			if current := s.store.Snapshot(); !current.Empty() {
				s.logger.Warn("stellorbit runtime endpoint is empty, starting with cached rule snapshot", "version", current.Version, "checksum", current.Checksum)
				return nil
			}
			if err := s.store.ReplaceWithContext(ctx, rulestore.EmptySnapshot(s.now())); err != nil {
				return err
			}
			s.logger.Warn("stellorbit runtime endpoint is empty, starting with empty rule snapshot")
			return nil
		}
		return errors.New("stellorbit runtime endpoint is required")
	}
	if err := s.fullSync(ctx); err != nil {
		if s.config.AllowEmptyStartup {
			current := s.store.Snapshot()
			if !current.Empty() {
				s.logger.Warn("stellorbit bootstrap full sync failed, starting with cached rule snapshot", "version", current.Version, "checksum", current.Checksum, "error", err)
				return nil
			}
			if cacheErr := s.store.ReplaceWithContext(ctx, rulestore.EmptySnapshot(s.now())); cacheErr != nil {
				return errors.Join(err, cacheErr)
			}
			s.logger.Warn("stellorbit bootstrap full sync failed, starting with empty rule snapshot", "error", err)
			return nil
		}
		return err
	}
	return nil
}

func (s *Syncer) RunWatch(ctx context.Context) {
	if s.client == nil || !s.client.Enabled() {
		return
	}

	resyncTicker := time.NewTicker(s.config.WatchResyncInterval)
	defer resyncTicker.Stop()
	events := make(chan WatchEvent, s.config.WatchEventBuffer)
	go s.processWatchEvents(ctx, events)

	for {
		snapshot := s.store.Snapshot()
		watchCtx, cancelWatch := context.WithCancel(ctx)
		watchDone := make(chan error, 1)
		go func() {
			watchDone <- s.client.Watch(watchCtx, snapshot.Version, snapshot.Checksum, func(event WatchEvent) error {
				select {
				case events <- event:
					return nil
				default:
					return errors.New("stellorbit watch event backlog is full")
				}
			})
		}()

		reconnect := false
		for !reconnect {
			select {
			case <-ctx.Done():
				cancelWatch()
				<-watchDone
				return
			case <-resyncTicker.C:
				if err := s.fullSync(ctx); err != nil {
					s.logger.Error("periodic rule full sync failed", "error", err)
				}
				cancelWatch()
				err := <-watchDone
				if err != nil && !errors.Is(err, context.Canceled) && ctx.Err() == nil {
					s.logger.Error("stellorbit watch stream stopped", "error", err)
				}
				reconnect = true
			case err := <-watchDone:
				cancelWatch()
				if errors.Is(err, context.Canceled) || ctx.Err() != nil {
					return
				}
				if err != nil {
					s.logger.Error("stellorbit watch stream stopped", "error", err)
				}
				reconnect = true
			}
		}

		timer := time.NewTimer(s.config.WatchReconnectDelay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (s *Syncer) processWatchEvents(ctx context.Context, events <-chan WatchEvent) {
	for {
		select {
		case <-ctx.Done():
			return
		case event := <-events:
			if err := s.handleWatchEvent(ctx, event); err != nil {
				s.logger.Error("stellorbit watch event handling failed", "eventType", event.EventType, "error", err)
			}
		}
	}
}

func (s *Syncer) handleWatchEvent(ctx context.Context, event WatchEvent) error {
	switch event.EventType {
	case "WATCH_HEARTBEAT":
		s.logger.Debug("stellorbit rule watch heartbeat", "latestSnapshotVersion", event.LatestSnapshotVersion)
		return nil
	case "DELTA_CHANGED":
		return s.fetchAndApplyDelta(ctx)
	case "FULL_SYNC_REQUIRED":
		return s.fullSync(ctx)
	default:
		s.logger.Warn("unknown stellorbit watch event", "eventType", event.EventType)
		return nil
	}
}

func (s *Syncer) fullSync(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var lastErr error
	for attempt := 1; attempt <= s.config.FullSyncMaxRetries; attempt++ {
		if err := s.fullSyncLocked(ctx); err != nil {
			lastErr = err
			var changed snapshotChangedWhilePagingError
			if errors.As(err, &changed) && attempt < s.config.FullSyncMaxRetries {
				s.logger.Warn("snapshot changed while paging, retrying full sync", "attempt", attempt, "error", err)
				continue
			}
			return err
		}
		return nil
	}
	return lastErr
}

func (s *Syncer) fullSyncLocked(ctx context.Context) error {
	var configs []rulestore.PublishedConfig
	var snapshotVersion int64
	var checksum string
	var generatedAt time.Time
	totalPages := 1

	for page := 0; page < totalPages; page++ {
		response, err := s.client.Snapshot(ctx, page, s.config.SnapshotPageSize)
		if err != nil {
			return err
		}
		if page == 0 {
			snapshotVersion = response.SnapshotVersion
			checksum = response.Checksum
			generatedAt = response.GeneratedAt
			totalPages = response.Configs.TotalPages
			if totalPages <= 0 {
				totalPages = 1
			}
		}
		if response.SnapshotVersion != snapshotVersion || response.Checksum != checksum {
			return snapshotChangedWhilePagingError{
				firstVersion:    snapshotVersion,
				firstChecksum:   checksum,
				currentVersion:  response.SnapshotVersion,
				currentChecksum: response.Checksum,
			}
		}
		for _, cfg := range response.Configs.Content {
			configs = append(configs, cfg.Published())
		}
	}

	snapshot, err := rulestore.BuildSnapshot(snapshotVersion, checksum, generatedAt, configs)
	if err != nil {
		return err
	}
	if err := s.store.ReplaceWithContext(ctx, snapshot); err != nil {
		return err
	}
	s.logger.Info("loaded stellorbit distributed rate limit snapshot", "version", snapshotVersion, "checksum", checksum, "configs", len(configs))
	return nil
}

func (s *Syncer) fetchAndApplyDelta(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	current := s.store.Snapshot()
	deltaCtx, cancel := context.WithTimeout(ctx, s.config.DeltaFetchTimeout)
	defer cancel()

	delta, err := s.client.Changes(deltaCtx, current.Version, current.Checksum)
	if err != nil {
		return err
	}
	if delta.ChangeCount != 0 && delta.ChangeCount != len(delta.Changes) {
		return fmt.Errorf("delta change count mismatch: declared=%d actual=%d", delta.ChangeCount, len(delta.Changes))
	}
	if delta.ToSnapshotVersion <= current.Version {
		return fmt.Errorf("delta target version must advance: current=%d target=%d", current.Version, delta.ToSnapshotVersion)
	}
	if delta.ToChecksum == "" {
		return errors.New("delta target checksum is required")
	}
	if delta.FromSnapshotVersion != current.Version || delta.FromChecksum != current.Checksum {
		if err := s.fullSyncLocked(ctx); err != nil {
			return fmt.Errorf("delta is not continuous and full sync failed: %w", err)
		}
		return nil
	}
	changes := make([]rulestore.PublishedChange, 0, len(delta.Changes))
	for _, change := range delta.Changes {
		changes = append(changes, change.Published())
	}
	next, err := rulestore.ApplyChanges(current, delta.ToSnapshotVersion, delta.ToChecksum, delta.GeneratedAt, changes)
	if err != nil {
		return err
	}
	if err := s.store.ReplaceWithContext(ctx, next); err != nil {
		return err
	}
	s.logger.Info("applied stellorbit distributed rate limit delta", "fromVersion", delta.FromSnapshotVersion, "toVersion", delta.ToSnapshotVersion, "changes", len(changes))
	return nil
}
