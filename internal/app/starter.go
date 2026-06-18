package app

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/stellhub/stellar"
	stellargrpc "github.com/stellhub/stellar/transport/grpc"
	pb "github.com/stellhub/stellpulsar-service/api/stellpulsar/v1"
	"github.com/stellhub/stellpulsar-service/internal/config"
	"github.com/stellhub/stellpulsar-service/internal/grpcapi"
	"github.com/stellhub/stellpulsar-service/internal/quota"
	"github.com/stellhub/stellpulsar-service/internal/registry"
	"github.com/stellhub/stellpulsar-service/internal/rulestore"
	"github.com/stellhub/stellpulsar-service/internal/runtimeapi"
	"github.com/stellhub/stellpulsar-service/internal/topology"
	"google.golang.org/grpc"
)

type Starter struct {
	cfg      config.Config
	logger   *slog.Logger
	store    *rulestore.Store
	syncer   *runtimeapi.Syncer
	provider registry.Provider
	cancel   context.CancelFunc
	conn     *grpc.ClientConn
}

func NewStarter() *Starter {
	return &Starter{}
}

func (s *Starter) Name() string {
	return "stellpulsar-service"
}

func (s *Starter) Condition(ctx stellar.StarterContext) bool {
	s.cfg = config.Load(ctx.Config())
	return true
}

func (s *Starter) Init(ctx context.Context, app *stellar.App) error {
	s.logger = app.Logger()
	s.store = rulestore.NewEmptyStore(nil)

	cache, ok := app.Cache()
	if !ok {
		return errors.New("stellpulsar requires cache.enabled=true")
	}
	if err := s.store.AttachCache(ctx, cache); err != nil {
		return err
	}
	if strings.TrimSpace(s.cfg.GRPC.ClientName) != "" {
		conn, _, err := app.NewGRPCClient(ctx, s.cfg.GRPC.ClientName)
		if err != nil {
			return err
		}
		s.conn = conn
	}

	serviceRegistry, _ := app.ServiceRegistry()
	s.provider = registry.NewProvider(registry.ProviderConfig{
		Registry: serviceRegistry,
		Config:   s.cfg,
	})

	orbitClient := runtimeapi.NewClient(s.cfg.StellOrbit.RuntimeEndpoint, &http.Client{Timeout: s.cfg.StellOrbit.HTTPTimeout}, runtimeapi.WithBearerToken(s.cfg.StellOrbit.AuthToken))
	s.syncer = runtimeapi.NewSyncer(orbitClient, s.store, s.logger, runtimeapi.SyncerConfig{
		SnapshotPageSize:    s.cfg.Rules.SnapshotPageSize,
		WatchReconnectDelay: s.cfg.Rules.WatchReconnectDelay,
		WatchResyncInterval: s.cfg.Rules.WatchResyncInterval,
		DeltaFetchTimeout:   s.cfg.Rules.DeltaFetchTimeout,
		AllowEmptyStartup:   s.cfg.Rules.AllowEmptyStartup,
		FullSyncMaxRetries:  s.cfg.Rules.FullSyncMaxRetries,
		WatchEventBuffer:    s.cfg.Rules.WatchEventBuffer,
	})

	rpc := app.RPC()
	if rpc == nil {
		return errors.New("stellpulsar requires grpc.server.enabled=true")
	}
	quotaEngine := quota.NewEngine(s.store, nil, quota.Config{
		ServerLagRetryAfter: s.cfg.Rules.ServerLagRetryAfter,
		BucketGCInterval:    s.cfg.Rules.BucketGCInterval,
		ExpiredBucketGrace:  s.cfg.Rules.ExpiredBucketGrace,
		MaxBuckets:          s.cfg.Rules.MaxBuckets,
	})
	var topologyMetrics *topology.Metrics
	if s.cfg.Observability.TopologyMetrics {
		metrics, err := topology.NewMetrics(app.Observability().Meter(), topology.MetricsConfig{
			Namespace:  s.cfg.Instance.Namespace,
			Service:    s.cfg.Instance.Service,
			InstanceID: s.cfg.Instance.ID,
		})
		if err != nil {
			return err
		}
		topologyMetrics = metrics
	}
	service := grpcapi.NewServer(s.store, s.provider, quotaEngine, grpcapi.Config{
		InstanceID:        s.cfg.Instance.ID,
		Namespace:         s.cfg.Instance.Namespace,
		Service:           s.cfg.Instance.Service,
		HeartbeatInterval: s.cfg.Rules.SessionHeartbeatInterval,
		TopologyMetrics:   topologyMetrics,
		AuthToken:         s.cfg.Security.AuthToken,
	})

	if err := rpc.Register(stellargrpc.Service{
		Description:    &pb.StellPulsarDiscoveryService_ServiceDesc,
		Implementation: service,
	}); err != nil {
		return err
	}
	return rpc.Register(stellargrpc.Service{
		Description:    &pb.StellPulsarRuntimeService_ServiceDesc,
		Implementation: service,
	})
}

func (s *Starter) Start(ctx context.Context) error {
	if err := s.syncer.Bootstrap(ctx); err != nil {
		return err
	}
	if err := s.provider.Start(ctx, s.store.Snapshot()); err != nil {
		return err
	}

	watchCtx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	go s.runProviderUpdates(watchCtx)
	go s.syncer.RunWatch(watchCtx)
	return nil
}

func (s *Starter) Stop(ctx context.Context) error {
	if s.cancel != nil {
		s.cancel()
	}
	if s.provider == nil {
		if s.conn != nil {
			return s.conn.Close()
		}
		return nil
	}
	err := s.provider.Stop(ctx)
	if s.conn != nil {
		err = errors.Join(err, s.conn.Close())
	}
	return err
}

func (s *Starter) runProviderUpdates(ctx context.Context) {
	updates, unsubscribe := s.store.Subscribe(ctx, 1)
	defer unsubscribe()

	for {
		select {
		case <-ctx.Done():
			return
		case snapshot, ok := <-updates:
			if !ok {
				return
			}
			updateCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			if err := s.provider.Update(updateCtx, snapshot); err != nil {
				s.logger.Error("update local registry rule metadata failed", "error", err)
			}
			cancel()
		}
	}
}
