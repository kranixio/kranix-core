package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/kranix-io/kranix-core/internal/autoscaler"
	"github.com/kranix-io/kranix-core/internal/circuitbreaker"
	"github.com/kranix-io/kranix-core/internal/drift"
	"github.com/kranix-io/kranix-core/internal/eventbus"
	"github.com/kranix-io/kranix-core/internal/eventsourcing"
	"github.com/kranix-io/kranix-core/internal/healthgate"
	"github.com/kranix-io/kranix-core/internal/plugin"
	"github.com/kranix-io/kranix-core/internal/cronsched"
	"github.com/kranix-io/kranix-core/internal/policy"
	"github.com/kranix-io/kranix-core/internal/resourcequota"
	"github.com/kranix-io/kranix-core/internal/reconciler"
	"github.com/kranix-io/kranix-core/internal/rollout"
	"github.com/kranix-io/kranix-core/internal/scheduler"
	"github.com/kranix-io/kranix-core/internal/state"
	"github.com/kranix-io/kranix-core/internal/warmstandby"
	"github.com/kranix-io/kranix-core/pkg/types"
	"gopkg.in/yaml.v3"
)

// Config represents the application configuration.
type Config struct {
	Core struct {
		ReconcileInterval       time.Duration `yaml:"reconcile_interval"`
		MaxConcurrentReconciles int           `yaml:"max_concurrent_reconciles"`
	} `yaml:"core"`
	State struct {
		Backend       string   `yaml:"backend"` // memory | postgres | etcd
		PostgresDSN   string   `yaml:"postgres_dsn"`
		EtcdEndpoints []string `yaml:"etcd_endpoints"`
	} `yaml:"state"`
	Policy struct {
		DefaultCPULimit           string `yaml:"default_cpu_limit"`
		DefaultMemoryLimit        string `yaml:"default_memory_limit"`
		EnforceNamespaceIsolation bool   `yaml:"enforce_namespace_isolation"`
	} `yaml:"policy"`
	EventBus struct {
		BufferSize int `yaml:"buffer_size"`
	} `yaml:"eventbus"`
	DriftDetection struct {
		Enabled       bool          `yaml:"enabled"`
		CheckInterval time.Duration `yaml:"check_interval"`
	} `yaml:"drift_detection"`
	EventSourcing struct {
		Enabled        bool          `yaml:"enabled"`
		StorageBackend string        `yaml:"storage_backend"`
		MaxEventAge    time.Duration `yaml:"max_event_age"`
		Compression    bool          `yaml:"compression"`
	} `yaml:"event_sourcing"`
	HealthGate struct {
		Enabled        bool          `yaml:"enabled"`
		DefaultTimeout time.Duration `yaml:"default_timeout"`
		CheckInterval  time.Duration `yaml:"check_interval"`
	} `yaml:"health_gate"`
	ResourceQuota struct {
		HardLimits []types.HardResourceQuota `yaml:"hard_limits"`
	} `yaml:"resource_quota"`
	RollbackHistory struct {
		Enabled     bool `yaml:"enabled"`
		MaxVersions int  `yaml:"max_versions"`
	} `yaml:"rollback_history"`
	WorkloadTags struct {
		RequireTeam        bool `yaml:"require_team"`
		RequireEnvironment bool `yaml:"require_environment"`
		RequireCostCenter  bool `yaml:"require_cost_center"`
	} `yaml:"workload_tags"`
	CircuitBreaker struct {
		Enabled                    bool  `yaml:"enabled"`
		DefaultFailureThreshold    int32 `yaml:"default_failure_threshold"`
		DefaultSuccessThreshold    int32 `yaml:"default_success_threshold"`
		DefaultOpenDurationSeconds int32 `yaml:"default_open_duration_seconds"`
		DefaultHalfOpenMaxRequests int32 `yaml:"default_half_open_max_requests"`
	} `yaml:"circuit_breaker"`
	WarmStandby struct {
		Enabled                bool  `yaml:"enabled"`
		DefaultStandbyReplicas int32 `yaml:"default_standby_replicas"`
		DefaultAutoPromote     bool  `yaml:"default_auto_promote"`
	} `yaml:"warm_standby"`
}

func main() {
	configPath := flag.String("config", "./config/local.yaml", "Path to configuration file")
	flag.Parse()

	// Load configuration
	config, loadErr := loadConfig(*configPath)
	if loadErr != nil {
		log.Fatalf("Failed to load config: %v", loadErr)
	}

	// Initialize components
	eventBus := eventbus.New(config.EventBus.BufferSize)

	// Initialize state store based on backend configuration
	var store state.Store
	var storeErr error

	switch config.State.Backend {
	case "postgres":
		if config.State.PostgresDSN == "" {
			log.Fatalf("Postgres DSN is required when using postgres backend")
		}
		store, storeErr = state.NewPostgresStore(config.State.PostgresDSN)
		if storeErr != nil {
			log.Fatalf("Failed to create postgres store: %v", storeErr)
		}
	case "etcd":
		if len(config.State.EtcdEndpoints) == 0 {
			log.Fatalf("Etcd endpoints are required when using etcd backend")
		}
		store, storeErr = state.NewEtcdStore(config.State.EtcdEndpoints)
		if storeErr != nil {
			log.Fatalf("Failed to create etcd store: %v", storeErr)
		}
	case "memory":
		fallthrough
	default:
		// Initialize event sourcing store for memory backend
		eventStore := eventsourcing.New(eventsourcing.Config{
			Enabled:        config.EventSourcing.Enabled,
			StorageBackend: config.EventSourcing.StorageBackend,
			MaxEventAge:    config.EventSourcing.MaxEventAge,
			Compression:    config.EventSourcing.Compression,
		}, eventBus)
		store = state.NewMemoryStore(eventStore)
	}

	if config.RollbackHistory.Enabled {
		maxV := config.RollbackHistory.MaxVersions
		if maxV <= 0 {
			maxV = 10
		}
		store = state.NewVersionedStore(store, maxV)
	}

	// Initialize health gate engine
	healthGateEngine := healthgate.New(healthgate.Config{
		Enabled:        config.HealthGate.Enabled,
		DefaultTimeout: config.HealthGate.DefaultTimeout,
		CheckInterval:  config.HealthGate.CheckInterval,
	}, eventBus)

	// Initialize cost provider and node registry for scheduler
	costProvider := &scheduler.DefaultCostProvider{}
	nodeRegistry := &scheduler.DefaultNodeRegistry{}
	sched := scheduler.New(costProvider, nodeRegistry)

	// Initialize rollout manager and autoscaler
	rolloutManager := rollout.New(store, sched, eventBus, healthGateEngine)
	metricsProvider := &autoscaler.DefaultMetricsProvider{}
	autoscalerConfig := autoscaler.Config{CheckInterval: 30 * time.Second}
	autoscalerEngine := autoscaler.New(autoscalerConfig, store, metricsProvider)

	controllerReg := plugin.NewRegistry()
	policyEngine := policy.New(policy.Config{
		DefaultCPULimit:           config.Policy.DefaultCPULimit,
		DefaultMemoryLimit:        config.Policy.DefaultMemoryLimit,
		EnforceNamespaceIsolation: config.Policy.EnforceNamespaceIsolation,
		RequireTeamTag:            config.WorkloadTags.RequireTeam,
		RequireEnvironmentTag:     config.WorkloadTags.RequireEnvironment,
		RequireCostCenterTag:      config.WorkloadTags.RequireCostCenter,
	})
	var quotaEngine *resourcequota.Engine
	if len(config.ResourceQuota.HardLimits) > 0 {
		quotaEngine = resourcequota.New(store, config.ResourceQuota.HardLimits)
	}
	cronEval := &cronsched.Evaluator{}

	circuitEngine := circuitbreaker.New(circuitbreaker.Config{
		Enabled:                    config.CircuitBreaker.Enabled,
		DefaultFailureThreshold:    config.CircuitBreaker.DefaultFailureThreshold,
		DefaultSuccessThreshold:    config.CircuitBreaker.DefaultSuccessThreshold,
		DefaultOpenDurationSeconds: config.CircuitBreaker.DefaultOpenDurationSeconds,
		DefaultHalfOpenMaxRequests: config.CircuitBreaker.DefaultHalfOpenMaxRequests,
	})
	standbyManager := warmstandby.New(warmstandby.Config{
		Enabled:                config.WarmStandby.Enabled,
		DefaultStandbyReplicas: config.WarmStandby.DefaultStandbyReplicas,
		DefaultAutoPromote:     config.WarmStandby.DefaultAutoPromote,
	}, circuitEngine)

	// Initialize drift detector
	var driftDetector *drift.Detector
	if config.DriftDetection.Enabled {
		driftDetector = drift.New(drift.Config{
			Enabled:       config.DriftDetection.Enabled,
			CheckInterval: config.DriftDetection.CheckInterval,
			DefaultTolerance: &types.DriftTolerance{
				ReplicaVariance:     1,
				ResourceVariancePct: 10.0,
				EnvVarDriftAllowed:  false,
				LabelDriftAllowed:   true,
			},
		}, eventBus, nil) // Runtime driver will be injected later
	}

	// Create reconciler engine
	reconcilerEngine := reconciler.New(
		reconciler.Config{
			ReconcileInterval:       config.Core.ReconcileInterval,
			MaxConcurrentReconciles: config.Core.MaxConcurrentReconciles,
		},
		store,
		eventBus,
		sched,
		controllerReg,
		rolloutManager,
		autoscalerEngine,
		driftDetector,
		reconciler.Deps{
			Policy:  policyEngine,
			Quota:   quotaEngine,
			Cron:    cronEval,
			Circuit: circuitEngine,
			Standby: standbyManager,
		},
	)

	// Setup context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start reconciler
	go reconcilerEngine.Start(ctx)

	// Setup signal handling for graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Wait for shutdown signal
	<-sigCh
	log.Println("Shutting down...")

	// Stop reconciler
	reconcilerEngine.Stop()

	log.Println("Shutdown complete")
}

// loadConfig reads and parses the configuration file.
func loadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, err
	}

	return &config, nil
}
