package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/kranix-io/kranix-core/internal/eventbus"
	"github.com/kranix-io/kranix-core/internal/plugin"
	"github.com/kranix-io/kranix-core/internal/policy"
	"github.com/kranix-io/kranix-core/internal/reconciler"
	"github.com/kranix-io/kranix-core/internal/scheduler"
	"github.com/kranix-io/kranix-core/internal/state"
	"gopkg.in/yaml.v3"
)

// Config represents the application configuration.
type Config struct {
	Core struct {
		ReconcileInterval       time.Duration `yaml:"reconcile_interval"`
		MaxConcurrentReconciles int           `yaml:"max_concurrent_reconciles"`
	} `yaml:"core"`
	State struct {
		Backend     string `yaml:"backend"`
		PostgresDSN string `yaml:"postgres_dsn"`
	} `yaml:"state"`
	Policy struct {
		DefaultCPULimit           string `yaml:"default_cpu_limit"`
		DefaultMemoryLimit        string `yaml:"default_memory_limit"`
		EnforceNamespaceIsolation bool   `yaml:"enforce_namespace_isolation"`
	} `yaml:"policy"`
	EventBus struct {
		BufferSize int `yaml:"buffer_size"`
	} `yaml:"eventbus"`
}

func main() {
	configPath := flag.String("config", "./config/local.yaml", "Path to configuration file")
	flag.Parse()

	// Load configuration
	config, err := loadConfig(*configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Initialize components
	store := state.NewMemoryStore()
	eventBus := eventbus.New(config.EventBus.BufferSize)
	scheduler := scheduler.New()
	controllerReg := plugin.NewRegistry()
	policyEngine := policy.New(policy.Config{
		DefaultCPULimit:           config.Policy.DefaultCPULimit,
		DefaultMemoryLimit:        config.Policy.DefaultMemoryLimit,
		EnforceNamespaceIsolation: config.Policy.EnforceNamespaceIsolation,
	})
	_ = policyEngine // TODO: integrate policy engine into reconciler

	// Create reconciler engine
	reconcilerEngine := reconciler.New(
		reconciler.Config{
			ReconcileInterval:       config.Core.ReconcileInterval,
			MaxConcurrentReconciles: config.Core.MaxConcurrentReconciles,
		},
		store,
		eventBus,
		scheduler,
		controllerReg,
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
