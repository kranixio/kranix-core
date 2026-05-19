package state

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"

	_ "github.com/lib/pq"

	"github.com/kranix-io/kranix-core/pkg/types"
)

// PostgresStore provides a PostgreSQL implementation of Store.
type PostgresStore struct {
	db       *sql.DB
	mu       sync.RWMutex
	watchers []chan types.Event
	muWatchers sync.RWMutex
}

// NewPostgresStore creates a new PostgreSQL state store.
func NewPostgresStore(dsn string) (*PostgresStore, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to postgres: %w", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping postgres: %w", err)
	}

	// Create tables
	if err := createTables(db); err != nil {
		return nil, fmt.Errorf("failed to create tables: %w", err)
	}

	return &PostgresStore{
		db:       db,
		watchers: make([]chan types.Event, 0),
	}, nil
}

// createTables creates the necessary tables in PostgreSQL.
func createTables(db *sql.DB) error {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS workloads (
			id VARCHAR(255) PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			namespace VARCHAR(255),
			labels JSONB,
			spec JSONB NOT NULL,
			status JSONB NOT NULL,
			history JSONB,
			tenant JSONB,
			created_at TIMESTAMP NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMP NOT NULL DEFAULT NOW()
		)`,
		`CREATE INDEX IF NOT EXISTS idx_workloads_namespace ON workloads(namespace)`,
		`CREATE INDEX IF NOT EXISTS idx_workloads_labels ON workloads USING GIN(labels)`,
		`ALTER TABLE workloads ADD COLUMN IF NOT EXISTS rollback_versions JSONB`,
		`ALTER TABLE workloads ADD COLUMN IF NOT EXISTS tags JSONB`,
	}

	for _, query := range queries {
		if _, err := db.Exec(query); err != nil {
			return fmt.Errorf("failed to execute query: %w", err)
		}
	}

	return nil
}

// Get retrieves a workload by ID.
func (s *PostgresStore) Get(ctx context.Context, id string) (*types.Workload, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var workload types.Workload
	var labelsJSON, historyJSON, tenantJSON, rollbackJSON, tagsJSON []byte

	query := `SELECT id, name, namespace, labels, spec, status, history, tenant, rollback_versions, tags, created_at, updated_at 
			  FROM workloads WHERE id = $1`

	err := s.db.QueryRowContext(ctx, query, id).Scan(
		&workload.ID,
		&workload.Name,
		&workload.Namespace,
		&labelsJSON,
		&workload.Spec,
		&workload.Status,
		&historyJSON,
		&tenantJSON,
		&rollbackJSON,
		&tagsJSON,
		&workload.CreatedAt,
		&workload.UpdatedAt,
	)

	if err == sql.ErrNoRows {
		return nil, ErrWorkloadNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed to query workload: %w", err)
	}

	if len(labelsJSON) > 0 {
		if err := json.Unmarshal(labelsJSON, &workload.Labels); err != nil {
			return nil, fmt.Errorf("failed to unmarshal labels: %w", err)
		}
	}

	if len(historyJSON) > 0 {
		if err := json.Unmarshal(historyJSON, &workload.History); err != nil {
			return nil, fmt.Errorf("failed to unmarshal history: %w", err)
		}
	}

	if len(tenantJSON) > 0 {
		if err := json.Unmarshal(tenantJSON, &workload.Tenant); err != nil {
			return nil, fmt.Errorf("failed to unmarshal tenant: %w", err)
		}
	}
	if len(rollbackJSON) > 0 {
		if err := json.Unmarshal(rollbackJSON, &workload.RollbackVersions); err != nil {
			return nil, fmt.Errorf("failed to unmarshal rollback_versions: %w", err)
		}
	}
	if len(tagsJSON) > 0 {
		if err := json.Unmarshal(tagsJSON, &workload.Tags); err != nil {
			return nil, fmt.Errorf("failed to unmarshal tags: %w", err)
		}
	}

	return &workload, nil
}

// List retrieves all workloads, optionally filtered by namespace.
func (s *PostgresStore) List(ctx context.Context, namespace string) ([]*types.Workload, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var query string
	var args []interface{}

	if namespace != "" {
		query = `SELECT id, name, namespace, labels, spec, status, history, tenant, rollback_versions, tags, created_at, updated_at 
				 FROM workloads WHERE namespace = $1 ORDER BY created_at DESC`
		args = []interface{}{namespace}
	} else {
		query = `SELECT id, name, namespace, labels, spec, status, history, tenant, rollback_versions, tags, created_at, updated_at 
				 FROM workloads ORDER BY created_at DESC`
	}

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query workloads: %w", err)
	}
	defer rows.Close()

	var workloads []*types.Workload

	for rows.Next() {
		var workload types.Workload
		var labelsJSON, historyJSON, tenantJSON, rollbackJSON, tagsJSON []byte

		err := rows.Scan(
			&workload.ID,
			&workload.Name,
			&workload.Namespace,
			&labelsJSON,
			&workload.Spec,
			&workload.Status,
			&historyJSON,
			&tenantJSON,
			&rollbackJSON,
			&tagsJSON,
			&workload.CreatedAt,
			&workload.UpdatedAt,
		)

		if err != nil {
			return nil, fmt.Errorf("failed to scan workload: %w", err)
		}

		if len(labelsJSON) > 0 {
			if err := json.Unmarshal(labelsJSON, &workload.Labels); err != nil {
				return nil, fmt.Errorf("failed to unmarshal labels: %w", err)
			}
		}

		if len(historyJSON) > 0 {
			if err := json.Unmarshal(historyJSON, &workload.History); err != nil {
				return nil, fmt.Errorf("failed to unmarshal history: %w", err)
			}
		}

		if len(tenantJSON) > 0 {
			if err := json.Unmarshal(tenantJSON, &workload.Tenant); err != nil {
				return nil, fmt.Errorf("failed to unmarshal tenant: %w", err)
			}
		}
		if len(rollbackJSON) > 0 {
			if err := json.Unmarshal(rollbackJSON, &workload.RollbackVersions); err != nil {
				return nil, fmt.Errorf("failed to unmarshal rollback_versions: %w", err)
			}
		}
		if len(tagsJSON) > 0 {
			if err := json.Unmarshal(tagsJSON, &workload.Tags); err != nil {
				return nil, fmt.Errorf("failed to unmarshal tags: %w", err)
			}
		}

		workloads = append(workloads, &workload)
	}

	return workloads, nil
}

// Create stores a new workload.
func (s *PostgresStore) Create(ctx context.Context, workload *types.Workload) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	labelsJSON, err := json.Marshal(workload.Labels)
	if err != nil {
		return fmt.Errorf("failed to marshal labels: %w", err)
	}

	historyJSON, err := json.Marshal(workload.History)
	if err != nil {
		return fmt.Errorf("failed to marshal history: %w", err)
	}

	tenantJSON, err := json.Marshal(workload.Tenant)
	if err != nil {
		return fmt.Errorf("failed to marshal tenant: %w", err)
	}

	rollbackJSON, err := json.Marshal(workload.RollbackVersions)
	if err != nil {
		return fmt.Errorf("failed to marshal rollback_versions: %w", err)
	}

	tagsJSON, err := json.Marshal(workload.Tags)
	if err != nil {
		return fmt.Errorf("failed to marshal tags: %w", err)
	}

	query := `INSERT INTO workloads (id, name, namespace, labels, spec, status, history, tenant, rollback_versions, tags, created_at, updated_at)
			  VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`

	_, err = s.db.ExecContext(ctx, query,
		workload.ID,
		workload.Name,
		workload.Namespace,
		labelsJSON,
		workload.Spec,
		workload.Status,
		historyJSON,
		tenantJSON,
		rollbackJSON,
		tagsJSON,
		workload.CreatedAt,
		workload.UpdatedAt,
	)

	if err != nil {
		return fmt.Errorf("failed to create workload: %w", err)
	}

	s.notify(types.Event{
		Type:     types.WorkloadDeployRequested,
		Workload: workload,
	})

	return nil
}

// Update updates an existing workload.
func (s *PostgresStore) Update(ctx context.Context, workload *types.Workload) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	labelsJSON, err := json.Marshal(workload.Labels)
	if err != nil {
		return fmt.Errorf("failed to marshal labels: %w", err)
	}

	historyJSON, err := json.Marshal(workload.History)
	if err != nil {
		return fmt.Errorf("failed to marshal history: %w", err)
	}

	tenantJSON, err := json.Marshal(workload.Tenant)
	if err != nil {
		return fmt.Errorf("failed to marshal tenant: %w", err)
	}

	rollbackJSON, err := json.Marshal(workload.RollbackVersions)
	if err != nil {
		return fmt.Errorf("failed to marshal rollback_versions: %w", err)
	}

	tagsJSON, err := json.Marshal(workload.Tags)
	if err != nil {
		return fmt.Errorf("failed to marshal tags: %w", err)
	}

	query := `UPDATE workloads 
			  SET name = $2, namespace = $3, labels = $4, spec = $5, status = $6, 
			      history = $7, tenant = $8, rollback_versions = $9, tags = $10, updated_at = $11
			  WHERE id = $1`

	result, err := s.db.ExecContext(ctx, query,
		workload.ID,
		workload.Name,
		workload.Namespace,
		labelsJSON,
		workload.Spec,
		workload.Status,
		historyJSON,
		tenantJSON,
		rollbackJSON,
		tagsJSON,
		workload.UpdatedAt,
	)

	if err != nil {
		return fmt.Errorf("failed to update workload: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		return ErrWorkloadNotFound
	}

	s.notify(types.Event{
		Type:     types.WorkloadUpdated,
		Workload: workload,
	})

	return nil
}

// Delete removes a workload.
func (s *PostgresStore) Delete(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Get workload for notification
	workload, err := s.Get(ctx, id)
	if err != nil {
		return err
	}

	query := `DELETE FROM workloads WHERE id = $1`

	result, err := s.db.ExecContext(ctx, query, id)
	if err != nil {
		return fmt.Errorf("failed to delete workload: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		return ErrWorkloadNotFound
	}

	s.notify(types.Event{
		Type:     types.WorkloadDeleted,
		Workload: workload,
	})

	return nil
}

// Watch returns a channel that emits workload change events.
func (s *PostgresStore) Watch(ctx context.Context) (<-chan types.Event, error) {
	s.muWatchers.Lock()
	defer s.muWatchers.Unlock()

	ch := make(chan types.Event, 100)
	s.watchers = append(s.watchers, ch)
	return ch, nil
}

// notify sends an event to all watchers.
func (s *PostgresStore) notify(event types.Event) {
	s.muWatchers.RLock()
	defer s.muWatchers.RUnlock()

	for _, ch := range s.watchers {
		select {
		case ch <- event:
		default:
			// Channel full, skip
		}
	}
}

// Close closes the database connection.
func (s *PostgresStore) Close() error {
	return s.db.Close()
}
