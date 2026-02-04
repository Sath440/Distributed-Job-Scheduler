package store

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	StatusPending   = "pending"
	StatusRunning   = "running"
	StatusSuccess   = "success"
	StatusFailed    = "failed"
	StatusRetryWait = "retry_wait"
)

type Store struct {
	pool *pgxpool.Pool
}

type Job struct {
	ID        string
	Name      string
	Status    string
	CreatedAt time.Time
}

type Node struct {
	JobID          string     `json:"job_id"`
	NodeID         string     `json:"node_id"`
	Name           string     `json:"name"`
	Command        string     `json:"command"`
	Retries        int32      `json:"retries"`
	TimeoutSeconds int32      `json:"timeout_seconds"`
	IdempotencyKey string     `json:"idempotency_key"`
	Status         string     `json:"status"`
	Attempt        int32      `json:"attempt"`
	StartedAt      *time.Time `json:"started_at"`
	CompletedAt    *time.Time `json:"completed_at"`
	NextRunAt      *time.Time `json:"next_run_at"`
	LastOutput     string     `json:"last_output"`
}

func New(ctx context.Context, url string) (*Store, error) {
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, err
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Close() {
	s.pool.Close()
}

func (s *Store) CreateJob(ctx context.Context, name string) (string, error) {
	jobID := uuid.NewString()
	_, err := s.pool.Exec(ctx, `INSERT INTO jobs (id, name, status) VALUES ($1, $2, $3)`, jobID, name, StatusPending)
	if err != nil {
		return "", err
	}
	return jobID, nil
}

func (s *Store) InsertNode(ctx context.Context, jobID string, node Node) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO job_nodes (job_id, node_id, name, command, retries, timeout_seconds, idempotency_key, status, attempt)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		jobID, node.NodeID, node.Name, node.Command, node.Retries, node.TimeoutSeconds, node.IdempotencyKey, StatusPending, 0,
	)
	return err
}

func (s *Store) InsertDependency(ctx context.Context, jobID, nodeID, dependsOn string) error {
	_, err := s.pool.Exec(ctx, `INSERT INTO job_node_deps (job_id, node_id, depends_on) VALUES ($1, $2, $3)`, jobID, nodeID, dependsOn)
	return err
}

func (s *Store) ReadyNodeIDs(ctx context.Context, jobID string) ([]string, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT n.node_id
		FROM job_nodes n
		WHERE n.job_id = $1
		AND n.status = $2
		AND NOT EXISTS (
			SELECT 1 FROM job_node_deps d
			JOIN job_nodes dn ON dn.job_id = d.job_id AND dn.node_id = d.depends_on
			WHERE d.job_id = n.job_id AND d.node_id = n.node_id AND dn.status <> $3
		)
	`, jobID, StatusPending, StatusSuccess)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (s *Store) TryStartNode(ctx context.Context, jobID, nodeID string) (*Node, bool, error) {
	row := s.pool.QueryRow(ctx, `
		UPDATE job_nodes
		SET status = $3, attempt = attempt + 1, started_at = NOW(), next_run_at = NULL
		WHERE job_id = $1 AND node_id = $2
		AND status IN ($4, $5)
		AND (next_run_at IS NULL OR next_run_at <= NOW())
		RETURNING job_id, node_id, name, command, retries, timeout_seconds, idempotency_key, status, attempt
	`, jobID, nodeID, StatusRunning, StatusPending, StatusRetryWait)

	var node Node
	if err := row.Scan(&node.JobID, &node.NodeID, &node.Name, &node.Command, &node.Retries, &node.TimeoutSeconds, &node.IdempotencyKey, &node.Status, &node.Attempt); err != nil {
		return nil, false, nil
	}
	return &node, true, nil
}

func (s *Store) UpdateNodeResult(ctx context.Context, jobID, nodeID, status, output string, completedAt time.Time, nextRunAt *time.Time) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE job_nodes
		SET status = $3,
			completed_at = $4,
			last_output = $5,
			next_run_at = $6
		WHERE job_id = $1 AND node_id = $2
	`, jobID, nodeID, status, completedAt, output, nextRunAt)
	return err
}

func (s *Store) FetchNode(ctx context.Context, jobID, nodeID string) (*Node, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT job_id, node_id, name, command, retries, timeout_seconds, idempotency_key, status, attempt, started_at, completed_at, next_run_at, last_output
		FROM job_nodes
		WHERE job_id = $1 AND node_id = $2
	`, jobID, nodeID)
	var node Node
	if err := row.Scan(&node.JobID, &node.NodeID, &node.Name, &node.Command, &node.Retries, &node.TimeoutSeconds, &node.IdempotencyKey, &node.Status, &node.Attempt, &node.StartedAt, &node.CompletedAt, &node.NextRunAt, &node.LastOutput); err != nil {
		return nil, err
	}
	return &node, nil
}

func (s *Store) FetchJobNodes(ctx context.Context, jobID string) ([]Node, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT job_id, node_id, name, command, retries, timeout_seconds, idempotency_key, status, attempt, started_at, completed_at, next_run_at, last_output
		FROM job_nodes
		WHERE job_id = $1
		ORDER BY node_id
	`, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var nodes []Node
	for rows.Next() {
		var node Node
		if err := rows.Scan(&node.JobID, &node.NodeID, &node.Name, &node.Command, &node.Retries, &node.TimeoutSeconds, &node.IdempotencyKey, &node.Status, &node.Attempt, &node.StartedAt, &node.CompletedAt, &node.NextRunAt, &node.LastOutput); err != nil {
			return nil, err
		}
		nodes = append(nodes, node)
	}
	return nodes, rows.Err()
}

func (s *Store) UpdateJobStatus(ctx context.Context, jobID, status string) error {
	_, err := s.pool.Exec(ctx, `UPDATE jobs SET status = $2 WHERE id = $1`, jobID, status)
	return err
}

func (s *Store) JobStatus(ctx context.Context, jobID string) (string, error) {
	row := s.pool.QueryRow(ctx, `SELECT status FROM jobs WHERE id = $1`, jobID)
	var status string
	if err := row.Scan(&status); err != nil {
		return "", err
	}
	return status, nil
}

func (s *Store) ListRetryDueNodes(ctx context.Context) ([]Node, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT job_id, node_id, name, command, retries, timeout_seconds, idempotency_key, status, attempt
		FROM job_nodes
		WHERE status = $1 AND next_run_at IS NOT NULL AND next_run_at <= NOW()
	`, StatusRetryWait)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var nodes []Node
	for rows.Next() {
		var node Node
		if err := rows.Scan(&node.JobID, &node.NodeID, &node.Name, &node.Command, &node.Retries, &node.TimeoutSeconds, &node.IdempotencyKey, &node.Status, &node.Attempt); err != nil {
			return nil, err
		}
		nodes = append(nodes, node)
	}
	return nodes, rows.Err()
}

func (s *Store) ListStuckRunningNodes(ctx context.Context) ([]Node, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT job_id, node_id, name, command, retries, timeout_seconds, idempotency_key, status, attempt, started_at
		FROM job_nodes
		WHERE status = $1 AND started_at IS NOT NULL AND (started_at + (timeout_seconds || ' seconds')::interval) <= NOW()
	`, StatusRunning)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var nodes []Node
	for rows.Next() {
		var node Node
		if err := rows.Scan(&node.JobID, &node.NodeID, &node.Name, &node.Command, &node.Retries, &node.TimeoutSeconds, &node.IdempotencyKey, &node.Status, &node.Attempt, &node.StartedAt); err != nil {
			return nil, err
		}
		nodes = append(nodes, node)
	}
	return nodes, rows.Err()
}

func (s *Store) HasJobFailed(ctx context.Context, jobID string) (bool, error) {
	row := s.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM job_nodes WHERE job_id = $1 AND status = $2)`, jobID, StatusFailed)
	var exists bool
	if err := row.Scan(&exists); err != nil {
		return false, err
	}
	return exists, nil
}

func (s *Store) HasJobSucceeded(ctx context.Context, jobID string) (bool, error) {
	row := s.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM job_nodes WHERE job_id = $1 AND status <> $2)`, jobID, StatusSuccess)
	var pending bool
	if err := row.Scan(&pending); err != nil {
		return false, err
	}
	return !pending, nil
}

func (s *Store) MarkJobIfComplete(ctx context.Context, jobID string) error {
	failed, err := s.HasJobFailed(ctx, jobID)
	if err != nil {
		return err
	}
	if failed {
		return s.UpdateJobStatus(ctx, jobID, StatusFailed)
	}
	complete, err := s.HasJobSucceeded(ctx, jobID)
	if err != nil {
		return err
	}
	if complete {
		return s.UpdateJobStatus(ctx, jobID, StatusSuccess)
	}
	return nil
}

func (s *Store) VerifyNodeExists(ctx context.Context, jobID, nodeID string) error {
	row := s.pool.QueryRow(ctx, `SELECT 1 FROM job_nodes WHERE job_id = $1 AND node_id = $2`, jobID, nodeID)
	var val int
	if err := row.Scan(&val); err != nil {
		return errors.New("node not found")
	}
	return nil
}
