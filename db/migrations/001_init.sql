CREATE TABLE IF NOT EXISTS jobs (
  id UUID PRIMARY KEY,
  name TEXT NOT NULL,
  status TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS job_nodes (
  job_id UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
  node_id TEXT NOT NULL,
  name TEXT NOT NULL,
  command TEXT NOT NULL,
  retries INTEGER NOT NULL DEFAULT 0,
  timeout_seconds INTEGER NOT NULL DEFAULT 0,
  idempotency_key TEXT,
  status TEXT NOT NULL,
  attempt INTEGER NOT NULL DEFAULT 0,
  started_at TIMESTAMPTZ,
  completed_at TIMESTAMPTZ,
  next_run_at TIMESTAMPTZ,
  last_output TEXT,
  PRIMARY KEY (job_id, node_id),
  UNIQUE (job_id, idempotency_key)
);

CREATE TABLE IF NOT EXISTS job_node_deps (
  job_id UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
  node_id TEXT NOT NULL,
  depends_on TEXT NOT NULL,
  PRIMARY KEY (job_id, node_id, depends_on)
);

CREATE INDEX IF NOT EXISTS idx_job_nodes_status ON job_nodes(job_id, status);
CREATE INDEX IF NOT EXISTS idx_job_nodes_next_run ON job_nodes(next_run_at);
