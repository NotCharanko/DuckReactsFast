-- name: create_job_status_table
CREATE TABLE IF NOT EXISTS job_status (
    job_id TEXT PRIMARY KEY,
    job_type TEXT NOT NULL, -- e.g., 'well_import', 'odata_import'
    status TEXT NOT NULL, -- 'pending', 'running', 'completed', 'failed'
    progress REAL DEFAULT 0.0,
    total_records INTEGER,
    new_records INTEGER,
    duplicate_records INTEGER,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMP,
    completed_at TIMESTAMP
);

-- name: insert_job
INSERT INTO job_status (job_id, job_type, status, created_at, updated_at)
VALUES (?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

-- name: update_job_details
UPDATE job_status SET
    status = ?,
    progress = ?,
    total_records = ?,
    new_records = ?,
    duplicate_records = ?,
    error_message = ?,
    started_at = COALESCE(?, started_at), -- Only update if new value is provided
    completed_at = ?,
    updated_at = CURRENT_TIMESTAMP
WHERE job_id = ?;

-- name: get_job_by_id
SELECT * FROM job_status WHERE job_id = ?;

-- name: get_active_job_by_type
SELECT * FROM job_status WHERE job_type = ? AND status IN ('pending', 'running');

-- name: mark_stale_jobs_as_failed
UPDATE job_status SET status = 'failed', error_message = 'Job was marked as failed due to application restart.', updated_at = CURRENT_TIMESTAMP
WHERE status = 'running';
