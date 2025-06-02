import pytest
import duckdb
from pathlib import Path
import time
import uuid
import asyncio # Required for async tests

from src.shared.job_manager import JobManager, JobStatus, Job
# Assuming load_sql is in src.shared.utils.sql_loader
from src.shared.utils.sql_loader import load_sql as load_jobs_sql

@pytest.fixture(scope='function')
def memory_db_conn_jobs():
    conn = duckdb.connect(':memory:')
    # Corrected path assuming operations/ is at src/infrastructure/operations/
    sql_queries = load_jobs_sql(Path("src/infrastructure/operations/jobs.sql"))
    conn.execute(sql_queries['create_job_status_table'])
    yield conn
    conn.close()

@pytest.fixture
def job_manager(memory_db_conn_jobs):
    # JobManager's __init__ already calls _initialize_db which includes mark_stale_jobs_as_failed
    return JobManager(db_conn=memory_db_conn_jobs)

@pytest.mark.asyncio
async def test_create_job_success(job_manager):
    job_type = "test_import"
    job_id = await job_manager.create_job(job_type)
    assert job_id is not None

    # Retrieve directly from DB to check raw values if Job.from_row has issues or for direct validation
    job_row = job_manager.db_conn.execute("SELECT * FROM job_status WHERE job_id = ?", [job_id]).fetchone()
    assert job_row is not None
    # Assuming column order: job_id, job_type, status, progress, total_records, new_records, duplicate_records, error_message, created_at, updated_at, started_at, completed_at
    assert job_row[0] == job_id
    assert job_row[1] == job_type
    assert job_row[2] == JobStatus.PENDING.value # Status stored as string

    # Test get_job_status
    status_obj = await job_manager.get_job_status(job_id)
    assert status_obj is not None
    assert status_obj.job_id == job_id
    assert status_obj.job_type == job_type
    assert status_obj.status == JobStatus.PENDING


@pytest.mark.asyncio
async def test_create_job_fails_if_active(job_manager):
    job_type = "concurrent_test"
    job_id1 = await job_manager.create_job(job_type)
    assert job_id1 is not None

    # Try to create another job of the same type while first is pending
    job_id2 = await job_manager.create_job(job_type)
    assert job_id2 is None

    # Mark first job as running
    await job_manager.update_job(job_id1, status=JobStatus.RUNNING, progress=10.0)
    job_id3 = await job_manager.create_job(job_type)
    assert job_id3 is None

@pytest.mark.asyncio
async def test_update_and_get_job(job_manager):
    job_type = "update_test"
    job_id = await job_manager.create_job(job_type)
    assert job_id is not None

    current_time_for_test = datetime.now() # For started_at, completed_at

    await job_manager.update_job(job_id, status=JobStatus.RUNNING, progress=50.0, total_records=100, started_at=current_time_for_test)
    status1 = await job_manager.get_job_status(job_id)
    assert status1 is not None
    assert status1.status == JobStatus.RUNNING
    assert status1.progress == 50.0
    assert status1.total_records == 100
    assert status1.started_at is not None # Check it's set

    await job_manager.update_job(job_id, status=JobStatus.COMPLETED, progress=100.0, new_records=80, duplicate_records=20, completed_at=current_time_for_test)
    status2 = await job_manager.get_job_status(job_id)
    assert status2 is not None
    assert status2.status == JobStatus.COMPLETED
    assert status2.new_records == 80
    assert status2.duplicate_records == 20
    assert status2.completed_at is not None

@pytest.mark.asyncio
async def test_get_non_existent_job(job_manager):
    status = await job_manager.get_job_status(str(uuid.uuid4()))
    assert status is None

# This test needs to be synchronous if JobManager init is sync, or manage event loop if async parts are called
def test_mark_stale_jobs(memory_db_conn_jobs): # Removed job_manager fixture to control instantiation
    # Simulate a stale job
    stale_job_id = str(uuid.uuid4())
    job_type = "stale_test"
    # Manually insert a job that appears stale
    memory_db_conn_jobs.execute(
        "INSERT INTO job_status (job_id, job_type, status, created_at, updated_at, started_at) VALUES (?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
        [stale_job_id, job_type, JobStatus.RUNNING.value] # Stored as string value
    )

    # Re-initialize JobManager to trigger startup reconciliation (_initialize_db)
    new_job_manager = JobManager(db_conn=memory_db_conn_jobs) # This call runs _initialize_db

    # Retrieve the job status using the new manager instance or direct DB query
    status_row = memory_db_conn_jobs.execute("SELECT status, error_message FROM job_status WHERE job_id = ?", [stale_job_id]).fetchone()
    assert status_row is not None
    assert status_row[0] == JobStatus.FAILED.value # Check string value
    assert "Job was marked as failed due to application restart" in status_row[1]

# Added import for datetime for test_update_and_get_job
from datetime import datetime
```
