import time
import uuid # For generating unique job IDs
from enum import Enum
from typing import Dict, Optional, Any, Mapping # Added Any, Mapping
from dataclasses import dataclass, fields # Added fields
import asyncio
import logging
import duckdb # Added duckdb
from pathlib import Path # Added Path

from ..shared.utils.sql_loader import load_sql # Added load_sql

logger = logging.getLogger(__name__)

class JobStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    # TIMEOUT is essentially a FAILED status with a specific error message.
    # We can simplify by using FAILED and setting an appropriate error message.

@dataclass
class Job: # This dataclass can represent the structure in the DB
    job_id: str # Changed from id to job_id to match DB
    job_type: str
    status: JobStatus
    created_at: Any # duckdb returns datetime, not float
    updated_at: Any # duckdb returns datetime, not float
    started_at: Optional[Any] = None
    completed_at: Optional[Any] = None
    progress: float = 0.0 # Changed from int to float
    total_records: Optional[int] = None
    new_records: Optional[int] = None
    duplicate_records: Optional[int] = None
    error_message: Optional[str] = None # Changed from error to error_message

    # Helper to convert a DuckDB row (tuple) to a Job object
    @classmethod
    def from_row(cls, row: tuple, column_names: list) -> "Job":
        row_dict = dict(zip(column_names, row))
        # Convert status string from DB to JobStatus enum
        if 'status' in row_dict and isinstance(row_dict['status'], str):
            row_dict['status'] = JobStatus(row_dict['status'])

        # Ensure all dataclass fields are present, defaulting if necessary
        # This is important if some DB columns might be null and not directly in row_dict
        # or if there's a mismatch in optional fields.
        # However, with controlled INSERT/UPDATEs, row_dict should have all necessary keys.
        # For safety, one could iterate cls.__dataclass_fields__ and get from row_dict with defaults.

        # Filter out keys not in the dataclass definition to prevent errors
        valid_keys = {f.name for f in fields(cls)}
        filtered_row_dict = {k: v for k, v in row_dict.items() if k in valid_keys}

        return cls(**filtered_row_dict)


class JobManager:
    """
    Manages the lifecycle of background jobs (e.g., data imports).
    It uses a DuckDB database connection to persist job statuses, ensuring that
    job information is not lost across application restarts and providing a
    centralized way to track job progress and outcomes.
    """
    def __init__(self, db_conn: duckdb.DuckDBPyConnection):
        self.db_conn = db_conn
        self._lock = asyncio.Lock() # To protect against concurrent job creation attempts for the same type

        # Load SQL queries from the jobs.sql file.
        sql_path = Path(__file__).parent.parent / "infrastructure" / "operations" / "jobs.sql"
        self.queries = load_sql(str(sql_path))

        # Initialize database schema: create the job_status table if it doesn't exist
        # and reconcile stale jobs (e.g., mark jobs that were 'running' as 'failed'
        # if the application restarted before they completed).
        self._initialize_db()

    def _initialize_db(self):
        """Creates the job status table if it doesn't exist and marks stale jobs."""
        try:
            self.db_conn.execute(self.queries['create_job_status_table'])
            logger.info("Job status table initialized.")

            # Mark stale jobs (those in 'running' state) as 'failed'
            # This handles cases where the application might have crashed.
            result = self.db_conn.execute(self.queries['mark_stale_jobs_as_failed'])
            if result.fetchnone() is not None and hasattr(result, 'rowcount') and result.rowcount > 0 : # DuckDB specific check for changes
                 logger.info(f"Marked {result.rowcount} stale jobs as failed.")
            else: # Attempt to get rowcount for DuckDB versions that support it or check changes another way
                # DuckDB's execute often returns a relation object.
                # For UPDATE, INSERT, DELETE, the number of affected rows is not directly returned by execute()
                # but can be inferred or might be available in newer versions through specific methods.
                # For now, we'll assume the query ran and log its intent.
                # A more robust way would be to SELECT COUNT(*) WHERE status = 'running' before and after.
                logger.info("Checked for stale jobs to mark as failed.")

        except Exception as e:
            logger.error(f"Error initializing job database: {e}", exc_info=True)
            # Depending on the severity, we might want to raise this
            # to prevent the application from starting with a non-functional JobManager.

    async def create_job(self, job_type: str) -> Optional[str]:
        """
        Create a new job if no active (pending or running) job of the same type exists.
        The job_type parameter is used for concurrency control, ensuring that only one
        job of a specific type (e.g., 'well_import', 'odata_import') can be active at a time.
        """
        async with self._lock:
            try:
                # Check for existing active job of this type to prevent duplicates.
                active_job_row = self.db_conn.execute(self.queries['get_active_job_by_type'], [job_type]).fetchone()
                if active_job_row:
                    logger.warning(f"An active job of type '{job_type}' already exists (ID: {active_job_row[0]}). Cannot create new job.")
                    return None # Or raise an exception

                job_id = str(uuid.uuid4())
                initial_status = JobStatus.PENDING.value

                self.db_conn.execute(self.queries['insert_job'], [job_id, job_type, initial_status])
                logger.info(f"Created new job {job_id} of type {job_type} with status {initial_status}.")
                return job_id
            except Exception as e:
                logger.error(f"Error creating job of type {job_type}: {e}", exc_info=True)
                return None # Or re-raise as an application-specific exception

    async def update_job(
        self,
        job_id: str,
        status: JobStatus,
        progress: Optional[float] = None,
        total_records: Optional[int] = None,
        new_records: Optional[int] = None,
        duplicate_records: Optional[int] = None,
        error_message: Optional[str] = None,
        started_at: Optional[Any] = None, # Pass datetime objects if specific start time is needed
        completed_at: Optional[Any] = None # Pass datetime objects
    ):
        """Update job status and details in the database."""
        try:
            # For started_at, we only want to set it if the status is RUNNING and started_at is not already set.
            # The COALESCE in SQL handles this if `None` is passed when it shouldn't be updated.
            # If status is RUNNING and current started_at is None, we might pass datetime.now() or rely on DB.
            # For this implementation, we'll let the caller pass an explicit started_at if needed.
            # If it's the first time moving to RUNNING, started_at should be set.
            # If status is COMPLETED or FAILED, completed_at should be set.
            
            # The SQL query 'update_job_details' expects parameters in a specific order:
            # status, progress, total_records, new_records, duplicate_records,
            # error_message, started_at, completed_at, job_id
            
            params = [
                status.value,
                progress,
                total_records,
                new_records,
                duplicate_records,
                error_message,
                started_at, # Will be passed as COALESCE(?, started_at) in SQL
                completed_at,
                job_id
            ]
            self.db_conn.execute(self.queries['update_job_details'], params)
            logger.debug(f"Updated job {job_id}: status={status.value}, progress={progress}")

        except Exception as e:
            logger.error(f"Error updating job {job_id}: {e}", exc_info=True)
            # Handle error, perhaps re-raise or log

    async def get_job_status(self, job_id: str) -> Optional[Job]:
        """Get job by ID from the database."""
        try:
            result = self.db_conn.execute(self.queries['get_job_by_id'], [job_id]).fetchone()
            if result:
                # Get column names from the cursor description (if available and simple)
                # Or assume a fixed order / list of names.
                # For DuckDB, description provides (name, type_code, display_size, internal_size, precision, scale, null_ok)
                column_names = [desc[0] for desc in self.db_conn.description]
                return Job.from_row(result, column_names)
            return None
        except Exception as e:
            logger.error(f"Error getting job {job_id}: {e}", exc_info=True)
            return None