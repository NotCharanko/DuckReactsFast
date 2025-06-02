import logging
import asyncio # Ensure asyncio is imported
from typing import List, Optional, Dict, Any, Tuple, AsyncGenerator
import pandas as pd # Will be removed if validation fully moves to Polars
from dataclasses import asdict # Will be removed if validation fully moves to Polars
import polars as pl # Added for type hinting and direct use

from ...domain.entities.well_production import WellProduction
from ...domain.repositories.well_production_repository import WellProductionRepository
from ...domain.ports.external_api_port import ExternalApiPort
from ...shared.batch_processor import BatchProcessor, BatchResult
from ...shared.exceptions import (
    ValidationException,
    ApplicationException,
    ExternalApiException,
    BatchProcessingException
)
from ...shared.job_manager import JobManager
from ...infrastructure.adapters.external_api_adapter import ExternalApiAdapter
from ...shared.utils.timing_decorator import async_timed, timed # Added import

logger = logging.getLogger(__name__)

class WellProductionImportService:
    """
    Service for importing well production data.
    Handles fetching, validation, and batch insertion of data.
    """

    def __init__(
        self,
        external_api: ExternalApiAdapter,
        repository: WellProductionRepository,
        job_manager: JobManager,
        batch_processor: BatchProcessor # Added batch_processor
    ):
        self.external_api = external_api
        self.repository = repository
        self.job_manager = job_manager
        self.batch_processor = batch_processor # Stored batch_processor

    async def _process_and_insert_chunk(self, data_chunk_df: pl.DataFrame) -> Dict[str, Any]:
        """
        Processes a single DataFrame chunk: validates data using _validate_production_data_df
        (offloaded to a thread) and then inserts the validated data into the repository.
        Returns a dictionary with processing statistics for the chunk.
        """
        if data_chunk_df is None or data_chunk_df.is_empty():
            return {
                'inserted': 0, 'duplicates': 0, 'validation_errors': [],
                'validated_count': 0, 'input_count': 0
            }

        input_count = data_chunk_df.height
        # Offload potentially CPU-bound validation to a separate thread
        # to avoid blocking the asyncio event loop.
        valid_df, validation_errors = await asyncio.to_thread(self._validate_production_data_df, data_chunk_df)

        validated_count = valid_df.height
        inserted_count = 0
        duplicate_count = 0

        if not valid_df.is_empty():
            _, inserted_count, duplicate_count = await self.repository.bulk_insert(valid_df)

        return {
            'inserted': inserted_count,
            'duplicates': duplicate_count,
            'validation_errors': validation_errors,
            'validated_count': validated_count,
            'input_count': input_count
        }

    @async_timed
    async def import_production_data(
        self,
        filters: Optional[Dict[str, Any]] = None,
        batch_id: str = None
    ) -> BatchResult:
        """
        Import well production data from an external source using a streaming approach.
        This method consumes an asynchronous stream of DataFrame chunks from an external API adapter,
        processes these chunks using BatchProcessor for robust batching and retries,
        validates and inserts data from each chunk, aggregates overall results,
        and updates the JobManager with progress and final status.
        """
        logger.info(f"Starting well production data import (batch ID: {batch_id}) with streaming.")

        # Aggregated statistics
        total_records_from_source_agg = 0
        total_new_records_inserted_agg = 0
        total_duplicate_records_skipped_agg = 0
        total_failed_validation_records_agg = 0
        all_validation_errors_agg = [] # Stores string representations of errors

        # The processor function for BatchProcessor.process_stream
        # It receives a list of items (DataFrames in this case)
        async def stream_processor(data_chunks: List[pl.DataFrame]) -> List[Dict[str, Any]]:
            chunk_results = []
            for chunk_df in data_chunks:
                result = await self._process_and_insert_chunk(chunk_df)
                chunk_results.append(result)
            return chunk_results

        data_stream = self.external_api.fetch_well_production_data(filters=filters)
        processed_item_count_for_job_update = 0

        try:
            async for batch_processing_result in self.batch_processor.process_stream(
                items=data_stream, # AsyncGenerator[pl.DataFrame, None]
                processor=stream_processor # Callable[[List[pl.DataFrame]], Coroutine[Any, Any, List[Dict[str, Any]]]]
            ):
                # batch_processing_result is a BatchResult from BatchProcessor
                # Its 'results' field contains the list of dicts from stream_processor

                # Aggregate stats from this batch
                for chunk_proc_result in batch_processing_result.results:
                    total_records_from_source_agg += chunk_proc_result['input_count']
                    total_new_records_inserted_agg += chunk_proc_result['inserted']
                    total_duplicate_records_skipped_agg += chunk_proc_result['duplicates']

                    # Errors from _validate_production_data_df are lists of dicts
                    # Convert them to strings for the final BatchResult error list
                    for val_error_detail in chunk_proc_result['validation_errors']:
                        all_validation_errors_agg.append(str(val_error_detail)) # Or format more nicely

                    # Failed validation is input_count - validated_count
                    total_failed_validation_records_agg += (chunk_proc_result['input_count'] - chunk_proc_result['validated_count'])

                processed_item_count_for_job_update += batch_processing_result.total_items # total_items processed by this BatchProcessor batch

                if batch_id:
                    # Job manager update:
                    # total_records for job manager could be an estimate or updated as we go.
                    # For now, let's assume we don't know the grand total upfront with streaming.
                    # We can update progress based on processed items if total is known,
                    # or just log incremental updates.
                    await self.job_manager.update_job(
                        batch_id,
                        # total_records=???, # This is tricky with true streaming
                        progress_increment=batch_processing_result.total_items, # Number of DataFrames processed in this batch
                        # Or, if BatchProcessor's total_items means something else (e.g. rows if it could see inside DataFrames)
                        # We need to be careful about what 'total_items' means for BatchProcessor with DataFrame inputs.
                        # For now, assume one DataFrame is one item for BatchProcessor.
                        new_records_increment=sum(r['inserted'] for r in batch_processing_result.results),
                        duplicate_records_increment=sum(r['duplicates'] for r in batch_processing_result.results),
                        # We might need a 'failed_validation_increment' too
                    )
            
            if total_records_from_source_agg == 0:
                 logger.info(f"No data streamed from external API for batch ID: {batch_id}")
                 data_status = 'no_data_from_source'
            elif total_new_records_inserted_agg > 0:
                data_status = 'updated'
            elif total_failed_validation_records_agg == total_records_from_source_agg : # All records failed validation
                data_status = 'all_failed_validation'
            elif total_duplicate_records_skipped_agg == (total_records_from_source_agg - total_failed_validation_records_agg) and (total_records_from_source_agg - total_failed_validation_records_agg) > 0 : # All valid records were duplicates
                data_status = 'no_new_data_all_duplicates'
            else: # Default if none of the above specific statuses fit
                data_status = 'processed_with_mixed_results'


            # Final success rate calculation
            # Total "attempted" inserts are records that passed validation
            attempted_inserts = total_records_from_source_agg - total_failed_validation_records_agg
            success_rate = (total_new_records_inserted_agg / attempted_inserts * 100) if attempted_inserts > 0 else 100
            if total_records_from_source_agg == 0 : success_rate = 100 # No data, so 100% success? Or 0%?
            if attempted_inserts == 0 and total_records_from_source_agg > 0: success_rate = 0


            logger.info(f"Streaming import completed for batch ID {batch_id}: "
                        f"{total_new_records_inserted_agg} new, "
                        f"{total_duplicate_records_skipped_agg} duplicates, "
                        f"{total_failed_validation_records_agg} failed validation "
                        f"out of {total_records_from_source_agg} total records from source.")

            final_batch_result = BatchResult(
                batch_id=batch_id,
                total_items=total_records_from_source_agg,
                processed_items=total_new_records_inserted_agg,
                failed_items=total_failed_validation_records_agg + total_duplicate_records_skipped_agg,
                success_rate=success_rate,
                errors=all_validation_errors_agg,
                # execution_time_ms and memory_usage_mb would ideally be measured by the BatchProcessor or JobManager
                metadata={
                    'new_records': total_new_records_inserted_agg,
                    'duplicate_records': total_duplicate_records_skipped_agg,
                    'failed_validation_records': total_failed_validation_records_agg,
                    'data_status': data_status
                }
            )
            if batch_id: # Final update for the job
                 await self.job_manager.update_job(
                    batch_id, status='completed',
                    final_statistics=final_batch_result.metadata,
                    # Ensure progress is set to 100 if not already,
                    # and total_records is accurate if it can be determined.
                 )
            return final_batch_result

        except (ApplicationException, ExternalApiException, BatchProcessingException) as e: # Catch specific exceptions
            logger.error(f"Error during streaming import (batch ID: {batch_id}): {str(e)}", exc_info=True)
            if batch_id:
                await self.job_manager.update_job(batch_id, status='failed', error=str(e))
            # Re-raise specific exceptions if they are already suitable for API response
            # Or wrap in ApplicationException if not.
            if not isinstance(e, ApplicationException): # Wrap if it's not already one
                 raise ApplicationException(message=f"Import failed: {str(e)}", cause=e, batch_id=batch_id)
            else:
                 raise
        except Exception as e: # Catch any other unexpected errors
            logger.error(f"Unexpected error during streaming import (batch ID: {batch_id}): {str(e)}", exc_info=True)
            if batch_id:
                await self.job_manager.update_job(batch_id, status='failed', error=str(e))
            raise ApplicationException(
                message=f"Import failed due to unexpected error: {str(e)}",
                cause=e,
                batch_id=batch_id
            )

    @timed
    def _validate_production_data_df(
        self,
        production_df: pl.DataFrame 
    ) -> Tuple[pl.DataFrame, List[Dict[str, Any]]]: 
        """
        Validate production data using Polars DataFrame for efficiency.
        Performs schema mapping, type casting, and rule-based validation.
        Returns a DataFrame with valid rows and a list of dictionaries detailing errors.
        """
        if production_df.is_empty():
            return production_df, []

        errors: List[Dict[str, Any]] = []
        df_to_validate = production_df.clone() # Work on a clone to avoid modifying the original

        # --- Field Name Mapping (Align with WellProduction entity and DB schema) ---
        # Source JSON field names (keys) to target DataFrame/DB column names (values)
        rename_map = {
            # Direct mappings for fields that already match
            "field_code": "field_code",
            "_field_name": "field_name",  # Fix: underscore prefix in source
            "well_code": "well_code", 
            "_well_reference": "well_reference",  # Fix: underscore prefix in source
            "well_name": "well_name",
            "production_period": "production_period",
            "days_on_production": "days_on_production",
            "oil_production_kbd": "oil_production_kbd",
            "gas_production_mmcfd": "gas_production_mmcfd",
            "liquids_production_kbd": "liquids_production_kbd",
            "water_production_kbd": "water_production_kbd",
            "data_source": "data_source",
            "source_data": "source_data", 
            "partition_0": "partition_0",
            # Legacy Pascal case mappings (in case data format changes)
            "FieldCode": "field_code",
            "FieldName": "field_name",
            "WellCode": "well_code",
            "WellReference": "well_reference",
            "WellName": "well_name",
            "ProductionPeriod": "production_period",
            "DaysOnProduction": "days_on_production",
            "OilProductionKBD": "oil_production_kbd",
            "GasProductionMMCFD": "gas_production_mmcfd",
            "LiquidsProductionKBD": "liquids_production_kbd",
            "WaterProductionKBD": "water_production_kbd",
            "DataSource": "data_source",
            "SourceData": "source_data", 
            "Partition0": "partition_0",
            "createdAt": "created_at", 
            "updatedAt": "updated_at"
        }
        actual_rename_map = {k: v for k, v in rename_map.items() if k in df_to_validate.columns}
        if actual_rename_map:
            df_to_validate = df_to_validate.rename(actual_rename_map)

        # --- Define Target Schema (Matches DuckDB well_production table) ---
        # This ensures correct types and column order for DB insertion.
        target_schema_with_types = {
            "field_code": pl.Int64, "field_name": pl.Utf8, "well_code": pl.Int64,
            "well_reference": pl.Utf8, "well_name": pl.Utf8, "production_period": pl.Utf8,
            "days_on_production": pl.Int64, "oil_production_kbd": pl.Float64,
            "gas_production_mmcfd": pl.Float64, "liquids_production_kbd": pl.Float64,
            "water_production_kbd": pl.Float64, "data_source": pl.Utf8,
            "source_data": pl.Utf8, "partition_0": pl.Utf8,
            "created_at": pl.Datetime, "updated_at": pl.Datetime
        }
        target_columns_ordered = list(target_schema_with_types.keys())

        # --- Type Casting and Column Preparation ---
        expressions_for_casting = []
        for col_name, target_type in target_schema_with_types.items():
            if col_name in df_to_validate.columns:
                current_type = df_to_validate[col_name].dtype
                if target_type == pl.Datetime and current_type == pl.Utf8:
                    # Attempt to parse ISO 8601 format, Coalesce errors to null
                    # Example: "2023-01-15T10:00:00Z" or "2023-01-15T10:00:00.123456Z"
                    # Polars' strptime is quite flexible. Adjust format string if needed.
                    expressions_for_casting.append(
                        pl.col(col_name).str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S%.f%Z", strict=False, exact=False).alias(col_name)
                    )
                elif target_type == pl.Int64 and current_type == pl.Utf8:
                    expressions_for_casting.append(pl.col(col_name).cast(pl.Int64, strict=False).alias(col_name))
                elif target_type == pl.Float64 and current_type == pl.Utf8:
                     expressions_for_casting.append(pl.col(col_name).cast(pl.Float64, strict=False).alias(col_name))
                elif current_type != target_type:
                    expressions_for_casting.append(pl.col(col_name).cast(target_type, strict=False).alias(col_name))
            else:
                # Add missing columns as null literals of the target type
                expressions_for_casting.append(pl.lit(None, dtype=target_type).alias(col_name))
        
        if expressions_for_casting:
            df_to_validate = df_to_validate.with_columns(expressions_for_casting)
        
        # Ensure all target columns exist, filling with null if any were missed (e.g. not in expressions_for_casting)
        for col_name, target_type in target_schema_with_types.items():
            if col_name not in df_to_validate.columns:
                df_to_validate = df_to_validate.with_columns(pl.lit(None, dtype=target_type).alias(col_name))

        # Select columns in the target order, effectively dropping any unexpected ones
        df_validated = df_to_validate.select(target_columns_ordered)

        # --- Validation Rules ---
        # Rule 1: Primary Key components must not be null
        # PK: (well_code, field_code, production_period)
        pk_cols = ["well_code", "field_code", "production_period"]
        pk_null_condition = None
        for pk_col in pk_cols:
            condition = pl.col(pk_col).is_null()
            if pk_null_condition is None:
                pk_null_condition = condition
            else:
                pk_null_condition = pk_null_condition | condition
        
        invalid_pk_rows = df_validated.filter(pk_null_condition)
        if not invalid_pk_rows.is_empty():
            for row_dict in invalid_pk_rows.to_dicts(): # Convert failing rows to dicts for error reporting
                errors.append({
                    "error_type": "NullPrimaryKeyComponent",
                    "message": f"Primary key component is null for data: { {k: row_dict.get(k) for k in pk_cols} }",
                    "data": {k: row_dict.get(k) for k in pk_cols} # Include only PK cols for brevity
                })
            df_validated = df_validated.filter(pk_null_condition.is_not()) # Keep only non-null PKs

        # Rule 2: days_on_production >= 0
        if "days_on_production" in df_validated.columns and df_validated["days_on_production"].dtype == pl.Int64:
            dop_invalid_condition = pl.col("days_on_production") < 0
            invalid_dop_rows = df_validated.filter(dop_invalid_condition)
            if not invalid_dop_rows.is_empty():
                for row_dict in invalid_dop_rows.to_dicts():
                    errors.append({
                        "error_type": "InvalidDaysOnProduction",
                        "message": f"days_on_production is negative ({row_dict.get('days_on_production')}) for PK: ({row_dict.get('well_code')}, {row_dict.get('field_code')}, {row_dict.get('production_period')})",
                        "data": { "well_code": row_dict.get('well_code'), "field_code": row_dict.get('field_code'), "production_period": row_dict.get('production_period'), "days_on_production": row_dict.get('days_on_production')}
                    })
                df_validated = df_validated.filter(dop_invalid_condition.is_not())
        
        # Add more validation rules as needed...
        # Example: Check string lengths, specific value constraints, etc.
        # For each rule, filter out invalid rows, add to `errors`, and update `df_validated`

        logger.info(f"Polars validation: Input rows: {production_df.height}, Valid rows: {df_validated.height}, Errors: {len(errors)}")
        return df_validated, errors

    # Removed: _create_well_production_entities_from_df - this is now part of validation/transformation to Polars DF

    # Removed: _get_default_batch_config - BatchProcessor is not used in this simplified flow

    # Removed: _run_batch_processor - BatchProcessor is not used in this simplified flow

# Removing the unused _import_stats attribute from __init__ if it's fully managed within import_production_data
# Also removing BatchProcessor from __init__ if unused.

# The old _validate_production_data and _insert_batch methods are effectively replaced or unused.
# The edit tool will handle removal if the new content fully replaces the old methods' lines.
# If the old methods are not fully covered by the diff, they might need explicit deletion.
# The provided diff implies replacement of import_production_data and addition of _validate_production_data_df.
# The unused methods _validate_production_data (original) and _insert_batch should be manually checked for removal if not overwritten by the tool.

# For the purpose of this edit, I am focusing on replacing `import_production_data` and adding the new validation method.
# The original `_validate_production_data` and `_insert_batch` are implicitly removed by not being included in the new version of the file content that this edit would generate if it replaced the whole file.
# If the edit tool does line-by-line patching, those methods would remain unless explicitly targeted for deletion.
# Assuming this edit is comprehensive for the service logic.
