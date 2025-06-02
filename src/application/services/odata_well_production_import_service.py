"""
OData Well Production Import Service for importing data from external OData APIs.
Implements hexagonal architecture and DDD principles with object calisthenics.
"""
import logging
import asyncio # Ensure asyncio is imported
from typing import Optional, Dict, Any, Tuple, List, AsyncGenerator
import polars as pl

from ...domain.entities.well_production import WellProduction
from ...domain.repositories.well_production_repository import WellProductionRepository
from ...domain.ports.odata_external_api_port import ODataExternalApiPort
from ...shared.batch_processor import BatchResult
from ...shared.exceptions import (
    ValidationException,
    ApplicationException,
    ExternalApiException
)
from ...shared.job_manager import JobManager
from ...shared.utils.timing_decorator import async_timed, timed

logger = logging.getLogger(__name__)


class ODataWellProductionImportService:
    """
    Service for importing well production data from external OData APIs.
    Follows DDD principles and object calisthenics for clean, maintainable code.
    """

from ...shared.batch_processor import BatchProcessor, BatchResult # Added BatchProcessor
from ...shared.exceptions import (
    ValidationException,
    ApplicationException,
    ExternalApiException,
    BatchProcessingException # Added BatchProcessingException
)
from ...shared.job_manager import JobManager
from ...shared.utils.timing_decorator import async_timed, timed

logger = logging.getLogger(__name__)


class ODataWellProductionImportService:
    """
    Service for importing well production data from external OData APIs.
    Follows DDD principles and object calisthenics for clean, maintainable code.
    """

    def __init__(
        self,
        odata_api_adapter: ODataExternalApiPort,
        repository: WellProductionRepository,
        job_manager: JobManager,
        batch_processor: BatchProcessor # Added BatchProcessor
    ):
        self._odata_api_adapter = odata_api_adapter
        self._repository = repository
        self._job_manager = job_manager
        self._batch_processor = batch_processor # Stored BatchProcessor

    async def _process_and_insert_chunk(self, data_chunk_df: pl.DataFrame) -> Dict[str, Any]:
        """
        Processes a single DataFrame chunk: validates data using _validate_production_dataframe
        (offloaded to a thread) and then inserts the validated data into the repository.
        Leverages existing validation and insertion helper methods.
        Returns a dictionary with processing statistics for the chunk.
        """
        if self._is_dataframe_empty(data_chunk_df):
            return {
                'inserted': 0, 'duplicates': 0, 'validation_errors': [],
                'validated_count': 0, 'input_count': 0, 'original_errors': []
            }

        input_count = data_chunk_df.height
        # _validate_production_dataframe returns (validated_df, list_of_error_dicts)
        # Offload potentially CPU-bound validation to a separate thread
        # to avoid blocking the asyncio event loop.
        validated_df, validation_errors_list_of_dicts = await asyncio.to_thread(
            self._validate_production_dataframe, data_chunk_df
        )

        validated_count = validated_df.height
        inserted_count = 0
        duplicate_count = 0

        if not self._is_dataframe_empty(validated_df):
            # _insert_validated_data returns InsertionResult(new_records, duplicate_records)
            insertion_result_obj = await self._insert_validated_data(validated_df)
            inserted_count = insertion_result_obj.new_records
            duplicate_count = insertion_result_obj.duplicate_records

        return {
            'inserted': inserted_count,
            'duplicates': duplicate_count,
            'validation_errors': validation_errors_list_of_dicts, # Keep as list of dicts
            'validated_count': validated_count,
            'input_count': input_count
        }

    @async_timed
    async def import_production_data_from_odata(
        self,
        batch_id: Optional[str] = None
    ) -> BatchResult:
        """
        Import well production data from an external OData API using a streaming approach.
        This method consumes an asynchronous stream of DataFrame chunks from the OData API adapter,
        processes these chunks using BatchProcessor for robust batching and retries,
        validates and inserts data from each chunk, aggregates overall results,
        and updates the JobManager with progress and final status.
        """
        logger.info(f"Starting OData well production data import (batch ID: {batch_id}) with streaming.")

        # Aggregated statistics using ImportMetrics
        aggregated_metrics = ImportMetrics()
        all_validation_errors_agg_str = [] # Stores string representations for final BatchResult

        # The processor function for BatchProcessor.process_stream
        async def stream_processor(data_chunks: List[pl.DataFrame]) -> List[Dict[str, Any]]:
            chunk_results = []
            for chunk_df in data_chunks:
                result = await self._process_and_insert_chunk(chunk_df)
                chunk_results.append(result)
            return chunk_results

        # Assuming _fetch_data_from_odata_api now returns an AsyncGenerator[pl.DataFrame, None]
        # This change in _fetch_data_from_odata_api is crucial and assumed to be done or compatible.
        data_stream = self._fetch_data_from_odata_api()

        try:
            async for batch_processing_result in self._batch_processor.process_stream(
                items=data_stream,
                processor=stream_processor
            ):
                # batch_processing_result is a BatchResult from BatchProcessor.
                # Its 'results' field contains the list of dicts from stream_processor.

                current_batch_new_records = 0
                current_batch_duplicate_records = 0

                for chunk_proc_result in batch_processing_result.results:
                    aggregated_metrics.total_records_from_source += chunk_proc_result['input_count']
                    aggregated_metrics.new_records += chunk_proc_result['inserted']
                    aggregated_metrics.duplicate_records += chunk_proc_result['duplicates']

                    current_batch_new_records += chunk_proc_result['inserted']
                    current_batch_duplicate_records += chunk_proc_result['duplicates']

                    for val_error_detail in chunk_proc_result['validation_errors']:
                        all_validation_errors_agg_str.append(str(val_error_detail))

                    aggregated_metrics.failed_validation_records += (chunk_proc_result['input_count'] - chunk_proc_result['validated_count'])

                if batch_id:
                    await self._job_manager.update_job(
                        batch_id,
                        progress_increment=batch_processing_result.total_items, # Assuming one DataFrame chunk is one item
                        new_records_increment=current_batch_new_records,
                        duplicate_records_increment=current_batch_duplicate_records,
                    )

            data_status = self._determine_data_status(aggregated_metrics)

            logger.info(f"OData streaming import completed for batch ID {batch_id}: "
                       f"{aggregated_metrics.new_records} new, "
                       f"{aggregated_metrics.duplicate_records} duplicates, "
                       f"{aggregated_metrics.failed_validation_records} failed validation "
                       f"out of {aggregated_metrics.total_records_from_source} total records.")

            final_batch_result = self._create_final_batch_result(batch_id, aggregated_metrics, data_status, all_validation_errors_agg_str)

            if batch_id:
                 await self._job_manager.update_job(
                    batch_id, status='completed',
                    final_statistics=final_batch_result.metadata,
                 )
            return final_batch_result

        except (ApplicationException, ExternalApiException, BatchProcessingException) as e:
            logger.error(f"Error during OData streaming import (batch ID: {batch_id}): {str(e)}", exc_info=True)
            if batch_id:
                await self._handle_job_failure_if_exists(batch_id, str(e)) # Pass error message
            if not isinstance(e, ApplicationException):
                 raise ApplicationException(message=f"OData import failed: {str(e)}", cause=e, batch_id=batch_id)
            else:
                 raise
        except Exception as e:
            logger.error(f"Unexpected error during OData streaming import (batch ID: {batch_id}): {str(e)}", exc_info=True)
            if batch_id:
                await self._handle_job_failure_if_exists(batch_id, str(e)) # Pass error message
            raise ApplicationException(
                message=f"OData import failed due to unexpected error: {str(e)}",
                cause=e,
                batch_id=batch_id
            )

    async def _fetch_data_from_odata_api(self) -> AsyncGenerator[pl.DataFrame, None]:
        """
        Fetch data from OData API, expecting an AsyncGenerator of DataFrames.
        This method now acts as a pass-through or could encapsulate pre-processing if needed
        before data is streamed to BatchProcessor.
        """
        try:
            # This assumes self._odata_api_adapter.fetch_well_production_data()
            # has been updated to be an AsyncGenerator[pl.DataFrame, None]
            # similar to ExternalApiAdapter.fetch_well_production_data
            async for data_chunk_df in self._odata_api_adapter.fetch_well_production_data():
                yield data_chunk_df
        except ExternalApiException as e:
            logger.error(f"Failed to fetch data from OData API stream: {e.message}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error fetching from OData API stream: {str(e)}", exc_info=True)
            raise ExternalApiException(
                message=f"Unexpected error during OData API fetch stream: {str(e)}",
                endpoint=self._odata_api_adapter.base_url, # base_url might not be on ODataExternalApiPort, adjust if needed
                cause=e
            )

    @timed
    def _validate_production_dataframe(
        self,
        production_dataframe: pl.DataFrame
    ) -> Tuple[pl.DataFrame, List[Dict[str, Any]]]:
        """
        Validate production data using Polars DataFrame for efficiency.
        Follows object calisthenics by avoiding primitive obsession and using intention-revealing names.
        """
        if self._is_dataframe_empty(production_dataframe):
            return production_dataframe, []

        validation_errors = []
        dataframe_to_validate = production_dataframe.clone()

        # Apply field name mapping
        dataframe_to_validate = self._apply_field_name_mapping(dataframe_to_validate)

        # Apply type casting and schema validation
        dataframe_to_validate = self._apply_type_casting_and_schema(dataframe_to_validate)

        # Apply business rule validations
        validated_dataframe, business_rule_errors = self._apply_business_rule_validations(dataframe_to_validate)
        validation_errors.extend(business_rule_errors)

        logger.info(f"Polars validation: Input rows: {production_dataframe.height}, "
                   f"Valid rows: {validated_dataframe.height}, Errors: {len(validation_errors)}")

        return validated_dataframe, validation_errors

    def _apply_field_name_mapping(self, dataframe: pl.DataFrame) -> pl.DataFrame:
        """Apply field name mapping to align with domain entity structure."""
        field_mapping = FieldMapping()
        actual_rename_map = field_mapping.get_applicable_mappings(dataframe.columns)
        
        if actual_rename_map:
            return dataframe.rename(actual_rename_map)
        return dataframe

    def _apply_type_casting_and_schema(self, dataframe: pl.DataFrame) -> pl.DataFrame:
        """Apply type casting and ensure schema compliance."""
        target_schema = TargetSchema()
        casting_expressions = target_schema.create_casting_expressions(dataframe)
        
        if casting_expressions:
            dataframe = dataframe.with_columns(casting_expressions)
        
        # Ensure all target columns exist
        dataframe = target_schema.ensure_all_columns_exist(dataframe)
        
        # Select columns in target order
        return dataframe.select(target_schema.get_ordered_columns())

    def _apply_business_rule_validations(
        self, 
        dataframe: pl.DataFrame
    ) -> Tuple[pl.DataFrame, List[Dict[str, Any]]]:
        """Apply business rule validations and return valid data with errors."""
        validator = BusinessRuleValidator()
        return validator.validate_dataframe(dataframe)

    async def _insert_validated_data(self, validated_dataframe: pl.DataFrame) -> 'InsertionResult':
        """Insert validated data into repository."""
        try:
            _, inserted_count, duplicate_count = await self._repository.bulk_insert(validated_dataframe)
            return InsertionResult(inserted_count, duplicate_count)
        except Exception as e:
            logger.error(f"Error inserting validated data: {str(e)}", exc_info=True)
            raise ApplicationException(
                message=f"Failed to insert validated data: {str(e)}",
                cause=e
            )

    def _is_dataframe_empty(self, dataframe: pl.DataFrame) -> bool:
        """Check if dataframe is empty following object calisthenics."""
        return dataframe is None or dataframe.is_empty()

    def _determine_data_status(self, metrics: 'ImportMetrics') -> str:
        """Determine the final data status based on import metrics."""
        if metrics.new_records > 0:
            return 'updated'
        elif metrics.total_records_from_source > 0 and metrics.failed_validation_records == metrics.total_records_from_source:
            return 'all_failed_validation'
        elif metrics.duplicate_records > 0:
            return 'no_new_data_all_duplicates'
        elif metrics.total_records_from_source == 0:
            return 'no_data_from_source'
        else:
            return 'no_new_data'

    def _create_empty_batch_result(self, batch_id: str, metrics: 'ImportMetrics') -> BatchResult:
        """Create batch result for empty data scenario."""
        logger.info(f"No data returned from OData API for batch ID: {batch_id}")
        return BatchResult(
            batch_id=batch_id,
            total_items=0,
            processed_items=0,
            failed_items=0,
            success_rate=100,
            errors=[],
            execution_time_ms=0,
            memory_usage_mb=0,
            metadata={'data_status': 'no_data_from_source'}
        )

    def _create_validation_failed_batch_result(
        self, 
        batch_id: str, 
        metrics: 'ImportMetrics', 
        validation_errors: List[Dict[str, Any]]
    ) -> BatchResult:
        """Create batch result for validation failure scenario."""
        logger.info(f"No valid records after validation for batch ID: {batch_id}")
        return BatchResult(
            batch_id=batch_id,
            total_items=metrics.total_records_from_source,
            processed_items=0,
            failed_items=metrics.failed_validation_records,
            success_rate=0,
            errors=[str(e) for e in validation_errors],
            execution_time_ms=0,
            memory_usage_mb=0,
            metadata={
                'new_records': 0,
                'duplicate_records': 0,
                'failed_validation_records': metrics.failed_validation_records,
                'data_status': 'all_failed_validation' if metrics.total_records_from_source > 0 else 'no_data_from_source'
            }
        )

    # Renamed from _create_successful_batch_result to reflect its use for the final aggregated result
    def _create_final_batch_result(
        self,
        batch_id: str,
        metrics: 'ImportMetrics',
        data_status: str,
        all_validation_errors_str: List[str] # Now expects list of strings
    ) -> BatchResult:
        """Create the final batch result for the entire import operation."""
        potential_inserts = metrics.total_records_from_source - metrics.failed_validation_records
        success_rate = ((metrics.new_records / potential_inserts) * 100) if potential_inserts > 0 else 100
        
        if metrics.total_records_from_source == 0: # No data from source
            success_rate = 100 # Or based on requirements, could be 0 if data was expected.
        elif potential_inserts == 0 : # All records from source failed validation
            success_rate = 0


        return BatchResult(
            batch_id=batch_id,
            total_items=metrics.total_records_from_source,
            processed_items=metrics.new_records,
            failed_items=metrics.failed_validation_records + metrics.duplicate_records,
            success_rate=success_rate,
            errors=all_validation_errors_str, # Use aggregated string errors
            execution_time_ms=0, # This would ideally be measured by JobManager or BatchProcessor
            memory_usage_mb=0,   # This would ideally be measured by JobManager or BatchProcessor
            metadata={
                'new_records': metrics.new_records,
                'duplicate_records': metrics.duplicate_records,
                'failed_validation_records': metrics.failed_validation_records,
                'data_status': data_status
            }
        )

    async def _update_job_progress_if_exists(self, batch_id: str, total_records: int, progress: int) -> None:
        """Update job progress if batch_id exists."""
        if batch_id:
            await self._job_manager.update_job(
                batch_id,
                total_records=total_records,
                progress=progress
            )

    async def _update_job_completion_if_exists(self, batch_id: str, metrics: 'ImportMetrics') -> None:
        """Update job completion if batch_id exists."""
        if batch_id:
            await self._job_manager.update_job(
                batch_id,
                progress=100,
                new_records=metrics.new_records,
                duplicate_records=metrics.duplicate_records
            )

    async def _handle_job_failure_if_exists(self, batch_id: str, error_message: str = "Import failed") -> None:
        """Handle job failure if batch_id exists, including the error message."""
        if batch_id:
            await self._job_manager.update_job(batch_id, status='failed', error=error_message)


# Value objects following object calisthenics principles
class ImportMetrics:
    """Encapsulates import metrics to avoid primitive obsession."""
    
    def __init__(self):
        self.total_records_from_source = 0
        self.new_records = 0
        self.duplicate_records = 0
        self.failed_validation_records = 0

    def set_total_records_from_source(self, count: int) -> None:
        self.total_records_from_source = count

    def set_insertion_results(self, new_records: int, duplicate_records: int) -> None:
        self.new_records = new_records
        self.duplicate_records = duplicate_records

    def set_failed_validation_records(self, count: int) -> None:
        self.failed_validation_records = count


class InsertionResult:
    """Encapsulates insertion results."""
    
    def __init__(self, new_records: int, duplicate_records: int):
        self.new_records = new_records
        self.duplicate_records = duplicate_records


class FieldMapping:
    """Handles field name mapping from external API to domain entity."""
    
    def __init__(self):
        self._mapping = {
            "_field_name": "field_name",
            "_well_reference": "well_reference",
            "field_code": "field_code",
            "well_code": "well_code",
            "well_name": "well_name",
            "production_period": "production_period",
            "days_on_production": "days_on_production",
            "oil_production_kbd": "oil_production_kbd",
            "gas_production_mmcfd": "gas_production_mmcfd",
            "liquids_production_kbd": "liquids_production_kbd",
            "water_production_kbd": "water_production_kbd",
            "data_source": "data_source",
            "source_data": "source_data",
            "partition_0": "partition_0"
        }

    def get_applicable_mappings(self, columns: List[str]) -> Dict[str, str]:
        """Get mappings that are applicable to the given columns."""
        return {k: v for k, v in self._mapping.items() if k in columns}


class TargetSchema:
    """Defines and manages the target schema for well production data."""
    
    def __init__(self):
        self._schema_with_types = {
            "field_code": pl.Int64,
            "field_name": pl.Utf8,
            "well_code": pl.Int64,
            "well_reference": pl.Utf8,
            "well_name": pl.Utf8,
            "production_period": pl.Utf8,
            "days_on_production": pl.Int64,
            "oil_production_kbd": pl.Float64,
            "gas_production_mmcfd": pl.Float64,
            "liquids_production_kbd": pl.Float64,
            "water_production_kbd": pl.Float64,
            "data_source": pl.Utf8,
            "source_data": pl.Utf8,
            "partition_0": pl.Utf8,
            "created_at": pl.Datetime,
            "updated_at": pl.Datetime
        }

    def create_casting_expressions(self, dataframe: pl.DataFrame) -> List[pl.Expr]:
        """Create casting expressions for type conversion."""
        expressions = []
        
        for column_name, target_type in self._schema_with_types.items():
            if column_name in dataframe.columns:
                current_type = dataframe[column_name].dtype
                expression = self._create_type_casting_expression(column_name, current_type, target_type)
                if expression is not None:
                    expressions.append(expression)
            else:
                # Add missing columns as null literals
                expressions.append(pl.lit(None, dtype=target_type).alias(column_name))
        
        return expressions

    def _create_type_casting_expression(self, column_name: str, current_type: pl.DataType, target_type: pl.DataType) -> Optional[pl.Expr]:
        """Create a type casting expression for a specific column."""
        if target_type == pl.Datetime and current_type == pl.Utf8:
            return pl.col(column_name).str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S%.f%Z", strict=False, exact=False).alias(column_name)
        elif target_type == pl.Int64 and current_type == pl.Utf8:
            return pl.col(column_name).cast(pl.Int64, strict=False).alias(column_name)
        elif target_type == pl.Float64 and current_type == pl.Utf8:
            return pl.col(column_name).cast(pl.Float64, strict=False).alias(column_name)
        elif current_type != target_type:
            return pl.col(column_name).cast(target_type, strict=False).alias(column_name)
        
        return None

    def ensure_all_columns_exist(self, dataframe: pl.DataFrame) -> pl.DataFrame:
        """Ensure all target columns exist in the dataframe."""
        for column_name, target_type in self._schema_with_types.items():
            if column_name not in dataframe.columns:
                dataframe = dataframe.with_columns(pl.lit(None, dtype=target_type).alias(column_name))
        return dataframe

    def get_ordered_columns(self) -> List[str]:
        """Get columns in the target order."""
        return list(self._schema_with_types.keys())


class BusinessRuleValidator:
    """Validates business rules for well production data."""
    
    def validate_dataframe(self, dataframe: pl.DataFrame) -> Tuple[pl.DataFrame, List[Dict[str, Any]]]:
        """Validate dataframe against business rules."""
        errors = []
        validated_dataframe = dataframe

        # Rule 1: Primary key components must not be null
        validated_dataframe, pk_errors = self._validate_primary_key_components(validated_dataframe)
        errors.extend(pk_errors)

        # Rule 2: Days on production must be non-negative
        validated_dataframe, dop_errors = self._validate_days_on_production(validated_dataframe)
        errors.extend(dop_errors)

        return validated_dataframe, errors

    def _validate_primary_key_components(self, dataframe: pl.DataFrame) -> Tuple[pl.DataFrame, List[Dict[str, Any]]]:
        """Validate that primary key components are not null."""
        errors = []
        primary_key_columns = ["well_code", "field_code", "production_period"]
        
        null_condition = None
        for pk_column in primary_key_columns:
            condition = pl.col(pk_column).is_null()
            null_condition = condition if null_condition is None else null_condition | condition
        
        invalid_rows = dataframe.filter(null_condition)
        if not invalid_rows.is_empty():
            for row_dict in invalid_rows.to_dicts():
                errors.append({
                    "error_type": "NullPrimaryKeyComponent",
                    "message": f"Primary key component is null for data: {row_dict}",
                    "data": {k: row_dict.get(k) for k in primary_key_columns}
                })
            dataframe = dataframe.filter(null_condition.is_not())

        return dataframe, errors

    def _validate_days_on_production(self, dataframe: pl.DataFrame) -> Tuple[pl.DataFrame, List[Dict[str, Any]]]:
        """Validate that days on production is non-negative."""
        errors = []
        
        if "days_on_production" in dataframe.columns and dataframe["days_on_production"].dtype == pl.Int64:
            invalid_condition = pl.col("days_on_production") < 0
            invalid_rows = dataframe.filter(invalid_condition)
            
            if not invalid_rows.is_empty():
                for row_dict in invalid_rows.to_dicts():
                    errors.append({
                        "error_type": "InvalidDaysOnProduction",
                        "message": f"days_on_production is negative ({row_dict.get('days_on_production')})",
                        "data": {
                            "well_code": row_dict.get('well_code'),
                            "field_code": row_dict.get('field_code'),
                            "production_period": row_dict.get('production_period'),
                            "days_on_production": row_dict.get('days_on_production')
                        }
                    })
                dataframe = dataframe.filter(invalid_condition.is_not())

        return dataframe, errors 