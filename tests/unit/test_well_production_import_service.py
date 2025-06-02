import pytest
import polars as pl
from unittest.mock import AsyncMock, MagicMock, patch
import asyncio # Required for async tests

from src.application.services.well_production_import_service import WellProductionImportService
from src.shared.batch_processor import BatchProcessor, BatchConfig, BatchResult # Assuming BatchConfig is not directly used here but BatchResult is
from src.shared.job_manager import JobManager, JobStatus # Assuming JobStatus is needed for JobManager mock
from src.infrastructure.adapters.external_api_adapter import ExternalApiAdapter # For spec
from src.domain.repositories.well_production_repository import WellProductionRepository # For spec

# Default Polars DataFrame for testing
DEFAULT_TEST_DF_SCHEMA = {
    "field_code": pl.Int64, "field_name": pl.Utf8, "well_code": pl.Int64,
    "well_reference": pl.Utf8, "well_name": pl.Utf8, "production_period": pl.Utf8,
    "days_on_production": pl.Int64, "oil_production_kbd": pl.Float64,
    "gas_production_mmcfd": pl.Float64, "liquids_production_kbd": pl.Float64,
    "water_production_kbd": pl.Float64, "data_source": pl.Utf8,
    "source_data": pl.Utf8, "partition_0": pl.Utf8,
    "created_at": pl.Datetime, "updated_at": pl.Datetime
}


@pytest.fixture
def mock_external_api_adapter():
    adapter = AsyncMock(spec=ExternalApiAdapter)

    # Create some sample DataFrames that _validate_production_data_df would expect
    # These should ideally match the structure after _map_polars_df_fields in the adapter
    df_chunk1 = pl.DataFrame([
        {"field_code": 1, "well_code": 101, "production_period": "2023-01-01", "days_on_production": 10, "oil_production_kbd": 1.1},
        {"field_code": 1, "well_code": 102, "production_period": "2023-01-01", "days_on_production": 10, "oil_production_kbd": 1.2},
    ], schema=DEFAULT_TEST_DF_SCHEMA)
    df_chunk2 = pl.DataFrame([
        {"field_code": 2, "well_code": 201, "production_period": "2023-02-01", "days_on_production": 20, "oil_production_kbd": 2.1},
    ], schema=DEFAULT_TEST_DF_SCHEMA)

    async def stream_data_mock(*args, **kwargs): # Accept arbitrary args for flexibility
        yield df_chunk1
        yield df_chunk2

    adapter.fetch_well_production_data = MagicMock(return_value=stream_data_mock())
    return adapter

@pytest.fixture
def mock_repository():
    repo = AsyncMock(spec=WellProductionRepository)
    # Simulate bulk_insert results: ([], new_records_count, duplicate_records_count)
    # Let's make it dynamic based on input DataFrame height for more realism if needed,
    # or just a fixed return for simplicity.
    async def mock_bulk_insert(df: pl.DataFrame):
        # Simulate some records being new, some duplicate for more complex scenarios if required.
        # For this test, let's assume all validated records are inserted as new.
        return ([], df.height, 0)
    repo.bulk_insert = MagicMock(side_effect=mock_bulk_insert)
    return repo

@pytest.fixture
def mock_job_manager():
    manager = AsyncMock(spec=JobManager)
    manager.update_job = AsyncMock() # Ensure methods called via asyncio.create_task are also AsyncMocks
    return manager

@pytest.fixture
def real_batch_processor():
    # Using a real BatchProcessor with a simple config for testing the service's interaction with it.
    # The processor function within the service will be the actual target of the test.
    config = BatchConfig(batch_size=1, max_concurrent_batches=1) # Process one DataFrame chunk at a time
    return BatchProcessor(config=config)


@pytest.fixture
def import_service(mock_external_api_adapter, mock_repository, mock_job_manager, real_batch_processor):
    return WellProductionImportService(
        external_api=mock_external_api_adapter,
        repository=mock_repository,
        job_manager=mock_job_manager,
        batch_processor=real_batch_processor
    )
@pytest.mark.asyncio
async def test_process_and_insert_chunk_valid_data(import_service, mock_repository):
    # Test the _process_and_insert_chunk method in isolation
    test_df = pl.DataFrame([
        {"field_code": 1, "well_code": 101, "production_period": "2023-01-01", "days_on_production": 10, "oil_production_kbd": 1.1},
    ], schema=DEFAULT_TEST_DF_SCHEMA)

    # Mock the validation method for this specific unit test
    # _validate_production_data_df returns (valid_df, errors_list)
    with patch.object(import_service, '_validate_production_data_df', return_value=(test_df, [])) as mock_validate:
        result = await import_service._process_and_insert_chunk(test_df)

        mock_validate.assert_called_once_with(test_df)
        mock_repository.bulk_insert.assert_called_once_with(test_df)
        assert result['inserted'] == test_df.height
        assert result['duplicates'] == 0
        assert result['validated_count'] == test_df.height
        assert result['input_count'] == test_df.height
        assert not result['validation_errors']

@pytest.mark.asyncio
async def test_process_and_insert_chunk_invalid_data(import_service, mock_repository):
    test_df = pl.DataFrame([
        {"field_code": 1, "well_code": 101, "production_period": "2023-01-01", "days_on_production": -5} # Invalid
    ], schema=DEFAULT_TEST_DF_SCHEMA)

    empty_df = test_df.clear() # An empty DataFrame of same schema
    validation_error_detail = [{"error_type": "InvalidDaysOnProduction", "data": {"days_on_production": -5}}]

    with patch.object(import_service, '_validate_production_data_df', return_value=(empty_df, validation_error_detail)) as mock_validate:
        result = await import_service._process_and_insert_chunk(test_df)

        mock_validate.assert_called_once_with(test_df)
        mock_repository.bulk_insert.assert_not_called()
        assert result['inserted'] == 0
        assert result['duplicates'] == 0
        assert result['validated_count'] == 0
        assert result['input_count'] == test_df.height
        assert result['validation_errors'] == validation_error_detail

@pytest.mark.asyncio
async def test_import_production_data_overall_flow(import_service, mock_external_api_adapter, mock_repository, mock_job_manager):
    job_id = "test_job_123"

    # Let _validate_production_data_df pass through valid data for simplicity in this flow test
    # The actual validation logic is tested in test__validate_production_data_df (if that were a separate test)
    # or covered by tests for _process_and_insert_chunk

    # df_chunk1 has 2 rows, df_chunk2 has 1 row (from mock_external_api_adapter)
    # Let's assume all are valid and inserted

    # Mock _validate_production_data_df to return the input df as valid and no errors
    async def mock_validate_passthrough(df_chunk):
        await asyncio.sleep(0) # simulate async work if needed by to_thread
        return df_chunk, []

    with patch.object(import_service, '_validate_production_data_df', side_effect=mock_validate_passthrough):
        final_result = await import_service.import_production_data(batch_id=job_id)

        # Verify adapter was called (implicitly by data_stream being consumed)
        mock_external_api_adapter.fetch_well_production_data.assert_called_once()

        # Verify repository calls - bulk_insert would be called for each chunk by BatchProcessor
        # df_chunk1 (2 rows), df_chunk2 (1 row)
        assert mock_repository.bulk_insert.call_count == 2
        # First call with df_chunk1, second with df_chunk2
        # Note: pl.DataFrame comparison can be tricky. Comparing shape or specific values might be better.
        # For simplicity, we trust the mock_bulk_insert side_effect which uses df.height.

        # Verify final aggregated BatchResult
        assert final_result.total_items == 3 # 2 from chunk1 + 1 from chunk2
        assert final_result.processed_items == 3 # All inserted as new
        assert final_result.failed_items == 0 # No duplicates, no validation errors in this setup
        assert final_result.metadata['new_records'] == 3
        assert final_result.metadata['duplicate_records'] == 0
        assert final_result.metadata['failed_validation_records'] == 0

        # Verify JobManager interactions
        # Initial update (progress_increment based on number of chunks processed by BatchProcessor)
        # With batch_size=1 in RealBatchProcessor, each chunk is one batch.
        mock_job_manager.update_job.assert_any_call(
            job_id,
            progress_increment=1, # Assuming BatchProcessor processes 1 DataFrame chunk as 1 item
            new_records_increment=2, # From first chunk
            duplicate_records_increment=0
        )
        mock_job_manager.update_job.assert_any_call(
            job_id,
            progress_increment=1, # Assuming BatchProcessor processes 1 DataFrame chunk as 1 item
            new_records_increment=1, # From second chunk
            duplicate_records_increment=0
        )
        # Final update
        mock_job_manager.update_job.assert_called_with( # Check the last call
            job_id, status='completed',
            final_statistics=final_result.metadata
        )

@pytest.mark.asyncio
async def test_import_production_data_empty_stream(import_service, mock_external_api_adapter, mock_job_manager):
    job_id = "empty_stream_job"
    # Configure adapter to yield nothing
    async def empty_stream_mock(*args, **kwargs):
        if False: # Make it an async generator
            yield
    mock_external_api_adapter.fetch_well_production_data = MagicMock(return_value=empty_stream_mock())

    final_result = await import_service.import_production_data(batch_id=job_id)

    assert final_result.total_items == 0
    assert final_result.processed_items == 0
    assert final_result.metadata['data_status'] == 'no_data_from_source'

    # Check final job update
    mock_job_manager.update_job.assert_called_with(
        job_id, status='completed',
        final_statistics=final_result.metadata
    )

# TODO: Add tests for error handling during _process_and_insert_chunk (e.g., repository error)
# TODO: Add tests for different outcomes of validation (some valid, some invalid)
```
