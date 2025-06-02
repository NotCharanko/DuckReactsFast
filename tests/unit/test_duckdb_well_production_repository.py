import pytest
import duckdb
from pathlib import Path
import polars as pl
from datetime import datetime
import asyncio

from src.infrastructure.repositories.duckdb_well_production_repository import DuckDBWellProductionRepository
from src.domain.entities.well_production import WellProduction
# Assuming load_sql is in src.shared.utils.sql_loader based on previous patterns
from src.shared.utils.sql_loader import load_sql as load_wells_sql

@pytest.fixture(scope='function')
def memory_db_conn(tmp_path):
    # Using a temporary file-based DB for tests that need to reopen connection (like some export logic)
    # or to be absolutely sure about PRAGMA scope if that was a concern.
    # For most unit tests, :memory: is fine, but PRAGMAs on shared :memory: can bleed.
    db_file = tmp_path / "test_wells.duckdb"
    conn = duckdb.connect(str(db_file))
    # Load schema
    # Corrected path assuming operations/ is at src/infrastructure/operations/
    sql_queries = load_wells_sql(Path("src/infrastructure/operations/wells.sql"))
    conn.execute(sql_queries['create_table'])
    # Indexes might not be strictly necessary for all unit tests but good for completeness
    if 'create_indexes' in sql_queries: # check if create_indexes exists
        conn.execute(sql_queries['create_indexes'])
    yield conn
    conn.close()
    # db_file.unlink(missing_ok=True) # Not strictly necessary with tmp_path, but good practice

@pytest.fixture
def well_repo(memory_db_conn, tmp_path): # tmp_path for downloads_dir
    # downloads_dir and csv_filename are needed for export_to_csv
    # Ensure downloads_dir is also using tmp_path for test isolation
    downloads_test_dir = tmp_path / "downloads"
    downloads_test_dir.mkdir(exist_ok=True)
    return DuckDBWellProductionRepository(conn=memory_db_conn, downloads_dir=downloads_test_dir, csv_filename="test_export.csv")

@pytest.fixture
def sample_production_entity():
    return WellProduction(
        field_code=1, field_name="Field A", well_code=101, well_reference="Ref1", well_name="Well A1",
        production_period="2023-01-01", days_on_production=30, oil_production_kbd=1.0,
        gas_production_mmcfd=0.5, liquids_production_kbd=0.2, water_production_kbd=0.1,
        data_source="TestSource", source_data="Raw1", partition_0="P1",
        created_at=datetime.now(), updated_at=datetime.now()
    )

@pytest.fixture
def sample_production_entity_2():
    return WellProduction(
        field_code=2, field_name="Field B", well_code=202, well_reference="Ref2", well_name="Well B1",
        production_period="2023-02-01", days_on_production=28, oil_production_kbd=2.0,
        gas_production_mmcfd=1.0, liquids_production_kbd=0.4, water_production_kbd=0.2,
        data_source="TestSource", source_data="Raw2", partition_0="P2",
        created_at=datetime.now(), updated_at=datetime.now()
    )

# Make tests async
@pytest.mark.asyncio
async def test_save_and_get_well(well_repo, sample_production_entity):
    await well_repo.save(sample_production_entity)
    retrieved = await well_repo.get_by_well_code(sample_production_entity.well_code)
    assert len(retrieved) == 1
    assert retrieved[0].well_name == sample_production_entity.well_name

    # Test upsert (save again)
    sample_production_entity.well_name = "Well A1 Updated"
    await well_repo.save(sample_production_entity)
    retrieved_updated = await well_repo.get_by_well_code(sample_production_entity.well_code)
    assert len(retrieved_updated) == 1
    assert retrieved_updated[0].well_name == "Well A1 Updated"

@pytest.mark.asyncio
async def test_bulk_insert_new_and_duplicates(well_repo, sample_production_entity, sample_production_entity_2):
    entities = [sample_production_entity, sample_production_entity_2]
    # Convert entities to Polars DataFrame
    # Assuming WellProduction has a .model_dump() or similar method (like Pydantic BaseModel)
    # If not, manual conversion is needed.
    try:
        data_dicts = [p.model_dump(by_alias=False) for p in entities]
    except AttributeError: # Fallback for non-Pydantic or different method
        data_dicts = [p.__dict__ for p in entities]


    incoming_df = pl.DataFrame(data_dicts)

    # Ensure correct schema for Polars DataFrame, especially datetime
    schema_overrides = {
        "created_at": pl.Datetime, # Polars uses 'Datetime' not pl.Datetime for casting string name
        "updated_at": pl.Datetime
    }
    # Cast if columns exist and are not already datetime
    for col, dtype in schema_overrides.items():
        if col in incoming_df.columns and incoming_df[col].dtype != dtype:
             # Attempt conversion from string if they are strings, or handle other types
            if incoming_df[col].dtype == pl.Object or pl.datatypes.is_temporal(incoming_df[col].dtype):
                 # If object, assume it's already datetime or needs specific parsing not covered here
                 # If already temporal, no cast needed unless changing resolution/timezone
                 pass
            else: # General cast for other types, might fail if incompatible
                incoming_df = incoming_df.with_columns(pl.col(col).cast(dtype, strict=False))


    _, new_count, dup_count = await well_repo.bulk_insert(incoming_df)
    assert new_count == 2
    assert dup_count == 0

    total_records = await well_repo.count()
    assert total_records == 2

    # Insert again (all duplicates)
    _, new_count_2, dup_count_2 = await well_repo.bulk_insert(incoming_df)
    assert new_count_2 == 0
    # The ON CONFLICT DO NOTHING means the existing 2 records are "duplicates" in the sense that they were skipped.
    assert dup_count_2 == 2

    total_records_after_dup = await well_repo.count()
    assert total_records_after_dup == 2

@pytest.mark.asyncio
async def test_count(well_repo, sample_production_entity):
    assert await well_repo.count() == 0
    await well_repo.save(sample_production_entity)
    assert await well_repo.count() == 1

@pytest.mark.asyncio
async def test_get_existing_composite_keys(well_repo, sample_production_entity):
    await well_repo.save(sample_production_entity)
    keys_to_check = [
        (sample_production_entity.well_code, sample_production_entity.field_code, sample_production_entity.production_period),
        (999, 999, "2000-01-01") # Non-existent key
    ]
    existing_keys = await well_repo.get_existing_composite_keys(keys_to_check)
    assert len(existing_keys) == 1
    # Convert sample_production_entity key parts to match what DB might return (e.g. int for codes)
    expected_key = (
        int(sample_production_entity.well_code),
        int(sample_production_entity.field_code),
        sample_production_entity.production_period
    )
    assert expected_key in existing_keys

# TODO: Add test for export_to_csv. This might be more of an integration test
# due to file system interaction, but can be unit-tested by mocking Path.write_text
# or by checking if the file is created in tmp_path and has expected content.
@pytest.mark.asyncio
async def test_export_to_csv(well_repo, sample_production_entity, tmp_path):
    await well_repo.save(sample_production_entity)

    # The repo's downloads_dir is already set to a tmp_path subdirectory
    csv_file_path = await well_repo.export_to_csv()

    assert csv_file_path.exists()

    # Basic check: read the CSV and verify header and one data row (excluding header)
    df_from_csv = pl.read_csv(csv_file_path)
    assert len(df_from_csv) == 1
    # Check a couple of values, ensure types are handled (e.g. well_code read as int)
    assert df_from_csv["well_code"][0] == sample_production_entity.well_code
    assert df_from_csv["well_name"][0] == sample_production_entity.well_name
    # Timestamps might need careful comparison due to string formatting in CSV
    # For simplicity, we'll skip direct timestamp content check here.

    # Clean up the specific export file if needed, though tmp_path handles overall cleanup
    # csv_file_path.unlink(missing_ok=True) # Usually handled by tmp_path fixture
```
