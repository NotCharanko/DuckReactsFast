import duckdb
import csv
import asyncio
from pathlib import Path
from typing import List, Optional, Set, Tuple
from datetime import datetime
import polars as pl

from ...domain.entities.well_production import WellProduction
from ...domain.repositories.well_production_repository import WellProductionRepository
from ...shared.utils.sql_loader import load_sql
from ...shared.utils.timing_decorator import async_timed, timed

class DuckDBWellProductionRepository(WellProductionRepository):
    """
    DuckDB implementation of the well production repository.
    This repository uses a shared DuckDB connection provided during initialization
    to interact with the 'well_production' table. It handles operations like
    data insertion (single and bulk), querying, and CSV export.
    The shared connection is managed externally (e.g., in main.py lifespan).
    """
    
    # Bulk processing configuration for CSV export
    BATCH_SIZE = 100_000  # Number of records per batch
    MEMORY_LIMIT = "6GB"  # Leave 2GB for system and other processes
    THREADS = 4  # Number of threads for parallel processing
    TEMP_DIR = Path("temp")  # Directory for temporary files during export
    
    def __init__(
        self, 
        conn: duckdb.DuckDBPyConnection, # Changed: accept connection object
        sql_path: Path = None,
        downloads_dir: Path = Path("downloads"),
        csv_filename: str = "wells_prod.csv"
    ):
        self.conn = conn # Changed: store connection object
        # self.db_path.parent.mkdir(exist_ok=True) # Removed: DB path managed in main.py
        
        # CSV export configuration
        self.downloads_dir = downloads_dir
        self.downloads_dir.mkdir(parents=True, exist_ok=True)
        self.csv_path = self.downloads_dir / csv_filename
        self.TEMP_DIR.mkdir(parents=True, exist_ok=True)
        
        # Load SQL queries from file
        if sql_path is None:
            sql_path = Path(__file__).parent.parent / "operations" / "wells.sql"
        self.queries = load_sql(str(sql_path))
        
        self._initialize_database()
    
    def _initialize_database(self):
        """Initialize the DuckDB database and create the table if it doesn't exist."""
        # Use the shared connection
        self.conn.execute(self.queries['create_table'])
        self.conn.execute(self.queries['create_indexes'])
    
    async def get_by_well_code(self, well_code: int) -> List[WellProduction]:
        """Get well production data by well code."""
        def _get_by_well_code_sync():
            # Use the shared connection
            results = self.conn.execute(
                self.queries['get_by_well_code'],
                [well_code]
            ).fetchall()
            return [self._row_to_entity(row) for row in results]
        
        return await asyncio.to_thread(_get_by_well_code_sync)
    
    async def get_by_field_code(self, field_code: int) -> List[WellProduction]:
        """Get all well production data for a field."""
        def _get_by_field_code_sync():
            # Use the shared connection
            results = self.conn.execute(
                self.queries['get_by_field_code'],
                [field_code]
            ).fetchall()
            return [self._row_to_entity(row) for row in results]
        
        return await asyncio.to_thread(_get_by_field_code_sync)
    
    # The `save` method, utilizing the 'insert_single' SQL query, performs an "upsert"
    # operation due to 'INSERT OR REPLACE'. If a record with the same primary key
    # (well_code, field_code, production_period) exists, it will be overwritten.
    # This is suitable for scenarios where individual records are updated, and the
    # latest version should replace any existing version.
    async def save(self, well_production: WellProduction) -> WellProduction:
        """Save well production data."""
        def _save_sync():
            # Use the shared connection
            self.conn.execute(
                self.queries['insert_single'],
                self._entity_to_params(well_production)
            )
            return well_production
        
        return await asyncio.to_thread(_save_sync)
    
    async def update(self, well_production: WellProduction) -> WellProduction:
        """Update well production data."""
        return await self.save(well_production)  # DuckDB handles upsert with INSERT OR REPLACE
    
    def _get_existing_composite_keys_sync(self, composite_keys: List[Tuple]) -> Set[Tuple]:
        """Synchronous helper to get existing composite keys."""
        if not composite_keys:
            return set()
        
        # Use the shared connection
        # Create a temporary table with the keys to check
        placeholders = ','.join(['(?, ?, ?)'] * len(composite_keys))
        query = f"""
            WITH input_keys AS (
                VALUES {placeholders}
            )
            SELECT DISTINCT well_code, field_code, production_period 
            FROM well_production 
            WHERE (well_code, field_code, production_period) IN (
                SELECT * FROM input_keys
            )
            """
            
            # Flatten the composite keys for the query
            flat_keys = [item for sublist in composite_keys for item in sublist]
            results = self.conn.execute(query, flat_keys).fetchall() # Use self.conn
            return set(results)
    
    @async_timed
    async def get_existing_composite_keys(self, composite_keys: List[Tuple]) -> Set[Tuple]:
        """Get existing composite keys for duplicate detection."""
        return await asyncio.to_thread(self._get_existing_composite_keys_sync, composite_keys)
    
    @timed
    def _bulk_insert_sync(self, incoming_df: pl.DataFrame) -> Tuple[List[WellProduction], int, int]:
        """Synchronous helper for bulk insert operations using Polars and ON CONFLICT DO NOTHING."""
        if incoming_df.is_empty():
            return [], 0, 0

        # Use the shared connection
        self.conn.register('incoming_productions_df', incoming_df)

        count_before = self.conn.execute("SELECT COUNT(*) FROM well_production").fetchone()[0] # Use self.conn

        insert_query = f"""
        INSERT INTO well_production
        SELECT * FROM incoming_productions_df
        ON CONFLICT (well_code, field_code, production_period) DO NOTHING;
        """
        self.conn.execute(insert_query) # Use self.conn

        count_after = self.conn.execute("SELECT COUNT(*) FROM well_production").fetchone()[0] # Use self.conn

        new_records_count = count_after - count_before
        total_incoming = incoming_df.height
        duplicate_count = total_incoming - new_records_count

        self.conn.unregister('incoming_productions_df') # Use self.conn (though unregister is on the connection itself)

        return [], new_records_count, duplicate_count
    
    @async_timed
    # The `bulk_insert` method, using an 'ON CONFLICT DO NOTHING' strategy within its
    # '_bulk_insert_sync' helper, inserts new records and skips any incoming records
    # that would cause a primary key conflict (i.e., duplicates).
    # This approach is generally preferred for large data ingestions where efficiency
    # is key, and skipping existing records is the desired behavior rather than
    # overwriting them.
    async def bulk_insert(self, incoming_df: pl.DataFrame) -> Tuple[List[WellProduction], int, int]:
        """
        Bulk insert well production data with duplicate detection from a Polars DataFrame.
        
        Returns:
            Tuple of (inserted_records, new_records_count, duplicate_records_count)
        """
        return await asyncio.to_thread(self._bulk_insert_sync, incoming_df)
    
    async def get_all(self) -> List[WellProduction]:
        """Get all well production data."""
        def _get_all_sync():
            # Use the shared connection
            results = self.conn.execute(self.queries['get_all']).fetchall()
            return [self._row_to_entity(row) for row in results]
        
        return await asyncio.to_thread(_get_all_sync)
    
    async def count(self) -> int:
        """Get the total count of well production records."""
        def _count_sync():
            # Use the shared connection
            result = self.conn.execute(self.queries['count_all']).fetchone()
            return result[0] if result else 0
        
        return await asyncio.to_thread(_count_sync)
    
    def _entity_to_params(self, well_production: WellProduction) -> list:
        """Convert a WellProduction entity to a list of parameters for SQL queries."""
        return [
            well_production.field_code,
            well_production.field_name,
            well_production.well_code,
            well_production.well_reference,
            well_production.well_name,
            well_production.production_period,
            well_production.days_on_production,
            well_production.oil_production_kbd,
            well_production.gas_production_mmcfd,
            well_production.liquids_production_kbd,
            well_production.water_production_kbd,
            well_production.data_source,
            well_production.source_data,
            well_production.partition_0,
            well_production.created_at,
            well_production.updated_at
        ]
    
    def _row_to_entity(self, row: tuple) -> WellProduction:
        """Convert a database row to a WellProduction entity."""
        return WellProduction(
            field_code=row[0],
            field_name=row[1],
            well_code=row[2],
            well_reference=row[3],
            well_name=row[4],
            production_period=row[5],
            days_on_production=row[6],
            oil_production_kbd=row[7],
            gas_production_mmcfd=row[8],
            liquids_production_kbd=row[9],
            water_production_kbd=row[10],
            data_source=row[11],
            source_data=row[12],
            partition_0=row[13],
            created_at=row[14],
            updated_at=row[15]
        )

    @timed
    def _export_to_csv_sync(self) -> Path:
        """Synchronous helper for CSV export operations."""
        try:
            # Use the shared connection
            # Configure DuckDB for optimal performance (potentially affects shared connection)
            self.conn.execute(f"PRAGMA memory_limit='{self.MEMORY_LIMIT}'")
            self.conn.execute(f"PRAGMA threads={self.THREADS}")
            
            # Get total count for progress tracking
            total_count = self.conn.execute("SELECT COUNT(*) FROM well_production").fetchone()[0]
            
            if total_count <= self.BATCH_SIZE:
                # For smaller datasets, export directly with optimized settings
                self.conn.execute(f"""
                    COPY (
                        SELECT 
                            field_code::VARCHAR as field_code,
                            field_name,
                            well_code::VARCHAR as well_code,
                            well_reference,
                            well_name,
                            production_period,
                            days_on_production::VARCHAR as days_on_production,
                            oil_production_kbd::VARCHAR as oil_production_kbd,
                            gas_production_mmcfd::VARCHAR as gas_production_mmcfd,
                            liquids_production_kbd::VARCHAR as liquids_production_kbd,
                            water_production_kbd::VARCHAR as water_production_kbd,
                            data_source,
                            source_data,
                            partition_0,
                            created_at::VARCHAR as created_at,
                            updated_at::VARCHAR as updated_at
                        FROM well_production
                        ORDER BY well_code, field_code, production_period
                    ) TO '{self.csv_path}' (
                        HEADER, 
                        DELIMITER ',',
                        QUOTE '"',
                        ESCAPE '"',
                        NULL 'NULL',
                        FORCE_QUOTE (field_name, well_reference, well_name, production_period, data_source, source_data, partition_0)
                    );
                """)
            else:
                # For larger datasets, use parallel export with temporary files
                # First, write headers
                with open(self.csv_path, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=self._get_fieldnames())
                    writer.writeheader()
                
                # Calculate number of chunks for parallel processing
                num_chunks = (total_count + self.BATCH_SIZE - 1) // self.BATCH_SIZE
                temp_files = []
                
                # Create temporary files for parallel export
                for i in range(num_chunks):
                    temp_file = self.TEMP_DIR / f"temp_export_{i}.csv"
                    temp_files.append(temp_file)
                    
                    # Export chunk to temporary file
                    self.conn.execute(f"""
                        COPY (
                            SELECT 
                                field_code::VARCHAR as field_code,
                                field_name,
                                well_code::VARCHAR as well_code,
                                well_reference,
                                well_name,
                                production_period,
                                days_on_production::VARCHAR as days_on_production,
                                oil_production_kbd::VARCHAR as oil_production_kbd,
                                gas_production_mmcfd::VARCHAR as gas_production_mmcfd,
                                liquids_production_kbd::VARCHAR as liquids_production_kbd,
                                water_production_kbd::VARCHAR as water_production_kbd,
                                data_source,
                                source_data,
                                partition_0,
                                created_at::VARCHAR as created_at,
                                updated_at::VARCHAR as updated_at
                            FROM well_production
                            ORDER BY well_code, field_code, production_period
                            LIMIT {self.BATCH_SIZE} OFFSET {i * self.BATCH_SIZE}
                        ) TO '{temp_file}' (
                            HEADER FALSE, 
                            DELIMITER ',',
                            QUOTE '"',
                            ESCAPE '"',
                            NULL 'NULL',
                            FORCE_QUOTE (field_name, well_reference, well_name, production_period, data_source, source_data, partition_0)
                        );
                    """)
                
                # Combine temporary files into final CSV
                with open(self.csv_path, 'ab') as outfile:
                    for temp_file in temp_files:
                        with open(temp_file, 'rb') as infile:
                            outfile.write(infile.read())
                        # Clean up temporary file
                        temp_file.unlink()
            
            # conn.close() # Removed: Do not close the shared connection here
            return self.csv_path
            
        except Exception as e:
            # Clean up any temporary files in case of error
            for temp_file in self.TEMP_DIR.glob("temp_export_*.csv"):
                try:
                    temp_file.unlink()
                except:
                    pass
            raise e

    @async_timed
    async def export_to_csv(self) -> Path:
        """Export all data from DuckDB to CSV for download using DuckDB's native export."""
        try:
            return await asyncio.to_thread(self._export_to_csv_sync)
        except Exception as e:
            # Fallback to the old method if DuckDB export fails
            well_productions = await self.get_all()
            await self._bulk_save_to_csv(well_productions, overwrite=True)
            return self.csv_path

    def _get_fieldnames(self) -> List[str]:
        """Get CSV fieldnames in the correct order."""
        return [
            'field_code', 'field_name', 'well_code', 'well_reference', 'well_name',
            'production_period', 'days_on_production', 'oil_production_kbd',
            'gas_production_mmcfd', 'liquids_production_kbd', 'water_production_kbd',
            'data_source', 'source_data', 'partition_0', 'created_at', 'updated_at'
        ]

    def _entity_to_row(self, entity: WellProduction) -> dict:
        """Convert WellProduction entity to CSV row dictionary."""
        return {
            'field_code': str(entity.field_code),
            'field_name': entity.field_name,
            'well_code': str(entity.well_code),
            'well_reference': entity.well_reference,
            'well_name': entity.well_name,
            'production_period': entity.production_period,
            'days_on_production': str(entity.days_on_production),
            'oil_production_kbd': str(entity.oil_production_kbd),
            'gas_production_mmcfd': str(entity.gas_production_mmcfd),
            'liquids_production_kbd': str(entity.liquids_production_kbd),
            'water_production_kbd': str(entity.water_production_kbd),
            'data_source': entity.data_source,
            'source_data': entity.source_data,
            'partition_0': entity.partition_0,
            'created_at': entity.created_at.isoformat() if entity.created_at else '',
            'updated_at': entity.updated_at.isoformat() if entity.updated_at else ''
        }

    async def _bulk_save_to_csv(self, well_productions: List[WellProduction], overwrite: bool = False) -> None:
        """Fallback method to save data to CSV using Python (slower but more reliable)."""
        mode = 'w' if overwrite else 'a'
        file_exists = self.csv_path.exists()
        
        with open(self.csv_path, mode, newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=self._get_fieldnames())
            
            # Write header if file is new or we're overwriting
            if overwrite or not file_exists:
                writer.writeheader()
            
            for well_production in well_productions:
                writer.writerow(self._entity_to_row(well_production)) 