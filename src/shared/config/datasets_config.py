# This module provides a template for configuring different datasets.
# While not fully integrated into a generic processing engine in the current version,
# it demonstrates how datasets could be defined with their specific components
# (schemas, database tables, services, adapters, etc.) for future extensibility.

# src/config/datasets_config.py
# Corrected import paths based on project structure
from src.domain.entities.well_production import WellProduction
# Import paths for repository and service will be strings, so direct imports not needed here for the config value itself.
# from src.infrastructure.db.duckdb_repo import DuckDBWellRepo # Old
# from src.application.services.wells_service import WellService # Old
# from src.infrastructure.external.pandas_csv_exporter import PandasCsvExporter # Old
# from src.application.services.fetchers import fetch_well_production_data_then_parse # Old

DATASETS = {
    "wells_production": {
        "schema": WellProduction,
        "table": "wells_production", # DB table name
        "sql_path": "src/infrastructure/operations/wells.sql", # Path to SQL file for this dataset
        # Example service for handling operations like import for this dataset
        "service": "src.application.services.well_production_import_service.WellProductionImportService",
        # Example repository for data access
        "repo": "src.infrastructure.repositories.duckdb_well_production_repository.DuckDBWellProductionRepository",
        # fetcher: # This would typically configure an adapter, e.g., ExternalApiAdapter with specific endpoint/credentials.
        # exporter: # CSV export is currently handled by DuckDBWellProductionRepository.export_to_csv()
        "export_path": "downloads/wells_prod.csv", # Default export path, actual path managed by repo
    },
    # Add more datasets here
}

def get_dataset_config(dataset_name: str):
    config = DATASETS.get(dataset_name)
    if not config:
        # It's better to raise a specific exception or return None,
        # depending on how this function is used.
        # Raising HTTPException here makes this utility function FastAPI-dependent.
        # Consider: raise KeyError(f"Dataset '{dataset_name}' not found.")
        from fastapi import HTTPException # Keep for now if this is the desired behavior in current context
        raise HTTPException(status_code=404, detail=f"Dataset configuration for '{dataset_name}' not found")
    return config
