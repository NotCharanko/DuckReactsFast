### main.py
import logging
import sys
import duckdb
from pathlib import Path
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from contextlib import asynccontextmanager
from datetime import datetime

# Import settings and dependency configuration
from src.shared.config.settings import get_settings
from src.shared.dependencies import configure_dependencies

# Import API routes from the interfaces layer
from .interfaces.api.well_production_routes import router as well_production_router

# Ensure logs directory exists
Path("logs").mkdir(exist_ok=True)

# Configure logging with both console and file handlers
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("logs/wells_api.log", mode="a")
    ]
)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan management."""
    logger.info("Starting Well Production API...")
    settings = get_settings()

    # Initialize shared DuckDB connection for the application lifecycle.
    # The connection is created at startup and closed at shutdown.
    # This ensures all parts of the application (e.g., repositories, job manager)
    # use the same database connection, facilitating transaction management (if needed)
    # and connection pooling by DuckDB itself.
    db_path = Path(settings.DATA_ROOT_DIR) / settings.DUCKDB_FILENAME
    db_path.parent.mkdir(parents=True, exist_ok=True) # Ensure data directory exists
    duckdb_connection = duckdb.connect(str(db_path), read_only=False)
    logger.info(f"DuckDB connection established to: {db_path}")

    app_config = {
        "external_api": {
            "base_url": settings.API_BASE_URL,
            "api_key": settings.API_KEY,
            "mock_mode": settings.ENV != "production", # Example: mock_mode is False if ENV is production
            "mock_file_path": str(settings.MOCKED_RESPONSE_PATH),
            "timeout_seconds": 30, # Or make these configurable too
            "max_retries": 3,
            "retry_delay_seconds": 1.0
        },
        "repository_paths": { # Add a new section for repository paths
            "data_dir": str(settings.DATA_ROOT_DIR),
            "downloads_dir": "downloads",
            "duckdb_filename": settings.DUCKDB_FILENAME,
            "csv_filename": settings.CSV_EXPORT_FILENAME
        },
        "duckdb_connection": duckdb_connection, # Provide the shared connection to the dependency container
        # Add other configurations like batch_processing if needed
    }
    # Configure global dependencies, making the app_config (including the db connection)
    # available to various components.
    configure_dependencies(config=app_config)

    yield # Application runs after this point

    # Cleanup: Close the shared DuckDB connection when the application shuts down.
    logger.info("Closing DuckDB connection...")
    duckdb_connection.close()
    logger.info("Shutting down Well Production API...")

app = FastAPI(
    title="Well Production API",
    description="High-performance API for managing millions of well production records using DuckDB with on-demand CSV export",
    version="1.0.0",
    lifespan=lifespan
)

# Mount static files under /static path
app.mount("/static", StaticFiles(directory="src", html=True), name="static")

# Fetch settings for CORS configuration
settings = get_settings()

# Configure CORS for production
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ALLOWED_ORIGINS, # Use configured origins
    allow_credentials=True,
    allow_methods=["GET", "POST"], # Consider making this configurable too if needed
    allow_headers=["*"], # Consider making this configurable too if needed
)

# Include API routes
app.include_router(well_production_router)

@app.get("/")
async def root():
    """API root endpoint with information."""
    return {
        "message": "Well Production API",
        "version": "1.0.0",
        "features": {
            "storage": "DuckDB primary storage with on-demand CSV export",
            "performance": "Optimized for millions of records",
            "architecture": "Hexagonal Architecture with DDD principles"
        },
        "endpoints": {
            "import": "/api/v1/wells/import (POST with filters)",
            "import_trigger": "/api/v1/wells/import/trigger (GET - simple trigger)",
            "import_run": "/api/v1/wells/import/run (GET - OData API import with pagination)",
            "download": "/api/v1/wells/download",
            "stats": "/api/v1/wells/stats",
            "docs": "/docs"
        }
    }

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy", 
        "service": "well-production-api",
        "version": "1.0.0",
        "database": "duckdb",
        "timestamp": datetime.now().isoformat()
    }
