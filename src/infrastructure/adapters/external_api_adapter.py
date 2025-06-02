"""
Improved external API adapter with comprehensive error handling and recovery.
Implements the ExternalApiPort interface without tight coupling.
"""
import json
import logging
import asyncio
from typing import List, Dict, Any, Optional, AsyncGenerator # Added AsyncGenerator
from pathlib import Path
import httpx
from datetime import datetime
import polars as pl

from ...domain.ports.external_api_port import ExternalApiPort
# from ...domain.entities.well_production import WellProduction # No longer returning WellProduction entities directly
from ...shared.config.settings import get_settings # Added import
from ...shared.exceptions import (
    ExternalApiException, 
    FileSystemException,
    ValidationException
)
from ...shared.utils.timing_decorator import async_timed # Added import

logger = logging.getLogger(__name__)


class ExternalApiAdapter(ExternalApiPort):
    """
    Adapter for external API services with comprehensive error handling.
    Supports both real API calls and mock mode for development/testing.
    """
    
    def __init__(
        self,
        base_url: Optional[str] = None,
        api_key: Optional[str] = None,
        mock_mode: bool = True,
        mock_file_path: Optional[str] = None,
        timeout_seconds: int = 30,
        max_retries: int = 3,
        retry_delay_seconds: float = 1.0
    ):
        self.base_url = base_url
        self.api_key = api_key
        self.mock_mode = mock_mode
        self.mock_file_path = Path(mock_file_path) if mock_file_path else get_settings().MOCKED_RESPONSE_PATH
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self.retry_delay_seconds = retry_delay_seconds
        
        # Validate configuration
        if not mock_mode and not base_url:
            raise ValueError("base_url is required when not in mock mode")
    
    async def fetch_well_production_data(
        self, 
        endpoint: Optional[str] = None,
        filters: Optional[Dict[str, Any]] = None
    ) -> AsyncGenerator[pl.DataFrame, None]:
        """
        Fetch well production data from external source, yielding DataFrame chunks.
        In live mode, this involves paginated API calls. In mock mode, it splits
        a pre-loaded mock DataFrame into chunks.
        
        Args:
            endpoint: Optional specific endpoint
            filters: Optional filters to apply
            
        Yields:
            polars.DataFrame: Chunks of well production data.
            
        Raises:
            ExternalApiException: When API call fails
            ValidationException: When data validation fails (e.g., mock file issues)
        """
        if self.mock_mode:
            async for chunk_df in self._fetch_mock_data_streaming(filters=filters):
                yield chunk_df
        else:
            async for chunk_df in self._fetch_real_data_streaming(endpoint, filters):
                yield chunk_df
    
    async def validate_connection(self) -> bool:
        """
        Validate connection to external API.
        
        Returns:
            True if connection is valid, False otherwise
        """
        try:
            if self.mock_mode:
                # For mock mode, check if mock file exists
                return self.mock_file_path.exists()
            else:
                # For real API, try a simple health check
                async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
                    response = await client.get(f"{self.base_url}/health")
                    return response.status_code == 200
        except Exception as e:
            logger.warning(f"Connection validation failed: {str(e)}")
            return False
    
    async def get_api_status(self) -> Dict[str, Any]:
        """
        Get status information from external API.
        
        Returns:
            Dictionary with status information
        """
        try:
            if self.mock_mode:
                return {
                    "status": "mock_mode",
                    "mock_file_exists": self.mock_file_path.exists(),
                    "mock_file_path": str(self.mock_file_path),
                    "last_check": datetime.utcnow().isoformat()
                }
            else:
                is_connected = await self.validate_connection()
                return {
                    "status": "connected" if is_connected else "disconnected",
                    "base_url": self.base_url,
                    "timeout_seconds": self.timeout_seconds,
                    "last_check": datetime.utcnow().isoformat()
                }
        except Exception as e:
            logger.error(f"Error getting API status: {str(e)}")
            return {
                "status": "error",
                "error_message": str(e),
                "last_check": datetime.utcnow().isoformat()
            }
    
    @async_timed
    async def _fetch_mock_data_streaming(self, chunk_size: int = 1000, filters: Optional[Dict[str, Any]] = None) -> AsyncGenerator[pl.DataFrame, None]:
        """
        Fetch and stream data from mock file in chunks, returning Polars DataFrames.
        `filters` argument is kept for signature consistency but not used in mock streaming.
        """
        try:
            if not self.mock_file_path.exists():
                raise FileSystemException(
                    message=f"Mock file not found: {self.mock_file_path}",
                    file_path=str(self.mock_file_path)
                )
            
            logger.info(f"Loading and streaming mock data from {self.mock_file_path} in chunks of {chunk_size}")
            
            with open(self.mock_file_path, 'r', encoding='utf-8') as f:
                raw_data = json.load(f)
            
            wells_data_list = raw_data.get('value', raw_data)
            
            if not isinstance(wells_data_list, list):
                raise ValidationException(
                    message="Mock data must be a list or contain a 'value' field with a list of records.",
                    field="wells_data_format"
                )

            if not wells_data_list:
                logger.info("Mock data list is empty. No records to stream.")
                yield pl.DataFrame() # Yield an empty DataFrame if no data
                return

            full_df = pl.DataFrame(wells_data_list)
            # Apply field mapping to the whole DataFrame at once for consistency
            full_df = self._map_polars_df_fields(full_df)

            for i in range(0, full_df.height, chunk_size):
                chunk_df = full_df.slice(i, chunk_size)
                logger.debug(f"Yielding mock data chunk: offset={i}, size={chunk_df.height}")
                yield chunk_df
            
            logger.info(f"Successfully streamed {full_df.height} well production records from mock file.")
            
        except (FileSystemException, ValidationException):
            raise
        except json.JSONDecodeError as e:
            raise ValidationException(
                message=f"Invalid JSON in mock file: {str(e)}",
                field="json_format"
            )
        except Exception as e: # Includes Polars errors during DataFrame creation or slicing
            logger.error(f"Error processing mock data for streaming: {e}", exc_info=True)
            raise FileSystemException( # Or could be ValidationException depending on error type
                message=f"Error reading/processing mock file for streaming: {str(e)}",
                file_path=str(self.mock_file_path),
                cause=e
            )

    @async_timed
    async def _fetch_real_data_streaming(
        self, 
        endpoint: Optional[str] = None,
        filters: Optional[Dict[str, Any]] = None,
        page_size: int = 1000 # Example page size for pagination
    ) -> AsyncGenerator[pl.DataFrame, None]:
        """
        Fetch data from real external API with pagination and retry logic, yielding DataFrame chunks.
        """
        endpoint = endpoint or "/wells_production" # Default endpoint
        current_page = 0 # Or 1, depending on API's pagination scheme
        # For OData, pagination is often done with $skip and $top
        # For other APIs, it might be `page` and `limit`, or cursor-based.
        # This example assumes $skip/$top or page/limit style.
        
        while True:
            for attempt in range(self.max_retries):
                try:
                    # Determine query parameters for pagination
                    # This is a placeholder; actual params depend on the API
                    paginated_filters = filters.copy() if filters else {}
                    # Example for OData-like $skip/$top:
                    # paginated_filters['$skip'] = current_page * page_size
                    # paginated_filters['$top'] = page_size
                    # Example for page/limit:
                    paginated_filters['page'] = current_page
                    paginated_filters['limit'] = page_size

                    url = f"{self.base_url}{endpoint}"
                    logger.info(f"Fetching data from {url} (Page: {current_page}, Size: {page_size}, Attempt: {attempt + 1}/{self.max_retries})")

                    async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
                        headers = {}
                        if self.api_key:
                            headers["Authorization"] = f"Bearer {self.api_key}"
                        
                        response = await client.get(url, headers=headers, params=paginated_filters)
                        
                        if response.status_code == 200:
                            data = response.json()
                            records = data.get('value', data) # Common in OData, or might be 'results', 'data' etc.

                            if not isinstance(records, list):
                                raise ValidationException(
                                    message="API response for records is not a list.",
                                    endpoint=url,
                                    status_code=response.status_code
                                )

                            if not records: # No more data
                                logger.info(f"No more records found at page {current_page}. Concluding fetch.")
                                return

                            # Convert current page's records to DataFrame
                            try:
                                chunk_df = pl.DataFrame(records)
                                # Apply field mapping
                                chunk_df = self._map_polars_df_fields(chunk_df)
                            except Exception as e:
                                logger.error(f"Error converting API response page to Polars DataFrame: {e}", exc_info=True)
                                raise ValidationException(
                                    message=f"Could not convert API response to Polars DataFrame: {str(e)}",
                                    endpoint=url, status_code=response.status_code
                                )

                            logger.debug(f"Yielding API data chunk: page={current_page}, size={chunk_df.height}")
                            yield chunk_df

                            # Check if this was the last page (logic depends on API)
                            # Some APIs return a 'nextLink', others rely on checking if returned count < page_size
                            if len(records) < page_size: # Common way to detect last page
                                logger.info(f"Fetched {len(records)} records, which is less than page size {page_size}. Assuming last page.")
                                return

                            current_page += 1
                            break # Break from retry loop, proceed to next page
                        
                        else: # HTTP error
                            error_msg = f"API returned status {response.status_code} for page {current_page}"
                            if response.status_code >= 500 and attempt < self.max_retries - 1:
                                logger.warning(f"{error_msg}, retrying in {self.retry_delay_seconds}s...")
                                await asyncio.sleep(self.retry_delay_seconds * (attempt + 1))
                                continue # Retry this page

                            raise ExternalApiException(message=error_msg, endpoint=url, status_code=response.status_code)

                except httpx.TimeoutException:
                    error_msg = f"Request timeout for page {current_page} after {self.timeout_seconds}s"
                    if attempt < self.max_retries - 1:
                        logger.warning(f"{error_msg}, retrying in {self.retry_delay_seconds}s...")
                        await asyncio.sleep(self.retry_delay_seconds * (attempt + 1))
                        continue
                    raise ExternalApiException(message=error_msg, endpoint=url)

                except httpx.RequestError as e:
                    error_msg = f"Request error for page {current_page}: {str(e)}"
                    if attempt < self.max_retries - 1:
                        logger.warning(f"{error_msg}, retrying in {self.retry_delay_seconds}s...")
                        await asyncio.sleep(self.retry_delay_seconds * (attempt + 1))
                        continue
                    raise ExternalApiException(message=error_msg, endpoint=url, cause=e)

                except (ValidationException, ExternalApiException): # Propagate these immediately
                    raise
                except Exception as e: # Unexpected errors
                    logger.error(f"Unexpected error during API fetch for page {current_page}: {e}", exc_info=True)
                    if attempt < self.max_retries - 1: # Allow retry for unexpected errors too
                         logger.warning(f"Unexpected error, retrying page {current_page} in {self.retry_delay_seconds}s...")
                         await asyncio.sleep(self.retry_delay_seconds * (attempt + 1))
                         continue
                    raise ExternalApiException(message=f"Unexpected error: {str(e)}", endpoint=url, cause=e)
            else: # This else corresponds to the for loop for retries
                 # If all retries for a page fail
                raise ExternalApiException(
                    message=f"Failed to fetch page {current_page} after {self.max_retries} attempts",
                    endpoint=f"{self.base_url}{endpoint}"
                )
    
    def _map_polars_df_fields(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Map column names in a Polars DataFrame from external API format to a standard format.
        This ensures consistency for the service layer by aligning DataFrame schemas
        (e.g., renaming columns, ensuring all expected columns are present).
        
        Args:
            df: Input Polars DataFrame with API-specific column names.
            
        Returns:
            Polars DataFrame with standardized column names.
        """
        # Example field mappings (adjust as per actual API and desired schema)
        # These should align with WellProduction entity fields or a defined schema.
        field_mappings = {
            # API OData field name: Standard DataFrame column name
            "FIELD_CODE": "field_code", # Ensure case consistency if API is case-insensitive
            "FIELD_NAME": "field_name", # Or map from '_field_name' if that's the JSON key
            "WELL_CODE": "well_code",
            "WELL_REFERENCE": "well_reference", # Or map from '_well_reference'
            "WELL_NAME": "well_name",
            "PRODUCTION_PERIOD": "production_period",
            "DAYS_ON_PRODUCTION": "days_on_production",
            "OIL_PRODUCTION_KBD": "oil_production_kbd",
            "GAS_PRODUCTION_MMCFD": "gas_production_mmcfd",
            "LIQUIDS_PRODUCTION_KBD": "liquids_production_kbd",
            "WATER_PRODUCTION_KBD": "water_production_kbd",
            "DATA_SOURCE": "data_source",
            "SOURCE_DATA": "source_data", # This might be the raw JSON string or similar
            "PARTITION_0": "partition_0",
            "CREATED_AT": "created_at", # Ensure datetime parsing if these are strings
            "UPDATED_AT": "updated_at",
            # Add any other specific mappings from the API
            # For example, if the API uses '_field_name', map it to 'field_name'
             '_field_name': 'field_name',
             '_well_reference': 'well_reference'
        }
        
        # Select and rename columns
        # Only include columns that are expected by the service/domain.
        # This also handles cases where some API fields might not be in field_mappings.

        # First, create a list of rename operations for columns that exist in the DataFrame
        # and are part of our mapping.
        rename_ops = {}
        current_columns = df.columns

        for api_col_name, standard_col_name in field_mappings.items():
            # Check if the API column name (case-insensitive) is in the DataFrame
            # Polars column names are case-sensitive. If API is not, this needs careful handling.
            # Assuming API field names in `field_mappings` match what Polars infers from JSON.
            if api_col_name in current_columns:
                if api_col_name != standard_col_name: # Only rename if different
                     rename_ops[api_col_name] = standard_col_name
            # If API uses a different casing but Polars infers a specific one, adjust `field_mappings` keys.

        # Apply renames
        if rename_ops:
            df = df.rename(rename_ops)

        # Ensure all *standard* columns are present, fill with null if missing from source
        # This is important for schema consistency downstream.
        # The schema should ideally be defined elsewhere (e.g., from WellProduction entity fields)
        # For now, let's assume field_mappings.values() are the desired final columns.
        standard_columns_expected = list(dict.fromkeys(field_mappings.values())) # Unique standard names

        for col_name in standard_columns_expected:
            if col_name not in df.columns:
                # Add the missing column, filled with nulls.
                # The type should ideally match the target schema.
                # For simplicity, Polars will use a default null type or one can specify.
                # Example: df = df.with_columns(pl.lit(None, dtype=pl.Utf8).alias(col_name))
                # For now, let Polars infer or use a generic null.
                df = df.with_columns(pl.lit(None).alias(col_name))

        # Select only the standard columns in the desired order (optional, but good practice)
        # df = df.select(standard_columns_expected)

        # Placeholder for type casting (example)
        # This should be robust and handle potential errors.
        # For example, ensuring numeric columns are numeric, dates are dates.
        # df = df.with_columns([
        #     pl.col("field_code").cast(pl.Int64, strict=False),
        #     pl.col("well_code").cast(pl.Int64, strict=False),
        #     pl.col("days_on_production").cast(pl.Float64, strict=False), # Or Int64 if always whole numbers
        #     pl.col("oil_production_kbd").cast(pl.Float64, strict=False),
        #     # ... other numeric fields
        #     # For dates, if they are strings:
        #     # pl.col("production_period").str.strptime(pl.Date, "%Y-%m-%d", strict=False).alias("production_period"),
        #     # pl.col("created_at").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%SZ", strict=False).alias("created_at"),
        # ])
        
        return df