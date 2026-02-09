# GOESMetadataCatalog

## Overview

The `GOESMetadataCatalog` class provides a comprehensive, high-performance solution for scanning, indexing, and managing metadata from large collections of GOES ABI L2+ NetCDF files. It operates in a lightweight manner by extracting only metadata without loading full data arrays, making it extremely efficient for processing massive GOES data archives containing thousands or millions of files.

### Design Philosophy

The catalog is designed around these core principles:

- **Metadata-First**: Extract only essential information, leaving data loading for downstream processing
- **Parallel Performance**: Leverage multi-core systems for concurrent file processing
- **Incremental Building**: Support for updating existing catalogs with new files
- **Robust Validation**: Comprehensive file structure and naming convention validation
- **Query-Ready**: Organized metadata enables efficient filtering and selection operations

## Key Features

### 🚀 **Performance Features**
- **Parallel Processing**: Uses ThreadPoolExecutor for concurrent file scanning across multiple CPU cores
- **Memory Efficient**: Only metadata extraction, no data array loading - can process millions of files
- **Incremental Updates**: Add new files to existing catalogs without reprocessing everything
- **CSV Persistence**: Human-readable catalog files that can be version controlled and shared

### 🔍 **Metadata Features**
- **Comprehensive Validation**: Validates GOES file structure, naming conventions, and content integrity
- **Rich Metadata Extraction**: Extracts observation-level metadata, band statistics, and data quality metrics
- **Attribute Promotion**: Converts NetCDF global attributes to time-indexed variables for proper concatenation
- **Quality Tracking**: Detailed error logging and processing statistics

### 📊 **Query Features**
- **Flexible Filtering**: Filter by time range, platform, orbital slot, scene ID, and custom criteria
- **Statistical Analysis**: Built-in band statistics for data quality assessment
- **Error Analysis**: Comprehensive error tracking and reporting
- **Provenance Tracking**: Complete processing history and file lineage

## Architecture Overview

```
GOES NetCDF Files → Parallel Scanner → Metadata Extractor → Validation Engine → Catalog Storage
                      ↓                    ↓                    ↓                    ↓
                File Discovery    Attribute Promotion    Quality Checks    CSV Files
                      ↓                    ↓                    ↓                    ↓
                ThreadPool         Statistics Calc      Error Logging    Query Interface
```

## Class Structure

### Initialization and Setup

```python
# Basic initialization
catalog = GOESMetadataCatalog(output_dir='./catalog')

# With custom configuration
catalog = GOESMetadataCatalog(
    output_dir='./catalog',
    sample_size=10,           # More files for better statistics
    validate_strict=True      # Strict validation for production
)
```

**Constructor Parameters:**
- **`output_dir`** (str | Path): Directory for CSV outputs and catalog storage
  - Creates subdirectories for different catalog components
  - Must have write permissions
  - Recommended: use absolute paths for reproducibility

### Core Processing Methods

#### Primary File Scanning

```python
# Scan files with default settings
catalog.scan_files(file_list)

# Scan with custom worker count
catalog.scan_files(file_list, max_workers=8)

# Scan with progress tracking
catalog.scan_files(file_list, show_progress=True)
```

**Method Signature:**
```python
scan_files(
    file_list: List[Union[str, Path]], 
    max_workers: Optional[int] = None,
    show_progress: bool = False
) -> None
```

**Processing Pipeline:**
1. **File Discovery Validation**: Checks file existence and basic accessibility
2. **Naming Convention Validation**: Validates GOES filename patterns and extracts metadata
3. **NetCDF Structure Validation**: Ensures proper NetCDF format and required variables
4. **Attribute Promotion**: Extracts global attributes and promotes to time-indexed variables
5. **Band Statistics Computation**: Calculates per-band statistics (min, max, mean, std, valid pixels)
6. **Quality Validation**: Validates platform IDs, orbital slots, scene IDs, and temporal consistency
7. **Error Tracking**: Records any processing errors with full context and stack traces
8. **Catalog Update**: Updates in-memory catalog with extracted metadata

#### Catalog Persistence

```python
# Export all catalog components to CSV
catalog.to_csv()

# Export specific components
catalog.to_observations_csv()
catalog.to_band_stats_csv()
catalog.to_errors_csv()
```

**Export Methods:**
- **`to_csv()`**: Complete catalog export (observations, band_stats, errors)
- **`to_observations_csv()`**: Main observation metadata only
- **`to_band_stats_csv()`**: Band statistics only
- **`to_errors_csv()`**: Error log only

#### Catalog Loading and Querying

```python
# Load catalog from CSV files
catalog = GOESMetadataCatalog.from_csv('./catalog')

# Load specific components
observations_df = catalog.load_observations()
band_stats_df = catalog.load_band_stats()
errors_df = catalog.load_errors()

# Query operations
filtered = catalog.filter_by_time('2024-01-01', '2024-01-31')
g18_data = catalog.filter_by_platform('G18')
east_data = catalog.filter_by_orbital_slot('GOES-East')
```

**Query Methods:**
- **`load_observations()`**: Returns pandas DataFrame with observation metadata
- **`load_band_stats()`**: Returns pandas DataFrame with per-band statistics
- **`load_errors()`**: Returns pandas DataFrame with processing errors
- **`filter_by_time(start, end)`**: Filter observations by time range
- **`filter_by_platform(platform_id)`**: Filter by GOES satellite (G16, G17, G18, G19)
- **`filter_by_orbital_slot(slot)`**: Filter by orbital position (GOES-East, GOES-West, etc.)

## Data Model

### Promoted Attributes

The catalog promotes NetCDF global attributes to time-indexed variables for proper concatenation and provenance tracking:

**Identity Attributes:**
- `observation_id`: UUID per product
- `dataset_name`: Original filename
- `naming_authority`: gov.nesdis.noaa

**Satellite/Instrument Attributes:**
- `platform_id`: GOES satellite identifier (G16, G17, G18, G19)
- `orbital_slot`: Orbital position (GOES-East, GOES-West, GOES-Test, GOES-Storage)
- `instrument_type`: Instrument type (e.g., GOES R Series ABI)
- `instrument_id`: Instrument identifier

**Temporal Attributes:**
- `time_coverage_start`: Observation start time
- `time_coverage_end`: Observation end time
- `timeline_id`: Scan mode (Full Disk, CONUS, Mesoscale)

**Quality Attributes:**
- `spatial_resolution`: Spatial resolution in km
- `scene_id`: Scene identifier
- `processing_level`: Data processing level

### Validation Rules

**Platform Validation:**
- Valid platforms: G16, G17, G18, G19
- Valid orbital slots: GOES-East, GOES-West, GOES-Test, GOES-Storage
- Valid scene IDs: Full Disk, CONUS, Mesoscale

**Filename Pattern:**
```
OR_ABI-L2-MCMIP[FCM]-M\d+_G\d\d+_s(\d{14})_e(\d{14})_c(\d{14})\.nc
```

**Components:**
- `OR_ABI-L2-MCMIP[FCM]-M\d+`: Product and mode identifier
- `G\d\d+`: Platform identifier
- `s(\d{14})`: Start time (YYYYMMDDHHMMSS)
- `e(\d{14})`: End time (YYYYMMDDHHMMSS)
- `c(\d{14})`: Creation time (YYYYMMDDHHMMSS)

### Band Statistics

For each band (1-16), the catalog extracts:
- Minimum value
- Maximum value
- Mean value
- Standard deviation
- Valid pixel count
- Total pixel count
- Data quality flag distribution

## Usage Examples

### Basic Catalog Building

```python
import glob
from goesdatabuilder.data.goes.multicloudcatalog import GOESMetadataCatalog

# Initialize catalog
catalog = GOESMetadataCatalog(output_dir='./catalog')

# Scan GOES files
goes_files = glob.glob('/data/GOES18/**/*.nc')
catalog.scan_files(goes_files)

# Export to CSV
catalog.to_csv()

# Load and query
df = catalog.load_observations()
print(f"Processed {len(df)} observations")
```

### Advanced Querying

```python
# Filter by multiple criteria
filtered_df = df[
    (df['platform_id'] == 'G18') &
    (df['orbital_slot'] == 'GOES-East') &
    (df['scene_id'] == 'Full Disk')
]

# Time-based filtering
january_data = catalog.filter_by_time('2024-01-01', '2024-01-31')

# Error analysis
errors_df = catalog.load_errors()
print(f"Processing errors: {len(errors_df)}")
```

### Incremental Updates

```python
# Load existing catalog
existing_catalog = GOESMetadataCatalog(output_dir='./catalog')
existing_df = existing_catalog.load_observations()

# Scan new files only
new_files = [f for f in all_files if f not in existing_df['filepath'].tolist()]
catalog.scan_files(new_files)

# Merge and export
catalog.to_csv()
```

## Performance Considerations

### Memory Usage
- Only metadata is loaded, not full data arrays
- Band statistics computed incrementally
- CSV persistence allows for memory-efficient processing

### Parallel Processing
- Default uses all available CPU cores
- I/O-bound operations benefit from parallelization
- Configurable worker count for resource-constrained environments

### Error Handling
- Failed files don't stop processing
- Detailed error context preserved
- Error logs available for debugging

## Output Files

### CSV Files Generated

1. **observations.csv**: Main observation metadata
2. **band_stats.csv**: Per-band statistics
3. **errors.csv**: Processing error log

### File Structure

```
catalog/
├── observations.csv     # Main metadata catalog
├── band_stats.csv      # Band-level statistics
├── errors.csv          # Processing errors
└── catalog_info.json   # Catalog metadata
```

## Integration with Pipeline

The `GOESMetadataCatalog` integrates seamlessly with the GOES data processing pipeline:

```
Raw GOES Files → GOESMetadataCatalog → GOESMultiCloudObservation → GeostationaryRegridder → GOESZarrStore
```

1. **Discovery**: Catalog discovers and validates raw GOES files
2. **Selection**: Query interface selects files for processing
3. **Provenance**: Provides metadata for downstream processing
4. **Tracking**: Maintains processing history and statistics

## Best Practices

1. **Regular Updates**: Schedule periodic catalog updates to capture new data
2. **Error Monitoring**: Review error logs for data quality issues
3. **Storage Planning**: Consider storage requirements for large catalogs
4. **Backup Strategy**: Implement backup procedures for catalog files
5. **Validation**: Periodically validate catalog integrity against source files

## Troubleshooting

### Common Issues

1. **Memory Errors**: Reduce `max_workers` or process files in batches
2. **Permission Errors**: Ensure write access to output directory
3. **Invalid Files**: Check error logs for problematic files
4. **Slow Processing**: Consider SSD storage for better I/O performance

### Debug Mode

Enable debug logging for detailed processing information:

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

## API Reference

### Constructor

```python
GOESMetadataCatalog(output_dir: Union[str, Path])
```

### Public Methods

```python
scan_files(file_list: List[Union[str, Path]], max_workers: Optional[int] = None) -> None
to_csv() -> None
load_observations() -> pd.DataFrame
load_band_stats() -> pd.DataFrame
load_errors() -> pd.DataFrame
filter_by_time(start_time: str, end_time: str) -> pd.DataFrame
filter_by_platform(platform_id: str) -> pd.DataFrame
filter_by_orbital_slot(orbital_slot: str) -> pd.DataFrame
```

### Properties

```python
observations: pd.DataFrame  # Current observations DataFrame
band_stats: pd.DataFrame    # Current band statistics DataFrame
errors: pd.DataFrame        # Current errors DataFrame
```
