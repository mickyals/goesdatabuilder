# Configuration Files

## Overview

The `goesdatabuilder` project uses a comprehensive YAML-based configuration system to control all aspects of GOES satellite data processing, from raw NetCDF file access to final Zarr store creation. These configuration files provide maximum flexibility while maintaining reproducibility and enabling easy parameter tuning without code modifications.

The configuration system is designed to be:
- **Environment-aware**: Supports variable expansion for different deployment environments
- **Hierarchical**: Allows inheritance and overriding of configuration parameters
- **Validated**: Built-in validation ensures configuration consistency
- **Extensible**: Easy to add new parameters without breaking existing functionality

## Configuration Structure

```
goesdatabuilder/
├── configurations/
│   ├── data/
│   │   └── goesmulticloudnc.yaml    # Data access, chunking, and regridding configuration
│   └── store/
│       └── goesmulticloudzarr.yaml  # Zarr store, metadata, and GOES-specific configuration
```

### Configuration Philosophy

The configuration system follows these principles:

1. **Separation of Concerns**: Data access and storage configurations are separate files
2. **Environment Independence**: Use environment variables for paths and deployment-specific settings
3. **Default-First**: Sensible defaults are provided, with overrides for specific use cases
4. **Validation-Ready**: All configurations can be validated before processing begins

## Data Configuration (`goesmulticloudnc.yaml`)

### Purpose and Scope

The data configuration file (`goesmulticloudnc.yaml`) is the primary control mechanism for how GOES NetCDF files are accessed, processed, and prepared for regridding. This configuration governs:

- **Data Access Patterns**: How files are discovered and loaded into memory
- **Memory Management**: Chunking strategies for efficient processing of large datasets
- **Regridding Setup**: Parameters for the geostationary-to-lat/lon transformation process
- **Performance Tuning**: Optimization settings for different hardware configurations

This configuration is used by:
- `GOESMetadataCatalog` for file discovery and metadata extraction
- `GOESMultiCloudObservation` for data loading and chunking
- `GeostationaryRegridder` for regridding parameter setup

### Complete Configuration Schema

```yaml
# ============================================================================
# DATA ACCESS CONFIGURATION
# ============================================================================
# Controls how GOES NetCDF files are discovered, accessed, and loaded
# ============================================================================

data_access:
  # Base directory containing GOES NetCDF files
  # Supports environment variable expansion (e.g., ${GOES_DATA_PATH})
  # Can be absolute or relative path
  # Should contain directories organized by date/platform (e.g., 2023/01/01/)
  file_dir: ${GOES_DATA_PATH}        
  
  # xarray engine for reading NetCDF files
  # Options: 'netcdf4' (default, good performance), 'h5netcdf' (sometimes faster)
  engine: netcdf4                     
  
  # Validation strictness for file reading
  # true: Strict validation, raises errors for invalid files (recommended for production)
  # false: Lenient validation, skips problematic files (good for messy archives)
  strict: true                        
  
  # Number of files to sample for metadata extraction and catalog building
  # Used by GOESMetadataCatalog to build initial statistics
  # Larger values provide better metadata accuracy but slower initialization
  # Recommended: 5-50 depending on archive size and variability
  sample_size: 5                      

  # ============================================================================
  # CHUNKING STRATEGY
  # ============================================================================
  # Controls how data is partitioned in memory for efficient processing
  # Critical for balancing memory usage vs. I/O performance
  # ============================================================================

  chunk_size:
    # Time dimension chunking
    # Almost always set to 1 for time-series processing
    # Enables processing one timestep at a time, minimizing memory footprint
    time: 1                           
    
    # Spatial chunking for y dimension (latitude-like in geostationary projection)
    # Recommended ranges: 256-1024 depending on available memory
    # Smaller chunks = less memory usage, more I/O operations
    # Larger chunks = better I/O efficiency, higher memory requirements
    y: 512                            
    
    # Spatial chunking for x dimension (longitude-like in geostationary projection)
    # Should typically match y chunk size for square chunks
    # Affects Dask task granularity and parallel processing efficiency
    x: 512                            

# ============================================================================
# REGRIDDING CONFIGURATION
# ============================================================================
# Controls the transformation from geostationary to regular lat/lon grid
# These parameters significantly impact processing time and output quality
# ============================================================================

regridding:
  # Directory for cached interpolation weights
  # Weights are computed once per platform/grid combination and reused
  # Should be persistent across processing sessions for performance
  # Organized by platform (e.g., GOES-East/, GOES-West/)
  # Supports environment variable expansion
  weights_dir: ${WEIGHTS_PATH}/GOES-East/  
  
  # Whether to load existing cached weights
  # true: Load cached weights if available (recommended for production)
  # false: Always recompute weights (useful for testing or parameter changes)
  load_cached: true                       
  
  # Threshold for direct interpolation vs. barycentric interpolation
  # Values closer to 1.0 favor direct interpolation (faster but less accurate)
  # Values closer to 0.0 favor barycentric interpolation (slower but more accurate)
  # 0.999 is typical for GOES data - provides good balance of speed/accuracy
  direct_hit_threshold: 0.999               

  # ============================================================================
  # TARGET GRID SPECIFICATION
  # ============================================================================
  # Defines the output lat/lon grid for regridded data
  # These parameters determine the spatial extent and resolution of output
  # ============================================================================

  target:
    # Latitude bounds in degrees (geographic coordinate system)
    # lat_min: Southernmost latitude of output grid
    # lat_max: Northernmost latitude of output grid
    # Full GOES disk: -81.3282 to 81.3282 (complete coverage)
    # Regional examples:
    #   - CONUS: 25.0 to 50.0 (continental US)
    #   - Tropics: -30.0 to 30.0 (tropical band)
    #   - Arctic: 60.0 to 90.0 (arctic region)
    lat_min: -60.0                        
    lat_max: 60.0                         
    
    # Latitude resolution in degrees (distance between grid points)
    # Determines output spatial resolution and file size
    # Common resolutions and their approximate spatial scales:
    #   - 0.1°  = ~11 km (good for regional studies, moderate file sizes)
    #   - 0.05° = ~5.5 km (high resolution, larger files)
    #   - 0.02° = ~2.2 km (very high resolution, very large files)
    #   - 0.01° = ~1.1 km (extreme resolution, massive files)
    lat_resolution: 0.1                   
    
    # Longitude bounds in degrees (geographic coordinate system)
    # lon_min: Westernmost longitude of output grid
    # lon_max: Easternmost longitude of output grid
    # Platform-specific recommended ranges:
    #   - GOES-East: -135.0 to -15.0 (Americas sector)
    #   - GOES-West: 165.0 to -115.0 (Pacific sector, crosses dateline)
    #   - GOES-Test: -75.0 to 5.0 (Test sector)
    # Can be customized for specific regional studies
    lon_min: -150.0                       
    lon_max: -30.0                        
    
    # Longitude resolution in degrees (should typically match lat_resolution)
    # Uses same scale as latitude resolution for square grid cells
    # Mismatched resolutions can cause distorted grid cells
    lon_resolution: 0.1                    
```

### Configuration Parameters

#### Data Access Parameters

- **file_dir**: Base directory containing GOES NetCDF files
  - Supports environment variable expansion (e.g., `${GOES_DATA_PATH}`)
  - Can be absolute or relative path
  - Should contain directories organized by date/platform

- **engine**: xarray engine for reading NetCDF files
  - `netcdf4`: Default engine, good performance
  - `h5netcdf`: Alternative engine, sometimes faster for specific operations

- **strict**: Validation strictness
  - `true`: Strict validation, raises errors for invalid files
  - `false`: Lenient validation, skips problematic files

- **sample_size**: Number of files to analyze for metadata extraction
  - Used by `GOESMetadataCatalog` to build initial catalog
  - Larger values provide better statistics but slower initialization

#### Chunking Parameters

- **chunk_size**: Memory-efficient data access pattern
  - **time**: Number of timesteps per chunk (typically 1 for time series)
  - **y/x**: Spatial chunk dimensions (512-1024 recommended)
  - Balances memory usage vs. I/O efficiency

#### Regridding Parameters

- **weights_dir**: Directory for cached interpolation weights
  - Should be persistent across processing sessions
  - Organized by platform (e.g., `GOES-East/`, `GOES-West/`)
  - Supports environment variable expansion

- **load_cached**: Whether to use existing weights
  - `true`: Load cached weights if available (recommended)
  - `false`: Always recompute weights (for testing)

- **direct_hit_threshold**: Threshold for direct interpolation vs. barycentric
  - Values closer to 1.0 favor direct hits
  - 0.999 is typical for GOES data
  - Affects interpolation quality vs. performance

#### Target Grid Parameters

- **lat_min/lat_max**: Latitude bounds in degrees
  - Typical range: -60 to 60 for mid-latitude focus
  - Full disk: -81.3282 to 81.3282
  - Affects memory usage and processing time

- **lon_min/lon_max**: Longitude bounds in degrees
  - GOES-East: -135 to -15 (Americas)
  - GOES-West: 165 to -115 (Pacific)
  - Can be customized for regional studies

- **lat_resolution/lon_resolution**: Output grid resolution
  - 0.1° = ~11km (good for regional studies)
  - 0.02° = ~2km (high resolution)
  - Finer resolution increases memory and computation requirements

### Usage Examples

#### Basic Usage

```python
from goesdatabuilder.data.goes.multicloudcatalog import GOESMetadataCatalog

# Load configuration
catalog = GOESMetadataCatalog.from_config('./configurations/data/goesmulticloudnc.yaml')

# Scan files using configuration parameters
catalog.scan_files(glob.glob('/data/GOES18/**/*.nc'))
```

#### Custom Configuration

```python
# Override specific parameters
config = {
    'data_access': {
        'file_dir': '/custom/data/path',
        'chunk_size': {'time': 1, 'y': 256, 'x': 256},
        'sample_size': 10
    },
    'regridding': {
        'target': {
            'lat_resolution': 0.05,  # Higher resolution
            'lon_resolution': 0.05
        }
    }
}

catalog = GOESMetadataCatalog.from_config_dict(config)
```

#### Environment Variables

```bash
# Set environment variables
export GOES_DATA_PATH="/data/goes"
export WEIGHTS_PATH="/cache/weights"

# Configuration will automatically expand these
# file_dir: ${GOES_DATA_PATH} -> "/data/goes"
# weights_dir: ${WEIGHTS_PATH}/GOES-East/ -> "/cache/weights/GOES-East/"
```

## Pipeline Configuration (`goesmulticlould.yaml`)

### Purpose and Scope

The pipeline configuration file (`goesmulticlould.yaml`) provides comprehensive control over the orchestration layer of the GOES Data Builder. This configuration manages all aspects of pipeline execution, from error handling and checkpointing to distributed computing and performance monitoring. It separates orchestration concerns from data access and storage configuration, enabling flexible deployment scenarios.

### Configuration Philosophy

The pipeline configuration follows these principles:

- **Orchestration First**: Centralized management of all pipeline execution aspects
- **Production Ready**: Enterprise-grade features for large-scale processing
- **Observability**: Comprehensive monitoring, logging, and progress tracking
- **Resilience**: Robust error handling, checkpointing, and recovery mechanisms
- **Scalability**: From single-machine to distributed cluster deployment

### Complete Configuration Schema

```yaml
# ============================================================================
# PIPELINE METADATA AND DEFAULTS
# ============================================================================
# Basic pipeline identification and global settings

pipeline:
  name: "GOES ABI L2+ Processing Pipeline"
  version: "1.0.0"
  description: "Regrid GOES ABI imagery to lat/lon grid and store in CF-compliant Zarr"
  
  # Use catalog for file discovery (vs explicit file list)
  use_catalog: true

# ============================================================================
# CATALOG SETTINGS
# ============================================================================
# Configuration for file discovery and filtering

catalog:
  # Parallel file scanning for metadata catalog building
  parallel: true
  max_workers: 8  # Number of workers for parallel catalog building
  
  # Optional filters applied to catalog before processing
  # These are applied IN ADDITION to filters in obs_config
  orbital_slot: null  # "GOES-East", "GOES-West", or null for all
  scene_id: null      # "C" (CONUS), "F" (Full Disk), "M1"/"M2" (Mesoscale), or null for all
  
  # NOTE: Time range and band filters are passed to initialize_observation()
  # rather than configured here, as they're more dynamic

# ============================================================================
# DASK DISTRIBUTED COMPUTING
# ============================================================================
# Configuration for parallel and distributed processing

dask:
  enabled: false  # Set to true to use Dask distributed
  
  # Remote cluster connection (if scheduler_address is provided)
  scheduler_address: null  # e.g., "tcp://scheduler.cluster.local:8786"
  
  # Local cluster settings (used if scheduler_address is null)
  local:
    n_workers: 8
    threads_per_worker: 4
    memory_limit: "8GB"  # Per worker memory limit
    
    # Advanced local cluster settings
    processes: true  # Use processes (true) vs threads (false)
    dashboard_address: ":8787"  # Dask dashboard port
  
  # Dask configuration overrides
  # See: https://docs.dask.org/en/stable/configuration.html
  config:
    "distributed.worker.memory.target": 0.80  # Target memory usage (80%)
    "distributed.worker.memory.spill": 0.90   # Spill to disk at 90%
    "distributed.worker.memory.pause": 0.95   # Pause worker at 95%
    "distributed.worker.memory.terminate": 0.98  # Kill worker at 98%
    "distributed.comm.timeouts.connect": "60s"
    "distributed.comm.timeouts.tcp": "60s"

# ============================================================================
# BATCHING AND ERROR HANDLING
# ============================================================================
# Configuration for processing batches and error recovery

batching:
  # Batch size (number of observations to process before updating store)
  # null = auto-calculate based on available memory
  batch_size: null
  
  # Checkpoint interval (save state every N observations)
  checkpoint_interval: 500
  
  # Error handling strategy
  continue_on_error: true  # Continue processing if single observation fails
  max_retries: 2           # Number of retry attempts for failed observations

# ============================================================================
# CHECKPOINTING AND RECOVERY
# ============================================================================
# Configuration for pipeline state management and recovery

checkpoints:
  enabled: true
  directory: "${OUTPUT_PATH}/checkpoints/"
  
  # Checkpoint retention policy
  keep_last_n: 5  # Keep only the last N checkpoints
  
  # Auto-resume from latest checkpoint on initialization
  auto_resume: false

# ============================================================================
# PROGRESS TRACKING
# ============================================================================
# Configuration for progress monitoring and logging

progress:
  show_progress: true  # Show tqdm progress bars
  log_interval: 100    # Log progress every N observations

# ============================================================================
# VALIDATION AND PRE-CHECKS
# ============================================================================
# Configuration for pipeline validation and size estimation

validation:
  # Run validation checks before processing
  validate_on_init: true
  
  # Estimate output size before processing
  estimate_sizes: true
  
  # Check available disk space
  check_disk_space: true
  required_free_space_gb: 100  # Minimum free space required (GB)

# ============================================================================
# LOGGING CONFIGURATION
# ============================================================================
# Comprehensive logging setup for pipeline monitoring

logging:
  level: "INFO"  # DEBUG, INFO, WARNING, ERROR, CRITICAL
  
  # Log file configuration (optional, null = console only)
  log_file: "${OUTPUT_PATH}/logs/pipeline.log"
  
  # Log rotation settings
  log_rotation: "100MB"  # Rotate after 100MB
  max_backups: 5         # Keep last 5 rotated logs
  
  # Log format configuration
  format: "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
  date_format: "%Y-%m-%d %H:%M:%S"

# ============================================================================
# PERFORMANCE MONITORING
# ============================================================================
# Optional performance tracking and metrics collection

monitoring:
  enabled: false
  
  # Memory usage tracking
  track_memory: true
  memory_sample_interval: 60  # seconds between samples
  
  # Timing performance tracking
  track_timing: true
  
  # Export metrics to file
  export_metrics: true
  metrics_file: "${OUTPUT_PATH}/metrics/pipeline_metrics.json"
  metrics_interval: 300  # Export every 5 minutes
  
  # Profiling (optional, requires py-spy or similar)
  enable_profiling: false
  profiling_output: "${OUTPUT_PATH}/profiling/"

# ============================================================================
# NOTIFICATIONS
# ============================================================================
# Optional notification system for pipeline events

notifications:
  enabled: false
  
  # Email notifications (requires SMTP configuration)
  email:
    enabled: false
    smtp_server: null
    smtp_port: 587
    smtp_user: null
    smtp_password: null
    from_address: null
    to_addresses: []
    
    # When to send email notifications
    on_start: false
    on_complete: true
    on_error: true
    on_checkpoint: false
  
  # Slack notifications (requires webhook URL)
  slack:
    enabled: false
    webhook_url: null
    
    # When to send Slack notifications
    on_start: false
    on_complete: true
    on_error: true

# ============================================================================
# RESOURCE LIMITS
# ============================================================================
# Optional resource constraints and limits

limits:
  # Maximum processing time (hours)
  max_processing_hours: null  # null = no limit
  
  # Maximum number of failures before aborting
  max_failures: null  # null = no limit
  
  # Maximum memory usage (GB)
  max_memory_gb: null  # null = no limit

# ============================================================================
# ADVANCED SETTINGS
# ============================================================================
# Rarely changed advanced configuration options

advanced:
  # Garbage collection management
  gc_interval: 100  # Run garbage collection every N observations
  
  # Zarr store flush interval
  flush_interval: 50  # Flush Zarr store every N observations
  
  # Worker timeout management
  worker_timeout: 3600  # Kill stuck workers after 1 hour
```

### Pipeline Configuration Parameters

#### **Pipeline Metadata**
```yaml
pipeline:
  name: "GOES ABI L2+ Processing Pipeline"
  version: "1.0.0"
  description: "Regrid GOES ABI imagery to lat/lon grid and store in CF-compliant Zarr"
  use_catalog: true  # Use metadata catalog for file discovery
```

#### **Catalog Configuration**
```yaml
catalog:
  parallel: true              # Enable parallel file scanning
  max_workers: 8              # Number of workers for catalog building
  orbital_slot: "GOES-East"   # Filter by satellite position
  scene_id: "F"              # Filter by scene type (F=Full Disk, C=CONUS, M=Mesoscale)
```

#### **Dask Distributed Computing**
```yaml
dask:
  enabled: true
  scheduler_address: "tcp://dask-scheduler:8786"  # Remote cluster
  
  local:
    n_workers: 8
    threads_per_worker: 4
    memory_limit: "8GB"
    processes: true
    dashboard_address: ":8787"
  
  config:
    "distributed.worker.memory.target": 0.80
    "distributed.worker.memory.spill": 0.90
```

#### **Batching and Error Handling**
```yaml
batching:
  batch_size: 100              # Process in batches of 100 observations
  checkpoint_interval: 500     # Save checkpoint every 500 observations
  continue_on_error: true      # Continue processing on individual failures
  max_retries: 2              # Retry failed observations up to 2 times
```

#### **Checkpointing**
```yaml
checkpoints:
  enabled: true
  directory: "${OUTPUT_PATH}/checkpoints/"
  keep_last_n: 5              # Keep only last 5 checkpoints
  auto_resume: false          # Don't auto-resume on startup
```

#### **Progress Tracking**
```yaml
progress:
  show_progress: true         # Show tqdm progress bars
  log_interval: 100          # Log progress every 100 observations
```

#### **Validation**
```yaml
validation:
  validate_on_init: true      # Run validation before processing
  estimate_sizes: true        # Estimate output size
  check_disk_space: true     # Verify sufficient disk space
  required_free_space_gb: 100 # Require 100GB free space
```

#### **Logging**
```yaml
logging:
  level: "INFO"
  log_file: "${OUTPUT_PATH}/logs/pipeline.log"
  log_rotation: "100MB"
  max_backups: 5
  format: "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
```

#### **Performance Monitoring**
```yaml
monitoring:
  enabled: true
  track_memory: true
  memory_sample_interval: 60
  track_timing: true
  export_metrics: true
  metrics_file: "${OUTPUT_PATH}/metrics/pipeline_metrics.json"
  metrics_interval: 300
```

#### **Notifications**
```yaml
notifications:
  enabled: true
  
  email:
    enabled: true
    smtp_server: "smtp.gmail.com"
    smtp_port: 587
    smtp_user: "your-email@gmail.com"
    smtp_password: "${EMAIL_PASSWORD}"
    from_address: "your-email@gmail.com"
    to_addresses: ["admin@example.com"]
    
    on_start: true
    on_complete: true
    on_error: true
  
  slack:
    enabled: true
    webhook_url: "${SLACK_WEBHOOK_URL}"
    
    on_start: false
    on_complete: true
    on_error: true
```

### Pipeline Configuration Examples

#### **Development Configuration**
```yaml
# Development pipeline configuration
pipeline:
  name: "GOES Development Pipeline"
  use_catalog: false  # Use explicit file list for testing

dask:
  enabled: false  # Disable distributed computing for development

batching:
  batch_size: 10  # Small batches for testing
  checkpoint_interval: 25

logging:
  level: "DEBUG"  # Verbose logging for development
  log_file: null   # Console only

validation:
  validate_on_init: true
  estimate_sizes: true

monitoring:
  enabled: false  # Disable monitoring for development
```

#### **Production Configuration**
```yaml
# Production pipeline configuration
pipeline:
  name: "GOES Production Pipeline"
  use_catalog: true

catalog:
  parallel: true
  max_workers: 16

dask:
  enabled: true
  scheduler_address: "tcp://dask-cluster:8786"
  
  config:
    "distributed.worker.memory.target": 0.85
    "distributed.worker.memory.spill": 0.90

batching:
  batch_size: 500
  checkpoint_interval: 1000
  continue_on_error: true
  max_retries: 3

checkpoints:
  enabled: true
  directory: "/shared/checkpoints/goes/"
  keep_last_n: 10

logging:
  level: "INFO"
  log_file: "/shared/logs/goes/pipeline.log"
  log_rotation: "500MB"
  max_backups: 10

validation:
  validate_on_init: true
  estimate_sizes: true
  check_disk_space: true
  required_free_space_gb: 500

monitoring:
  enabled: true
  track_memory: true
  memory_sample_interval: 30
  export_metrics: true
  metrics_file: "/shared/metrics/goes/pipeline_metrics.json"
  metrics_interval: 300

notifications:
  enabled: true
  
  email:
    enabled: true
    smtp_server: "smtp.company.com"
    smtp_user: "goes-pipeline@company.com"
    to_addresses: ["ops-team@company.com", "scientists@company.com"]
    
    on_start: true
    on_complete: true
    on_error: true
    on_checkpoint: false

limits:
  max_processing_hours: 24
  max_failures: 100
  max_memory_gb: 256
```

#### **High-Performance Configuration**
```yaml
# High-performance pipeline configuration
pipeline:
  name: "GOES High-Performance Pipeline"

catalog:
  parallel: true
  max_workers: 32

dask:
  enabled: true
  
  local:
    n_workers: 16
    threads_per_worker: 8
    memory_limit: "16GB"
    processes: true
  
  config:
    "distributed.worker.memory.target": 0.90
    "distributed.worker.memory.spill": 0.95

batching:
  batch_size: 1000
  checkpoint_interval: 2000
  continue_on_error: true

checkpoints:
  enabled: false  # Disable for maximum speed

logging:
  level: "WARNING"  # Minimal logging for performance
  log_file: null

validation:
  validate_on_init: false  # Skip validation for speed
  estimate_sizes: false
  check_disk_space: false

monitoring:
  enabled: false  # Disable monitoring for performance

advanced:
  gc_interval: 500  # Less frequent garbage collection
  flush_interval: 200  # Less frequent Zarr flushing
```

## Store Configuration (`goesmulticloudzarr.yaml`)
  type: local                          # Storage backend type
  path: null                           # Store path (set at runtime)

# Zarr format configuration
zarr:
  zarr_format: 3                       # Zarr V3 format

  # Compression settings
  compression:
    default:                            # Primary compression for data arrays
      compressor:
        codec: blosc                   # Compression codec
        cname: zstd                    # Compression algorithm
        clevel: 5                      # Compression level (1-9)
        shuffle: bitshuffle              # Data shuffling strategy
      chunks: auto                      # Chunk size strategy
      fill_value: NaN                  # Fill value for missing data

    secondary:                          # Secondary compression for metadata
      compressor:
        codec: blosc
        cname: zstd
        clevel: 5
        shuffle: bitshuffle
      chunks: auto
      fill_value: NaN

# GOES-specific configuration
goes:
  # Platform definitions
  platforms: ["GOES-East", "GOES-West", "GOES-Test"]
  bands: [8, 13, 15]                 # Bands to process
  spatial_resolution: "2km"             # Nominal spatial resolution

  # Global metadata (ACDD compliant)
  global_metadata:
    Conventions: "CF-1.13, ACDD-1.3"
    title: "GOES ABI L2+ Cloud and Moisture Imagery"
    summary: "Regridded GOES ABI imagery on regular lat/lon grid"
    institution: "University of Toronto"
    source: "GOES-R Series Advanced Baseline Imager"
    processing_level: "L2+"

    # Creator information
    creator_name: "Marble Platform"
    creator_type: "institution"
    creator_email: null
    creator_url: null

    # Project information
    project: null
    program: null

    # License and usage
    license: "CC BY 4.0"
    standard_name_vocabulary: "CF Standard Name Table v92"
    keywords: "GOES, ABI, satellite, imagery, regridded"

  # Regridding configuration
  regridding:
    method: "barycentric"
    triangulation: "delaunay"
    direct_hit_threshold: 0.999
    dqf_interpolated_flag: 5
    weights_cache_dir: "./regrid_weights/"
    save_weights: true
    validate_coverage: true
    min_coverage_fraction: 0.5

  # Processing metadata
  processing:
    software_name: "geolab"
    software_version: "0.1.0"
    software_url: "https://github.com/yourusername/geolab"
    processing_environment: "Python 3.11, PyTorch 2.1"

  # Target grid specifications (per platform)
  target_grids:
    GOES-East:
      lat_min: -81.3282
      lat_max: 81.3282
      lat_resolution: 0.02
      lon_min: -135.0
      lon_max: -15.0
      lon_resolution: 0.02
      crs: "EPSG:4326"

    GOES-West:
      lat_min: -81.3282
      lat_max: 81.3282
      lat_resolution: 0.02
      lon_min: 165.0
      lon_max: -115.0
      lon_resolution: 0.02
      crs: "EPSG:4326"

  # Band-specific metadata
  band_metadata:
    1:
      wavelength: 0.47
      long_name: "ABI Cloud and Moisture Imagery reflectance factor"
      standard_name: "toa_bidirectional_reflectance"
      units: "1"
      valid_range: [0.0, 1.0]
      products: ["Aerosol detection", "Cloud optical depth"]
    # ... (continues for all 16 bands)
```

### Key Configuration Sections

#### Storage Configuration

- **type**: Storage backend
  - `local`: File system storage
  - `zip`: Compressed archive
  - `memory`: In-memory storage
  - `fsspec`: Cloud storage (S3, GCS, Azure)

#### Compression Settings

- **codec**: Compression algorithm
  - `blosc`: Recommended for scientific data
  - `zstd`: Good compression ratio
  - `gzip`: Maximum compatibility

- **cname**: Specific compressor
  - `zstd`: Zstandard (recommended)
  - `lz4`: Fast compression
  - `zlib`: Standard compression

#### Platform Configuration

Each GOES platform has specific coverage areas:
- **GOES-East**: Americas sector (-135° to -15°)
- **GOES-West**: Pacific sector (165° to -115°)
- **GOES-Test**: Test sector (-75° to 5°)

#### Band Metadata

Complete metadata for all 16 ABI bands:
- **Bands 1-6**: Reflectance bands (0.47-2.24 μm)
- **Bands 7-16**: Brightness temperature bands (3.90-13.28 μm)

Each band includes:
- Wavelength and units
- CF standard names
- Valid value ranges
- Associated products/applications

### Usage Examples

#### Basic Store Creation

```python
from goesdatabuilder.store.datasets.goes import GOESZarrStore

# Create store with full configuration
store = GOESZarrStore('./configurations/store/goesmulticloudzarr.yaml')
store.initialize_store('./goes_data.zarr')
```

#### Custom Band Selection

```python
# Override band selection in code
config_path = './configurations/store/goesmulticloudzarr.yaml'
store = GOESZarrStore(config_path)

# Modify bands to process
store.BANDS = [1, 2, 3, 7, 14]  # Custom band set
```

#### Regional Processing

```python
# Process specific region with custom grid
store.initialize_region(
    region='GOES-East',
    lat=custom_lat_grid,
    lon=custom_lon_grid,
    bands=[8, 13, 15]
)
```

## Environment Variables

### Required Variables

- **GOES_DATA_PATH**: Base directory for GOES NetCDF files
- **WEIGHTS_PATH**: Directory for cached regridding weights

### Optional Variables

- **STORE_PATH**: Default location for Zarr stores
- **LOG_LEVEL**: Logging level (DEBUG, INFO, WARNING, ERROR)

### Example Environment Setup

```bash
# Create environment file
cat > .env << EOF
export GOES_DATA_PATH="/data/goes"
export WEIGHTS_PATH="/cache/weights"
export STORE_PATH="/output/zarr"
export LOG_LEVEL="INFO"
EOF

# Source environment
source .env
```

## Configuration Best Practices

### File Organization

```
project/
├── configurations/
│   ├── data/
│   │   ├── goesmulticloudnc.yaml      # Data access config
│   │   └── custom_data_config.yaml     # Custom data config
│   └── store/
│       ├── goesmulticloudzarr.yaml     # Main store config
│       └── regional_config.yaml       # Regional variant
├── data/                             # GOES data files
├── weights/                          # Cached regridding weights
└── output/                           # Generated Zarr stores
```

### Configuration Management

#### Version Control

- Include configuration files in version control
- Use descriptive names for different configurations
- Document configuration changes in commit messages

#### Environment-Specific Configs

```yaml
# Base configuration (goesmulticloudzarr.yaml)
store:
  path: ${STORE_PATH}/goes_data.zarr

# Development override (dev_overrides.yaml)
goes:
  bands: [1, 2, 3]  # Fewer bands for testing
  spatial_resolution: "10km"

# Production override (prod_overrides.yaml)
goes:
  bands: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
  spatial_resolution: "2km"
```

#### Configuration Validation

```python
from goesdatabuilder.store.datasets.goes import GOESZarrStore
import yaml

def validate_config(config_path):
    """Validate configuration file"""
    try:
        # Try to load configuration
        store = GOESZarrStore(config_path)
        print("✓ Configuration is valid")
        return True
    except Exception as e:
        print(f"✗ Configuration error: {e}")
        return False

# Validate all configurations
configs = [
    './configurations/data/goesmulticloudnc.yaml',
    './configurations/store/goesmulticloudzarr.yaml'
]

for config in configs:
    validate_config(config)
```

### Performance Tuning

#### Memory Optimization

```yaml
# For limited memory systems
data_access:
  chunk_size:
    time: 1
    y: 256        # Smaller chunks
    x: 256

zarr:
  compression:
    default:
      clevel: 3     # Lower compression for speed
```

#### High-Performance Processing

```yaml
# For high-performance systems
data_access:
  chunk_size:
    time: 1
    y: 1024       # Larger chunks
    x: 1024

zarr:
  compression:
    default:
      clevel: 7     # Higher compression
      shuffle: bitshuffle
```

## Troubleshooting

### Common Configuration Issues

#### Environment Variable Not Found

```bash
# Check if variable is set
echo $GOES_DATA_PATH

# Set variable if missing
export GOES_DATA_PATH="/path/to/goes/data"
```

#### Invalid YAML Syntax

```bash
# Validate YAML syntax
python -c "import yaml; yaml.safe_load(open('config.yaml'))"
```

#### Path Resolution Issues

```python
# Debug path resolution
import os
from pathlib import Path

config_path = './configurations/store/goesmulticloudzarr.yaml'
print(f"Config path: {Path(config_path).absolute()}")

# Check environment variable expansion
expanded = os.path.expandvars('${GOES_DATA_PATH}/data')
print(f"Expanded path: {expanded}")
```

### Configuration Debugging

```python
# Load and inspect configuration
import yaml

with open('./configurations/store/goesmulticloudzarr.yaml', 'r') as f:
    config = yaml.safe_load(f)

# Print key sections
print("Platforms:", config['goes']['platforms'])
print("Bands:", config['goes']['bands'])
print("Spatial resolution:", config['goes']['spatial_resolution'])
```

## Integration with Pipeline

### Complete Pipeline Configuration

```python
from goesdatabuilder.data.goes.multicloudcatalog import GOESMetadataCatalog
from goesdatabuilder.regrid.geostationary import GeostationaryRegridder
from goesdatabuilder.store.datasets.goes import GOESZarrStore

# Load configurations
data_config = './configurations/data/goesmulticloudnc.yaml'
store_config = './configurations/store/goesmulticloudzarr.yaml'

# Initialize components
catalog = GOESMetadataCatalog.from_config(data_config)
store = GOESZarrStore(store_config)

# Process pipeline
catalog.scan_files(glob.glob('/data/GOES/**/*.nc'))
store.initialize_store('./output/goes_data.zarr')

# Process each observation
for obs_file in catalog.get_observations():
    # Process with configured parameters
    pass
```

This configuration system provides flexibility while maintaining consistency across the GOES data processing pipeline.
