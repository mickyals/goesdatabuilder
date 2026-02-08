# Configuration Files

## Overview

The `goesdatabuilder` project uses YAML configuration files to control various aspects of data processing, regridding, and storage. These configuration files provide a flexible way to specify parameters without modifying code.

## Configuration Structure

```
goesdatabuilder/
├── configurations/
│   ├── data/
│   │   └── goesmulticloudnc.yaml    # Data access and processing configuration
│   └── store/
│       └── goesmulticloudzarr.yaml  # Zarr store and GOES-specific configuration
```

## Data Configuration (`goesmulticloudnc.yaml`)

### Purpose

Controls data access patterns, chunking strategies, and regridding parameters for processing GOES NetCDF files.

### Configuration Schema

```yaml
# Data access configuration
data_access:
  file_dir: ${GOES_DATA_PATH}        # Environment variable for data directory
  engine: netcdf4                     # xarray engine for NetCDF files
  strict: true                        # Strict file validation
  sample_size: 5                      # Number of files to sample for metadata

  # Chunking strategy for efficient memory usage
  chunk_size:
    time: 1                           # One timestep per chunk
    y: 512                            # Spatial chunking for y dimension
    x: 512                            # Spatial chunking for x dimension

# Regridding configuration
regridding:
  weights_dir: ${WEIGHTS_PATH}/GOES-East/  # Directory for cached weights
  load_cached: true                        # Load existing weights if available
  direct_hit_threshold: 0.999               # Threshold for direct interpolation

  # Target grid specification
  target:
    lat_min: -60.0                        # Minimum latitude (degrees)
    lat_max: 60.0                         # Maximum latitude (degrees)
    lat_resolution: 0.1                    # Latitude resolution (degrees)
    lon_min: -150.0                       # Minimum longitude (degrees)
    lon_max: -30.0                        # Maximum longitude (degrees)
    lon_resolution: 0.1                    # Longitude resolution (degrees)
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

## Store Configuration (`goesmulticloudzarr.yaml`)

### Purpose

Comprehensive configuration for Zarr store creation, GOES-specific metadata, regridding parameters, and band definitions. This is the main configuration file for the complete GOES data processing pipeline.

### Configuration Schema

```yaml
# Storage configuration
store:
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
