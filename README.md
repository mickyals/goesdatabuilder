# GOES Data Builder

> A Python package for processing GOES ABI L2+ data from raw NetCDF files to CF-compliant Zarr stores with metadata management, advanced regridding, and orchestration capabilities.

## Overview

The GOES Data Builder provides a pipeline for processing GOES (Geostationary Operational Environmental Satellite) ABI (Advanced Baseline Imager) Level 2+ data. It transforms raw geostationary-projected NetCDF files into analysis-ready Zarr stores with CF-1.13 and ACDD-1.3 compliant metadata, provenance tracking, and quality control.

### Key Features

- **Pipeline Orchestration**: Complete pipeline management with `GOESPipelineOrchestrator`
- **CF-Compliant Storage**: Full CF-1.13 and ACDD-1.3 metadata compliance
- **Advanced Regridding**: Delaunay triangulation with barycentric interpolation and weight caching
- **Metadata Management**: Automated metadata extraction, cataloging, and provenance tracking
- **Multi-Platform Support**: GOES-East, GOES-West, and GOES-Test platforms
- **Performance Optimized**: Parallel processing, Dask integration, and efficient memory management
- **Configuration-Driven**: Comprehensive YAML-based configuration with environment variable support
- **Quality Control**: Extended DQF flags, validation, and comprehensive error handling

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    GOESPipelineOrchestrator                     │
│                        (Orchestration Layer)                    │
├─────────────────────────────────────────────────────────────────┤
│  Configuration Management  │  Error Recovery  │  Resource Mgmt  │
│  - Parameter Validation     │  - Retry Logic    │  - Memory Ctrl  │
│  - Environment Expansion   │  - Checkpointing  │  - Parallelism   │
│  - Component Coordination   │  - State Tracking │  - Performance  │
├─────────────────────────────────────────────────────────────────┤
│                    Component Coordination                        │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────┐  │
│  │   Catalog   │  │ Observation │  │  Regridder  │  │  Store  │  │
│  │ Discovery   │  │   Loading   │  │ Transformation│  │ Output │  │
│  │ Indexing    │  │   Access    │  │   Quality    │  │ Storage │  │
│  └─────────────┘  └─────────────┘  └─────────────┘  └─────────┘  │
├─────────────────────────────────────────────────────────────────┤
│                         Data Flow                               │
│  Raw NetCDF → Catalog → Observation → Regridder → Store → Zarr  │
└─────────────────────────────────────────────────────────────────┘
```

### Core Components

#### 1. **Data Access Layer**
- **`GOESMultiCloudObservation`**: CF-aligned interface for GOES data with lazy loading
- **`GOESMetadataCatalog`**: High-performance metadata extraction and cataloging

#### 2. **Processing Layer** 
- **`GeostationaryRegridder`**: Advanced geostationary to lat/lon regridding with Delaunay triangulation
- **Extended DQF flags**: Quality tracking for interpolated and regridded data
- **Grid Utilities**: Antimeridian-safe coordinate handling and validation

#### 3. **Storage Layer**
- **`ZarrStoreBuilder`**: Configurable Zarr V3 store builder with multiple backends
- **`GOESZarrStore`**: GOES-specific CF-compliant Zarr store with provenance tracking

#### 4. **Orchestration Layer**
- **`GOESPipelineOrchestrator`**: Pipeline orchestration with checkpointing and recovery

## Installation

### Prerequisites

- Python 3.10 or higher
- Conda or pip package manager
- Sufficient disk space for data and cache (recommended: 100GB+)

### 1. Clone the Repository

```bash
git clone https://github.com/mickyals/goesdatabuilder.git
cd goesdatabuilder
```

### 2. Create and Activate Conda Environment

```bash
conda create -n goesdatabuilder python=3.13
conda activate goesdatabuilder
```

### 3. Install Dependencies

```bash
# Install from requirements file
pip install -r requirements.txt
```

### 4. Verify Installation

```python
from goesdatabuilder import GOESMultiCloudObservation, GeostationaryRegridder, GOESZarrStore
from goesdatabuilder.utils.grid_utils import build_longitude_array, validate_longitude_monotonic
print("GOES Data Builder installed successfully!")
```

## Quick Start

### Pipeline Usage

```python
from goesdatabuilder.pipelines.goesmulticloudpipeline import GOESPipelineOrchestrator

# Initialize orchestrator with configuration files
pipeline = GOESPipelineOrchestrator.from_configs(
    obs_config='./configs/data/goesmulticloudnc.yaml',
    store_config='./configs/store/goesmulticloudzarr.yaml',
    pipeline_config='./configs/pipelines/goesmulticloud.yaml'  # Optional
)

# Initialize all components (catalog, observation, regridder, store)
pipeline.initialize_all(
    store_path='./output/goes_data.zarr',
    use_catalog=True,
    use_dask_client=True
)

# Process all data with progress tracking and error recovery
pipeline.process_all(
    show_progress=True,
    continue_on_error=True
)

# Finalize and cleanup
pipeline.finalize()

# Get comprehensive processing summary
pipeline.print_summary()
```

### Step-by-Step Processing

```python
from goesdatabuilder.pipelines.goesmulticloudpipeline import GOESPipelineOrchestrator

# 1. Initialize orchestrator
pipeline = GOESPipelineOrchestrator(
    obs_config='./configs/data/goesmulticloudnc.yaml',
    store_config='./configs/store/goesmulticloudzarr.yaml'
)

# 2. Initialize components individually
catalog = pipeline.initialize_catalog()                    # Optional metadata catalog
observation = pipeline.initialize_observation()           # Load GOES data
regridder = pipeline.initialize_regridder()               # Setup regridding
store = pipeline.initialize_store('./output/goes.zarr')   # Initialize Zarr store

# 3. Process specific time range
pipeline.process_time_range(
    start_time='2024-01-01T00:00:00',
    end_time='2024-01-01T23:59:59',
    show_progress=True
)

# 4. Handle errors and retry if needed
if pipeline.failed_count > 0:
    pipeline.retry_failed()
    pipeline.print_summary()

# 5. Finalize
pipeline.finalize()
```

### Individual Component Usage

```python
from goesdatabuilder.data.goes.multicloud import GOESMultiCloudObservation
from goesdatabuilder.regrid.geostationary import GeostationaryRegridder
from goesdatabuilder.store.datasets.goes import GOESZarrStore

# 1. Load GOES data with CF compliance
obs = GOESMultiCloudObservation.from_config('./configs/data/goesmulticloudnc.yaml')

# 2. Initialize regridder with weight caching
regridder = GeostationaryRegridder(
    source_x=obs.x.values,
    source_y=obs.y.values,
    projection=obs.projection,
    target_resolution=0.02,
    weights_dir='./weights/GOES-East/',
    load_cached=True
)

# 3. Initialize CF-compliant Zarr store
store = GOESZarrStore('./configs/store/goesmulticloudzarr.yaml')
store.initialize_store('./output/goes_data.zarr')
store.initialize_region('GOES-East', regridder.target_lat, regridder.target_lon)

# 4. Process data with quality control
for i in range(len(obs.time)):
    obs_dict = regridder.regrid_to_observation_dict(obs, time_idx=i)
    store.append_observation('GOES-East', obs_dict)

# 5. Finalize with metadata
store.finalize_dataset()
```

## Project Structure

```
goesdatabuilder/
├── goesdatabuilder/                    # Main package
│   ├── data/                          # Data access layer
│   │   └── goes/
│   │       ├── multicloud.py           # CF-aligned GOES data interface
│   │       └── multicloudcatalog.py  # High-performance metadata cataloging
│   ├── regrid/                        # Processing layer
│   │   └── geostationary.py        # Advanced regridding engine
│   ├── store/                         # Storage layer
│   │   ├── zarrstore.py            # Base Zarr V3 store builder
│   │   └── datasets/
│   │       └── goes.py              # GOES-specific CF-compliant store
│   ├── pipelines/                     # Orchestration layer
│   │   └── goesmulticloudpipeline.py # Enterprise-grade pipeline orchestrator
│   └── configs/               # Configuration templates
│       ├── data/
│       │   └── goesmulticloudnc.yaml    # Data access & regridding config
│       ├── store/
│       │   └── goesmulticloudzarr.yaml  # Zarr store & GOES metadata config
│       └── pipelines/
│           └── goesmulticlould.yaml     # Pipeline orchestration config
├── goesdatabuilder-docs/               # Comprehensive documentation
│   ├── configs/                       # Configuration documentation
│   │   └── configuration-files.md      # Detailed configuration reference
│   ├── data/                         # Data layer documentation
│   │   └── goes/
│   │       ├── goesmetadatacatalog.md  # Metadata cataloging guide
│   │       └── goesmulticloud.md       # CF data interface guide
│   ├── regrid/                       # Regridding documentation
│   │   └── geostationaryregridder.md   # Advanced regridding guide
│   ├── store/                        # Storage documentation
│   │   ├── zarrstore.md               # Base store builder guide
│   │   └── datasets/
│   │       └── goes.md                # GOES store guide
│   └── pipelines/                    # Pipeline documentation
│       └── goesmulticloudpipeline.md  # Orchestration guide
├── notebooks/                        # Example notebooks and tutorials
├── requirements.txt                   # Python dependencies
├── pyproject.toml                    # Modern Python packaging
├── setup.py                         # Legacy setup support
└── README.md                        # This file
```

## Configuration

### Environment Variables

```bash
# Required paths
export GOES_DATA_PATH="/path/to/goes/netcdf/files"
export WEIGHTS_PATH="/path/to/regridding/weights/cache"

# Optional paths
export STORE_PATH="/path/to/output/zarr/stores"
export CATALOG_PATH="/path/to/metadata/catalog"

# Processing options
export LOG_LEVEL="INFO"
export DASK_SCHEDULER_ADDRESS="localhost:8786"  # Optional Dask cluster
```

### Configuration Files

The package uses comprehensive YAML configuration files organized by function:

#### **Data Configuration** (`goesmulticloudnc.yaml`)
- Data access patterns and file discovery
- Memory-efficient chunking strategies
- Regridding parameters and target grid specification
- Performance tuning options

#### **Store Configuration** (`goesmulticloudzarr.yaml`)
- Zarr V3 store setup and backend selection
- Compression and chunking optimization
- GOES-specific metadata and CF compliance
- Multi-platform organization (GOES-East/West/Test)

#### **Pipeline Configuration** (`goesmulticlould.yaml`)
- **Orchestration Parameters**: Error handling, checkpointing, and recovery settings
- **Dask Integration**: Distributed computing configuration and cluster management
- **Batch Processing**: Batch sizes, retry logic, and progress tracking
- **Monitoring & Logging**: Performance monitoring, validation, and notification settings
- **Resource Management**: Memory limits, timeouts, and garbage collection

📖 **See [Configuration Documentation](goesdatabuilder-docs/configs/configuration-files.md)** for comprehensive configuration options and examples.

## Documentation

### Core Components

- **[GOESPipelineOrchestrator](goesdatabuilder-docs/pipelines/goesmulticloudpipeline.md)**: Pipeline orchestration
- **[GOESMultiCloudObservation](goesdatabuilder-docs/data/goes/multicloud.md)**: CF-compliant data interface with lazy loading
- **[GeostationaryRegridder](goesdatabuilder-docs/regrid/geostationaryregridder.md)**: Advanced regridding with Delaunay triangulation
- **[GOESZarrStore](goesdatabuilder-docs/store/datasets/goes.md)**: GOES-specific CF-compliant Zarr store

### Supporting Components

- **[GOESMetadataCatalog](goesdatabuilder-docs/data/goes/multicloudmetadatacatalog.md)**: High-performance metadata cataloging
- **[ZarrStoreBuilder](goesdatabuilder-docs/store/zarrstore.md)**: Base Zarr V3 store builder
- **[Configuration Files](goesdatabuilder-docs/configs/configuration-files.md)**: Complete configuration reference

## Performance Optimization

### Memory Management

```yaml
# For memory-constrained systems
data_access:
  chunk_size:
    time: 1
    y: 256
    x: 256

zarr:
  compression:
    default:
      clevel: 3  # Lower compression for speed

# Pipeline configuration for memory efficiency
pipeline:
  batching:
    batch_size: 50        # Smaller batches
    checkpoint_interval: 100  # Frequent checkpoints
  dask:
    enabled: false        # Disable distributed computing
    local:
      memory_limit: "2GB"  # Conservative memory limit
```

### High-Performance Processing

```yaml
# For high-performance systems
data_access:
  chunk_size:
    time: 1
    y: 1024
    x: 1024

zarr:
  compression:
    default:
      clevel: 7  # Higher compression

# Pipeline configuration for maximum performance
pipeline:
  batching:
    batch_size: 500       # Larger batches
    continue_on_error: true
  dask:
    enabled: true
    local:
      n_workers: 16
      threads_per_worker: 4
      memory_limit: "8GB"
  checkpoints:
    enabled: false        # Disable for speed
```

### Parallel Processing with Dask

```python
# Configure for distributed processing
pipeline_config = {
    'dask': {
        'enabled': True,
        'scheduler_address': 'tcp://dask-scheduler:8786',  # Remote cluster
        'local': {
            'n_workers': 8,
            'threads_per_worker': 2,
            'memory_limit': '4GB'
        }
    },
    'batching': {
        'batch_size': 200,
        'parallel_workers': 8
    }
}

pipeline = GOESPipelineOrchestrator.from_configs(
    obs_config='./configs/data.yaml',
    store_config='./configs/store.yaml',
    pipeline_config=pipeline_config
)

# Initialize with Dask cluster
pipeline.initialize_all(use_dask_client=True)
```

## Troubleshooting

### Common Issues

#### Memory Errors
```python
# Reduce memory usage
pipeline._obs_config['data_access']['chunk_size'] = {
    'time': 1, 'y': 256, 'x': 256
}

# Process in smaller batches
pipeline.process_batch(
    start_idx=0, 
    end_idx=100,  # Process 100 at a time
    continue_on_error=True
)

# Enable memory-efficient processing
pipeline.initialize_all(
    use_dask_client=False  # Disable Dask to reduce overhead
)
```

#### Weight Computation Issues
```python
# Clear corrupted weights and recompute
import shutil
shutil.rmtree('$WEIGHTS_PATH/GOES-East/')

# Reinitialize regridder to recompute weights
pipeline.initialize_regridder(load_cached=False)

# Check regridding statistics
print(f"Coverage: {pipeline._regridder.coverage_fraction:.2%}")
print(f"Direct hits: {pipeline._regridder.direct_hit_fraction:.2%}")
```

#### File Discovery Problems
```python
# Check catalog status
if not pipeline.has_catalog:
    pipeline.initialize_catalog(force_rebuild=True)

# Verify file discovery
files = pipeline._get_files_from_catalog()
print(f"Found {len(files)} files to process")

# Check specific time range
files = pipeline._get_files_from_catalog(
    time_range=('2024-01-01', '2024-01-31')
)
print(f"January files: {len(files)}")
```

### Debug Mode and Diagnostics

```python
# Enable comprehensive logging
import logging
logging.basicConfig(level=logging.DEBUG)

# Validate pipeline setup
validation_results = pipeline.validate_setup()
for check, passed in validation_results.items():
    print(f"{'✓' if passed else '✗'} {check}: {passed}")

# Get detailed processing summary
summary = pipeline.summary()
print(f"Success rate: {summary['processing']['success_rate']:.1%}")
print(f"Failed indices: {summary['processing']['failed_indices']}")

# Estimate output size
size_estimates = pipeline.estimate_output_size()
print(f"Estimated output: {size_estimates['compressed_gb']:.1f} GB")
```

### Error Recovery

```python
# Handle processing errors gracefully
try:
    pipeline.process_all(show_progress=True)
except Exception as e:
    print(f"Processing failed: {e}")
    
    # Save current state
    pipeline.save_checkpoint('./emergency_checkpoint.json')
    
    # Retry failed observations
    pipeline.retry_failed(show_progress=True)
    
    # Export failed indices for manual review
    pipeline.export_failed_indices('./failed_indices.json')

# Resume from checkpoint
pipeline.resume_from_checkpoint(
    checkpoint_path='./emergency_checkpoint.json',
    store_path='./output/goes_data.zarr',
    continue_processing=True
)
```

### Performance Monitoring

```python
# Monitor processing progress
def progress_callback(progress):
    print(f"Progress: {progress['percent_complete']:.1f}%")
    print(f"Processed: {progress['processed_count']}")
    print(f"Failed: {progress['failed_count']}")
    if progress.get('current_file'):
        print(f"Current: {progress['current_file']}")

# Process with monitoring
pipeline.process_all(progress_callback=progress_callback)

# Get final statistics
pipeline.print_summary()
```

## License

 > This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
---

**GOES Data Builder** - Transforming GOES satellite data into analysis-ready formats with comprehensive metadata and quality control.
