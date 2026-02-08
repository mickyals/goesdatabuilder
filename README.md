# GOES Data Builder

> A comprehensive Python package for processing GOES ABI L2+ data from raw NetCDF files to CF-compliant Zarr stores with full metadata management and regridding capabilities.

## 🌟 Overview

The GOES Data Builder provides a complete end-to-end pipeline for processing GOES (Geostationary Operational Environmental Satellite) ABI (Advanced Baseline Imager) Level 2+ data. It transforms raw geostationary-projected NetCDF files into analysis-ready Zarr stores with CF-compliant metadata, provenance tracking, and quality control.

### Key Features

- **🔄 End-to-End Processing**: Complete pipeline from discovery to final Zarr store
- **📊 CF-Compliant Storage**: Full CF-1.13 and ACDD-1.3 metadata compliance
- **🗺️ Advanced Regridding**: Delaunay triangulation with barycentric interpolation
- **📋 Metadata Management**: Automated metadata extraction and cataloging
- **🎯 Multi-Platform Support**: GOES-East, GOES-West, and GOES-Test platforms
- **⚡ Performance Optimized**: Parallel processing and efficient memory management
- **🔧 Configurable**: Flexible YAML-based configuration system
- **📈 Quality Control**: Extended DQF flags and validation

## 🏗️ Architecture

```
Raw NetCDF Files → Metadata Catalog → Regridding → Zarr Store → QC Reports
                    ↓                    ↓              ↓
            GOESMetadataCatalog  GeostationaryRegridder  GOESZarrStore
```

### Core Components

1. **Data Access Layer**
   - `GOESMultiCloudObservation`: CF-aligned interface for GOES data
   - `GOESMetadataCatalog`: Metadata extraction and cataloging

2. **Processing Layer**
   - `GeostationaryRegridder`: Geostationary to lat/lon regridding
   - Extended DQF flags for regridded data quality tracking

3. **Storage Layer**
   - `ZarrStoreBuilder`: Configurable Zarr V3 store builder
   - `GOESZarrStore`: GOES-specific CF-compliant Zarr store

4. **Pipeline Layer**
   - `GOESMultiCloudPipeline`: Complete end-to-end processing pipeline

## 📦 Installation

### Prerequisites

- Python 3.11 or higher
- Conda or pip package manager
- Sufficient disk space for data and cache (recommended: 100GB+)

### 1. Clone the Repository

```bash
git clone https://github.com/mickyals/goesdatabuilder.git
cd goesdatabuilder
```

### 2. Create and Activate Conda Environment

```bash
conda create -n goesdatabuilder python=3.11
conda activate goesdatabuilder
```

### 3. Install Dependencies

```bash
# Install from requirements file
pip install -r requirements.txt

# Or install key dependencies manually
pip install xarray numpy pandas zarr scipy netcdf4 pyyaml
```

### 4. Verify Installation

```python
from goesdatabuilder import GOESMultiCloudObservation, GeostationaryRegridder, GOESZarrStore
print("GOES Data Builder installed successfully!")
```

## 🚀 Quick Start

### Basic Usage

```python
from goesdatabuilder.pipelines.goesmulticloudpipeline import GOESMultiCloudPipeline

# Initialize pipeline with configuration
pipeline = GOESMultiCloudPipeline(
    data_config='./configurations/data/goesmulticloudnc.yaml',
    store_config='./configurations/store/goesmulticloudzarr.yaml',
    output_dir='./output/'
)

# Run complete pipeline
results = pipeline.run()
print(f"Processed {len(results['processed_files'])} files")
print(f"Output: {results['output_path']}")
```

### Step-by-Step Processing

```python
from goesdatabuilder.data.goes.multicloud import GOESMultiCloudObservation
from goesdatabuilder.regrid.geostationary import GeostationaryRegridder
from goesdatabuilder.store.datasets.goes import GOESZarrStore

# 1. Load GOES data
obs = GOESMultiCloudObservation('/path/to/goes/file.nc')

# 2. Initialize regridder
regridder = GeostationaryRegridder(
    source_x=obs.x.values,
    source_y=obs.y.values,
    projection=obs.projection,
    target_resolution=0.02
)

# 3. Initialize store
store = GOESZarrStore('./configurations/store/goesmulticloudzarr.yaml')
store.initialize_store('./output/goes_data.zarr')
store.initialize_region('GOES-East', regridder.target_lat, regridder.target_lon)

# 4. Process data
for i in range(len(obs.time)):
    obs_dict = regridder.regrid_to_observation_dict(obs, time_idx=i)
    store.append_observation('GOES-East', obs_dict)
```

## 📁 Project Structure

```
goesdatabuilder/
├── goesdatabuilder/                    # Main package
│   ├── data/                          # Data access layer
│   │   └── goes/
│   │       ├── multicloud.py           # GOES data interface
│   │       └── multicloudcatalog.py  # Metadata cataloging
│   ├── regrid/                        # Processing layer
│   │   └── geostationary.py        # Regridding engine
│   ├── store/                         # Storage layer
│   │   ├── zarrstore.py            # Base Zarr builder
│   │   └── datasets/
│   │       └── goes.py              # GOES-specific store
│   ├── pipelines/                     # Pipeline layer
│   │   └── goesmulticloudpipeline.py # Complete pipeline
│   └── configurations/               # Configuration files
│       ├── data/
│       │   └── goesmulticloudnc.yaml
│       └── store/
│           └── goesmulticloudzarr.yaml
├── goesdatabuilder-docs/               # Documentation
│   ├── configs/                       # Configuration documentation
│   ├── data/                         # Data layer documentation
│   ├── regrid/                       # Regridding documentation
│   ├── store/                        # Storage documentation
│   └── pipelines/                    # Pipeline documentation
├── requirements.txt                    # Python dependencies
├── setup.py                          # Package setup
└── README.md                          # This file
```

## ⚙️ Configuration

### Environment Variables

```bash
# Required
export GOES_DATA_PATH="/path/to/goes/data"
export WEIGHTS_PATH="/path/to/weights/cache"

# Optional
export STORE_PATH="/path/to/output/stores"
export LOG_LEVEL="INFO"
```

### Configuration Files

The package uses YAML configuration files for different aspects:

- **`goesmulticloudnc.yaml`**: Data access and processing parameters
- **`goesmulticloudzarr.yaml`**: Zarr store and GOES-specific metadata

See [Configuration Documentation](goesdatabuilder-docs/configs/configuration-files.md) for detailed configuration options.

## 📚 Documentation

### Core Components

- **[GOESMultiCloudObservation](goesdatabuilder-docs/data/goes/multicloud.md)**: CF-aligned data interface
- **[GeostationaryRegridder](goesdatabuilder-docs/regrid/geostationaryregridder.md)**: Regridding engine
- **[GOESZarrStore](goesdatabuilder-docs/store/datasets/goes.md)**: GOES-specific Zarr store
- **[ZarrStoreBuilder](goesdatabuilder-docs/store/zarrstore.md)**: Base Zarr store builder

### Supporting Components

- **[GOESMetadataCatalog](goesdatabuilder-docs/data/goes/goesmetadatacatalog.md)**: Metadata cataloging
- **[GOESMultiCloudPipeline](goesdatabuilder-docs/pipelines/goesmulticloudpipeline.md)**: Complete processing pipeline
- **[Configuration Files](goesdatabuilder-docs/configs/configuration-files.md)**: Configuration reference

## 🎯 Use Cases

### Research Applications

- **Climate Studies**: Long-term time series analysis of atmospheric conditions
- **Weather Monitoring**: Real-time weather pattern analysis and forecasting
- **Disaster Response**: Wildfire detection, hurricane tracking, severe weather monitoring
- **Agricultural Monitoring**: Vegetation health, drought assessment, crop yield prediction

### Data Processing

- **Batch Processing**: Process historical archives efficiently
- **Real-time Processing**: Stream processing for near-real-time applications
- **Regional Analysis**: Focus on specific geographic areas of interest
- **Multi-Platform Analysis**: Combine data from multiple GOES satellites

### Integration Examples

```python
# Climate research example
pipeline = GOESMultiCloudPipeline(
    data_dir='/archive/goes/2023/',
    bands=[1, 2, 3, 7, 14],  # Key climate bands
    target_resolution=0.05  # 5km resolution
)

# Weather monitoring example
pipeline = GOESMultiCloudPipeline(
    data_dir='/realtime/goes/',
    platforms=['GOES-East'],
    bands=[7, 8, 13, 14],  # Weather-relevant bands
    target_resolution=0.02  # 2km resolution
)

# Disaster response example
pipeline = GOESMultiCloudPipeline(
    data_dir='/emergency/goes/',
    bands=[7, 14, 15],  # Fire and storm detection
    target_resolution=0.01  # 1km high resolution
)
```

## 🔧 Performance Optimization

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
```

### Parallel Processing

```python
# Configure for parallel processing
pipeline = GOESMultiCloudPipeline(
    parallel_workers=8,  # Number of CPU cores
    batch_size=200,      # Larger batches
    cache_weights=True    # Cache regridding weights
)
```

## 🐛 Troubleshooting

### Common Issues

#### Memory Errors
```bash
# Reduce chunk sizes
export GOES_CHUNK_SIZE="256"

# Use smaller batches
pipeline.set_batch_size(50)
```

#### File Not Found
```bash
# Check data directory
ls $GOES_DATA_PATH

# Verify file patterns
find $GOES_DATA_PATH -name "OR_ABI-L2-*.nc"
```

#### Weight Computation Issues
```bash
# Clear corrupted weights
rm -rf $WEIGHTS_PATH/GOES-East/

# Recompute weights
pipeline.recompute_weights()
```

### Debug Mode

```python
# Enable detailed logging
import logging
logging.basicConfig(level=logging.DEBUG)

# Enable validation
pipeline.enable_debug_logging()
```

## 🤝 Contributing

We welcome contributions! Please see our [Contributing Guidelines](CONTRIBUTING.md) for details.

### Development Setup

```bash
# Clone repository
git clone https://github.com/mickyals/goesdatabuilder.git
cd goesdatabuilder

# Create development environment
conda create -n goesdatabuilder-dev python=3.11
conda activate goesdatabuilder-dev

# Install in development mode
pip install -e .

# Install development dependencies
pip install pytest black flake8 mypy
```

### Running Tests

```bash
# Run all tests
pytest

# Run specific test suite
pytest tests/test_regrid.py

# Run with coverage
pytest --cov=goesdatabuilder
```

## 📄 License

 > This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
---

**GOES Data Builder** - Transforming GOES satellite data into analysis-ready formats with comprehensive metadata and quality control.
