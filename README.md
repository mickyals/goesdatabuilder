# GOES Data Builder

A Python package for processing GOES ABI L2+ Multi-Cloud (MCMIP) data from raw NetCDF files to CF-compliant Zarr V3 stores with metadata cataloging, Delaunay-based regridding, and pipeline orchestration.

## Overview

GOES Data Builder provides a pipeline for transforming raw geostationary-projected GOES ABI NetCDF files into analysis-ready Zarr stores with CF-compliant metadata, provenance tracking, and quality control. The pipeline handles coordinate transformation from geostationary fixed grid (x/y radians) to regular lat/lon, with extended DQF flags that track interpolation artifacts.

### Key Features

- **Pipeline orchestration** with checkpointing, error recovery, and retry logic (`GOESPipelineOrchestrator`)
- **CF-compliant Zarr V3 storage** with region groups, band arrays, and provenance attributes (`GOESZarrStore`)
- **Delaunay regridding** with barycentric interpolation and cached weights (~1s load vs ~40min compute) (`GeostationaryRegridder`)
- **Sequential metadata cataloging** with validation and CSV persistence (`GOESMetadataCatalog`)
- **Extended DQF flags** 0-6 tracking original quality, interpolation artifacts, and NaN sources
- **Dask integration** for lazy observation loading and parallel regridding across time
- **YAML configuration** with environment variable expansion and user-defined compression presets
- **Multiple storage backends** via ZarrStoreBuilder: local, memory, zip, fsspec (S3/GCS/Azure), object

## Architecture

```
Raw NetCDF Files
       |
       v
GOESMetadataCatalog ---- scan, validate, extract metadata to CSV
       |
       v
GOESMultiCloudObservation ---- lazy xarray Dataset, multi-file, per-band CMI/DQF access
       |
       v
GeostationaryRegridder ---- Delaunay triangulation, barycentric weights, DQF propagation
       |
       v
GOESZarrStore ---- CF-compliant Zarr V3 with region groups, band arrays, provenance
```

### Core Components

**Data Access Layer** (`goesdatabuilder.data.goes`)

- `GOESMultiCloudObservation`: Lazy xarray interface for GOES MCMIP NetCDF files with per-band `get_cmi(band)` / `get_dqf(band)` accessors and promoted metadata attributes
- `GOESMetadataCatalog`: Sequential file scanning with validation, band statistics extraction, and CSV persistence
- `multicloudconstants`: Central definitions for band metadata (`DEFAULT_BAND_METADATA`, `REFLECTANCE_BANDS`, `BRIGHTNESS_TEMP_BANDS`), DQF flags (`DQF_FLAGS`, flags 0-6), region names (`REGIONS`), filename patterns, and validation sets

**Processing Layer** (`goesdatabuilder.regrid`)

- `GeostationaryRegridder`: Geostationary to lat/lon regridding using Delaunay triangulation with barycentric interpolation, cached weight arrays, DQF classification logic, and diagnostic maps

**Utilities** (`goesdatabuilder.utils`)

- `grid_utils`: Antimeridian-safe `build_longitude_array`, coordinate validation helpers (`validate_longitude_monotonic`)

**Storage Layer** (`goesdatabuilder.store`)

- `ZarrStoreBuilder`: Configuration-driven Zarr V3 store builder supporting local, memory, zip, fsspec, and object backends with user-defined compression presets (`default`, `secondary`, custom)
- `GOESZarrStore`: GOES-specific subclass with region/band hierarchy, CF attributes from `multicloudconstants`, append workflows (`append_observation`, `append_batch`), and `finalize_dataset`

**Orchestration Layer** (`goesdatabuilder.pipelines`)

- `GOESPipelineOrchestrator`: Coordinates all components with checkpointing, error recovery, retry logic, progress tracking, and optional Dask client management

## Installation

### Prerequisites

- Python 3.10+
- Sufficient disk space for data and regridding weight cache

### Setup

```bash
git clone https://github.com/mickyals/goesdatabuilder.git
cd goesdatabuilder

conda create -n goesdatabuilder python=3.13
conda activate goesdatabuilder

pip install -e .
```

### Verify

```python
from goesdatabuilder.data.goes.multicloud import GOESMultiCloudObservation
from goesdatabuilder.regrid.geostationary import GeostationaryRegridder
from goesdatabuilder.store.datasets.goesmulticloudzarr import GOESZarrStore
print("GOES Data Builder installed successfully!")
```

## Quick Start

### Pipeline Usage

```python
from goesdatabuilder.pipelines.goesmulticloudpipeline import GOESPipelineOrchestrator

pipeline = GOESPipelineOrchestrator.from_configs(
    obs_config='./configs/data/goesmulticloudnc.yaml',
    store_config='./configs/store/goesmulticloudzarr.yaml',
    pipeline_config='./configs/pipelines/goesmulticloud.yaml'
)

pipeline.initialize_all(
    store_path='./output/goes_data.zarr',
    use_catalog=True,
    use_dask_client=False
)

pipeline.process_all(show_progress=True, continue_on_error=True)
pipeline.finalize()
pipeline.print_summary()
```

### Step-by-Step

```python
from goesdatabuilder.pipelines.goesmulticloudpipeline import GOESPipelineOrchestrator

pipeline = GOESPipelineOrchestrator(
    obs_config='./configs/data/goesmulticloudnc.yaml',
    store_config='./configs/store/goesmulticloudzarr.yaml'
)

catalog = pipeline.initialize_catalog()
observation = pipeline.initialize_observation()
regridder = pipeline.initialize_regridder()
store = pipeline.initialize_store('./output/goes.zarr')

pipeline.process_time_range(
    start_time='2024-01-01T00:00:00',
    end_time='2024-01-01T23:59:59',
    show_progress=True
)

if pipeline.failed_count > 0:
    pipeline.retry_failed()

pipeline.finalize()
```

### Individual Components

```python
from goesdatabuilder.data.goes.multicloud import GOESMultiCloudObservation
from goesdatabuilder.regrid.geostationary import GeostationaryRegridder
from goesdatabuilder.store.datasets.goesmulticloudzarr import GOESZarrStore

# Load data
obs = GOESMultiCloudObservation(config)

# Initialize regridder with weight caching
regridder = GeostationaryRegridder(
    source_x=obs.x.values,
    source_y=obs.y.values,
    projection=obs.projection,
    target_resolution=0.02,
    weights_dir='./weights/GOES-East/',
    load_cached=True
)

# Initialize store
store = GOESZarrStore('./configs/store/goesmulticloudzarr.yaml')
store.initialize_store('./output/goes_data.zarr')
store.initialize_region(
    region='GOES-East',
    lat=regridder.target_lat,
    lon=regridder.target_lon,
    bands=list(range(1, 17)),
    include_dqf=True,
    regridder=regridder
)

# Process observations
for t in range(len(obs.time)):
    cmi_data = {}
    dqf_data = {}
    for band in range(1, 17):
        cmi_2d = obs.get_cmi(band).isel(time=t)
        dqf_2d = obs.get_dqf(band).isel(time=t)
        cmi_data[band] = regridder.regrid(cmi_2d).values
        dqf_data[band] = regridder.regrid_dqf(dqf_2d).values

    timestamp = obs.time.isel(time=t).values
    platform_id = str(obs.isel_time(t)['platform_id'].values)

    store.append_observation(
        region='GOES-East',
        timestamp=timestamp,
        platform_id=platform_id,
        cmi_data=cmi_data,
        dqf_data=dqf_data
    )

store.finalize_dataset()
store.close_store()
```

Or using the regridder's convenience method:

```python
for t in range(len(obs.time)):
    obs_dict = regridder.regrid_to_observation_dict(obs, time_idx=t, bands=list(range(1, 17)))
    store.append_observation('GOES-East', **obs_dict)

store.finalize_dataset()
store.close_store()
```

## Project Structure

```
goesdatabuilder/
├── goesdatabuilder/
│   ├── __init__.py
│   ├── data/
│   │   ├── __init__.py
│   │   └── goes/
│   │       ├── __init__.py
│   │       ├── multicloud.py              # GOESMultiCloudObservation
│   │       ├── multicloudcatalog.py       # GOESMetadataCatalog
│   │       └── multicloudconstants.py     # Band metadata, DQF flags, validation sets
│   ├── regrid/
│   │   ├── __init__.py
│   │   └── geostationary.py               # GeostationaryRegridder
│   ├── store/
│   │   ├── __init__.py
│   │   ├── zarrstore.py                   # ZarrStoreBuilder (base)
│   │   └── datasets/
│   │       ├── __init__.py
│   │       └── goesmulticloudzarr.py      # GOESZarrStore
│   ├── pipelines/
│   │   ├── __init__.py
│   │   └── goesmulticloudpipeline.py      # GOESPipelineOrchestrator
│   └── utils/
│       ├── __init__.py
│       └── grid_utils.py                  # Longitude array construction, validation
├── configs/
│   ├── data/
│   │   └── goesmulticloudnc.yaml
│   ├── pipelines/
│   │   └── goesmulticloud.yaml
│   └── store/
│       └── goesmulticloudzarr.yaml
├── goesdatabuilder-docs/
│   ├── configs/
│   │   └── configuration-files.md
│   ├── data/goes/
│   │   ├── GOESMultiCloudObservation.md
│   │   ├── GOESMetadataCatalog.md
│   │   └── multicloudconstants.md
│   ├── pipelines/
│   │   └── goesmulticloudpipeline.md
│   ├── regrid/
│   │   └── geostationaryregridder.md
│   ├── store/
│   │   ├── zarrstore.md
│   │   └── goesmulticloudzarr.md
│   └── utils/
│       └── grid_utils.md
│
├── pyproject.toml
├── README.md
└── LICENSE
```

## Configuration

### Environment Variables

```bash
export GOES_DATA_PATH="/path/to/goes/netcdf/files"
export WEIGHTS_PATH="/path/to/regridding/weights/cache"
export STORE_PATH="/path/to/output/zarr/stores"
```

### Configuration Files

The package uses three YAML configuration files located in `configs/`:

**Data Configuration** (`configs/data/goesmulticloudnc.yaml`): File discovery (`file_dir`, `pattern`), xarray chunking (`chunk_size`), regridding parameters (target grid bounds/resolution, reference band, weight caching directory), and performance options.

**Store Configuration** (`configs/store/goesmulticloudzarr.yaml`): Zarr V3 backend selection (`store.type`, `store.path`), compression presets under the `zarr` key (`default` for CMI arrays, `secondary` for coordinates, plus any custom presets), and GOES-specific metadata (`goes.platforms`, `goes.bands`, `goes.band_metadata`).

**Pipeline Configuration** (`configs/pipelines/goesmulticloud.yaml`): Catalog settings, Dask client options, batching/checkpointing parameters, logging configuration. Optional; the orchestrator uses sensible defaults without it.

### Compression Preset Structure

Presets are defined directly under the `zarr` key, not nested under `zarr.compression`:

```yaml
zarr:
  zarr_format: 3
  default:
    compressor:
      codec: 'zarr.codecs:BloscCodec'
      kwargs:
        cname: zstd
        clevel: 5
        shuffle: bitshuffle
    serializer:
      codec: null
    filter:
      codec: null
    chunks: auto
    fill_value: null
  secondary:
    compressor:
      codec: 'zarr.codecs:BloscCodec'
      kwargs:
        cname: zstd
        clevel: 5
        shuffle: bitshuffle
    serializer:
      codec: null
    filter:
      codec: null
    chunks: auto
    fill_value: null
```

See [Configuration Documentation](goesdatabuilder-docs/configs/configuration-files.md) for full reference.

## Documentation

### Core Components

- [GOESPipelineOrchestrator](goesdatabuilder-docs/pipelines/goesmulticloudpipeline.md)
- [GOESMultiCloudObservation](goesdatabuilder-docs/data/goes/GOESMultiCloudObservation.md)
- [GeostationaryRegridder](goesdatabuilder-docs/regrid/geostationaryregridder.md)
- [GOESZarrStore](goesdatabuilder-docs/store/goesmulticloudzarr.md)

### Supporting Components

- [GOESMetadataCatalog](goesdatabuilder-docs/data/goes/GOESMetadataCatalog.md)
- [multicloudconstants](goesdatabuilder-docs/data/goes/multicloudconstants.md)
- [ZarrStoreBuilder](goesdatabuilder-docs/store/zarrstore.md)
- [grid\_utils](goesdatabuilder-docs/utils/grid_utils.md)
- [Configuration Files](goesdatabuilder-docs/configs/configuration-files.md)

## Troubleshooting

### Memory

Reduce xarray chunk sizes in the data config. Process in smaller batches via `pipeline.process_batch(start_idx=0, end_idx=100)`. Disable the Dask client if overhead is too high.

### Weight Computation

If regridding weights are corrupted, delete the weights directory and reinitialize the regridder with `load_cached=False`. Check coverage with `regridder.coverage_fraction`.

### File Discovery

Rebuild the catalog with `pipeline.initialize_catalog(force_rebuild=True)`. Verify file counts via `len(catalog.observations)`.

### Error Recovery

```python
# Save state on failure
pipeline.save_checkpoint('./checkpoint.json')

# Retry failed observations
pipeline.retry_failed(show_progress=True)

# Export failures for manual review
pipeline.export_failed_indices('./failed_indices.json')

# Resume later
pipeline.resume_from_checkpoint(
    checkpoint_path='./checkpoint.json',
    store_path='./output/goes_data.zarr'
)
```

### Diagnostics

```python
results = pipeline.validate_setup()
state = pipeline.processing_state
print(f"Success rate: {pipeline.success_rate:.1%}")
print(f"Failed indices: {state['failed_indices']}")

estimates = pipeline.estimate_output_size()
print(f"Estimated: {estimates['compressed_gb']:.1f} GB compressed")
```

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.