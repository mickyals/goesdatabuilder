# GOES Data Builder

A Python package for processing GOES ABI L2+ Multi-Cloud (MCMIP) data from raw NetCDF files to CF-compliant Zarr V3 stores with metadata cataloging, Delaunay-based regridding, and pipeline orchestration.

## Overview

GOES Data Builder provides a pipeline for transforming raw geostationary-projected GOES ABI NetCDF files into analysis-ready Zarr stores with CF-compliant metadata, provenance tracking, and quality control. The pipeline handles coordinate transformation from geostationary fixed grid (x/y radians) to regular lat/lon, with extended DQF flags that track interpolation artifacts.

### Key Features

- **Pipeline orchestration** with checkpointing, error recovery, retry logic, and data-driven region detection (`GOESPipelineOrchestrator`)
- **CF-compliant Zarr V3 storage** with region groups, band arrays, cached validation, and provenance attributes (`GOESZarrStore`)
- **Delaunay regridding** with barycentric interpolation and cached weights (~1s load vs ~40min compute) (`GeostationaryRegridder`)
- **Sequential metadata cataloging** with validation and CSV persistence (`GOESMetadataCatalog`)
- **Extended DQF flags** 0-6 tracking original quality, interpolation artifacts, and NaN sources
- **Dask integration** for lazy observation loading and parallel regridding across time
- **YAML configuration** with environment variable expansion and user-defined compression presets
- **Multiple storage backends** via ZarrStoreBuilder: local, memory, zip, fsspec (S3/GCS/Azure), object
- **Single orbital slot per run** with automatic region detection from loaded data

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

Each pipeline run processes one orbital slot (GOES-East, GOES-West, etc.) because each satellite has a different sub-satellite longitude, producing different geostationary projections and Delaunay triangulations. The orchestrator auto-detects the orbital slot from the loaded data and validates it against the store config.

### Core Components

**Data Access Layer** (`goesdatabuilder.data.goes`)

- `GOESMultiCloudObservation`: Lazy xarray interface for GOES MCMIP NetCDF files with per-band `get_cmi(band)` / `get_dqf(band)` accessors and promoted metadata attributes
- `GOESMetadataCatalog`: Sequential file scanning with validation, band statistics extraction, and CSV persistence
- `multicloudconstants`: Central definitions for band metadata (`DEFAULT_BAND_METADATA`, `REFLECTANCE_BANDS`, `BRIGHTNESS_TEMP_BANDS`, `BANDS`), DQF flags (`DQF_FLAGS`, named constants 0-6), region names (`REGIONS`), filename patterns, and validation sets

**Processing Layer** (`goesdatabuilder.regrid`)

- `GeostationaryRegridder`: Geostationary to lat/lon regridding using Delaunay triangulation with barycentric interpolation, cached weight arrays, DQF classification logic, and diagnostic maps

**Utilities** (`goesdatabuilder.utils`)

- `grid_utils`: Antimeridian-safe `build_longitude_array`, `is_antimeridian_crossing` detection, and `validate_longitude_monotonic` (checks in 0-360 space for crossing grids)

**Storage Layer** (`goesdatabuilder.store`)

- `ZarrStoreBuilder`: Configuration-driven Zarr V3 store builder supporting local, memory, zip, fsspec, and object backends with user-defined compression presets (`default`, `secondary`, custom), env var expansion in store paths, and context manager support
- `GOESZarrStore`: GOES-specific subclass with region/band hierarchy, CF attributes from `multicloudconstants`, per-region shape/band caches for fast append validation, append workflows (`append_observation`, `append_batch`), and `finalize_dataset`

**Orchestration Layer** (`goesdatabuilder.pipelines`)

- `GOESPipelineOrchestrator`: Coordinates all components with data-driven region detection, checkpointing, error recovery with cross-call retry limits, progress tracking, and optional Dask client management

## Installation

### Prerequisites

- Python 3.12+
- Sufficient disk space for data and regridding weight cache
- Sufficient memory to contain approximately 4x the size of a single GOES .nc file while regridding 

### Setup

```bash
pip install git+https://github.com/mickyals/goesdatabuilder.git
```

### Verify

```python
from goesdatabuilder import GOESMultiCloudObservation, GeostationaryRegridder
print("GOES Data Builder installed successfully!")
```

## Configuration

Configuration settings can be loaded as JSON or yaml files or can be set at runtime as a python dictionary.

For all configuration settings, options, and usage please see the 
[configuration documentation](./goesdatabuilder-docs/configs/configuration.md)

To see all default configuration settings:

```python
from goesdatabuilder import get_config
print(dict(get_config()))
```

## Quick Start

### Pipeline Usage

```python
from goesdatabuilder import GOESPipelineOrchestrator

# This will use the default configuration values (see below for alternatives)
pipeline = GOESPipelineOrchestrator()

# Region is auto-detected from loaded data's orbital_slot
pipeline.initialize_all(
    store_path='./output/goes_data.zarr',
    overwrite=True,
    use_dask_client=False
)

pipeline.process_all(show_progress=True, continue_on_error=True)
pipeline.retry_failed()
pipeline.finalize()
pipeline.print_summary()
```

### Multi-Region Processing

Each pipeline run processes one orbital slot. To process multiple regions into the same Zarr store, run once per slot with the appropriate catalog filter:

```yaml
# pipeline_east.yaml
catalog:
  orbital_slot: "GOES-East"
```

```yaml
# pipeline_west.yaml
catalog:
  orbital_slot: "GOES-West"
```

```python
for config in ['pipeline_east.yaml', 'pipeline_west.yaml']:
    set_config(config)
    pipeline = GOESPipelineOrchestrator()
    pipeline.initialize_all(store_path='./output/goes_data.zarr', overwrite=False)
    pipeline.process_all()
    pipeline.finalize()
```

By setting `overwrite=False` the zarr store will preserve the first region while adding the second.

### Step-by-Step

The following executes the pipeline step by step for a specific time range. By executing step by step you have more
control over the execution of the pipeline steps.

```python
from goesdatabuilder import GOESPipelineOrchestrator

pipeline = GOESPipelineOrchestrator()

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

The goesdatabuilder library also provides the individual component classes used by the pipeline for even
more customizable workflows. For example:

```python
from goesdatabuilder import GOESMultiCloudObservation, GeostationaryRegridder, GOESZarrStore

# Load data
obs = GOESMultiCloudObservation(...)

# Initialize regridder with weight caching
regridder = GeostationaryRegridder(
    source_x=obs.x.values,
    source_y=obs.y.values,
    projection=obs.satellite_projection,
    target_resolution=0.02,
    weights_dir='./weights/GOES-East/',
    load_cached=True
)

# Initialize store
store = GOESZarrStore(...)
store.initialize_store("./store.zarr", overwrite=True)
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

## Documentation

### Core Components

- [GOESPipelineOrchestrator](goesdatabuilder-docs/pipelines/GOESPipelineOrchestrator.md)
- [GOESMultiCloudObservation](goesdatabuilder-docs/data/goes/GOESMultiCloudObservation.md)
- [GeostationaryRegridder](goesdatabuilder-docs/regrid/GeostationaryRegridder.md)
- [GOESZarrStore](goesdatabuilder-docs/store/GOESZarrStore.md)

### Supporting Components

- [GOESMetadataCatalog](goesdatabuilder-docs/data/goes/GOESMetadataCatalog.md)
- [multicloudconstants](goesdatabuilder-docs/data/goes/multicloudconstants.md)
- [ZarrStoreBuilder](goesdatabuilder-docs/store/ZarrStoreBuilder.md)
- [grid_utils](goesdatabuilder-docs/utils/grid_utils.md)
- [Configuration](goesdatabuilder-docs/configs/configuration.md)

## Troubleshooting

### Memory

Reduce the "worker_threads" configuration setting. Each thread will regrid a single band in memory and if multiple simultaneous threads have
regridded data in memory before they can write the outputs to the store, this may exceed the available memory.

### Weight Computation

If regridding weights are corrupted, delete the weights directory and reinitialize the regridder with `load_cached=False`. Check coverage with `regridder.coverage_fraction`. Weight directories should be per orbital slot (do not share GOES-East weights with GOES-West).

### Shard/Chunk Errors

If you see `ValueError: chunk_shape needs to be divisible by shard's inner chunk_shape`, a 3D shard config from the `default` preset is being applied to a 1D coordinate array. Check the `create_array` debug logs. Coordinate arrays should use `preset='secondary'` which has `shards: null`.

### Mixed Orbital Slots

If `initialize_observation` raises a `ConfigError` about the observed orbital slot not matching configured regions, your file list contains data from multiple satellites. Filter by orbital slot in the pipeline config:

```yaml
catalog:
  orbital_slot: "GOES-East"
```

### File Discovery

Rebuild the catalog with `pipeline.initialize_catalog(force_rebuild=True)`. Verify file counts via `len(catalog.observations)`.

### Error Recovery

```python
# Save state on failure
pipeline.save_checkpoint('./checkpoint.json')

# Retry failed observations (max_retries enforced across calls)
pipeline.retry_failed(show_progress=True)

# Export failures for manual review
pipeline.export_failed_indices('./failed_indices.json')

# Resume from checkpoint (opens existing store, does not recreate)
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

pipeline.print_summary()
```

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
