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

## Configuration

Configuration settings can be loaded as JSON or yaml files or can be set at runtime as a python dictionary.

### Environment Variables

Some settings can also be set as environment variables:

- `GOES_FILE_DIR`: directory containing GOES input files, exported GOESMetadataCatalog data, or a file path containing a newline separated list of of input files
- `GOES_WEIGHTS_DIR`: directory containing (or to be used to store) regridding weights
- `GOES_OUTPUT_DIR`: directory containing outputs including checkpoints and logs
- `GOES_STORE_DIR`: location on disk of the zarr store (used if the zarr store type is "local")

### Configuration Files

Configuration settings can be modified using JSON or yaml files or using a python dictionary. These will override the default configuration
settings. See [the configuration documentation](#configuration) for more details.

```python
from goesdatabuilder import set_config
set_config("config.yaml", "config.json", config_dict={"data_access": {...}})
```

Multiple files can be used simultaneously and all configuration files will be merged. Values that are not merged (arrays and non-scalar JSON types) will replace values from configuration sources with lower precedence. In the example above, values in the `config_dict` will have
the highest precedence, followed by `config.json`, then `config.yaml`, and finally the default configuration values.

To see all current configuration settings:

```python
from goesdatabuilder import get_config
print(dict(get_config()))
```

The configuration files contain these main sections. Please see below for the default values in yaml format and explanations of each setting:

#### data_access

Settings for accessing GOES input files:

```yaml
data_access:
  # Source data location
  file_source: null # either: a directory path containing input files, exported GOESMetadataCatalog data, a list of input files, or a file path containing a newline separated list of of input files
  recursive: true  # search subdirectories if file_source is a directory path containing input iles
  chunk_size: "auto" # chunk size used when reading .nc data (-1 means no chunking), chunk size is ignored when reading data using the pipeline code

  # Validation
  sample_size: 5  # number of files to validate
  sampling_type: 'even' # how to select which files to validate. Choose from "even" (select files an even steps) or "random" (select randomly)
  seed: null # seed used when sampling_type is "random"

  # xarray
  engine: netcdf4  # xarray backend used to load GOES data files
  parallel: False  # If True, the open and preprocess steps of this function will be performed in parallel using dask.delayed.
```

Note that the "file_source" value can be overridden using the `GOES_FILE_DIR` [environment variable](#environment-variables).

#### regridding

Settings for regridding input data:

```yaml
regridding:
  weights_dir: null # path to use to store/cache calculated weights
  load_cached: true # load weights from cache (see weights_dir) if possible. If false, this will always calculate the weights from the input data
  reference_band: 7  # band used to compute weights (shortwave window)
  decimals:  6 # Number of decimal places to round to. If decimals is negative, it specifies the number of positions to the left of the decimal point.

  # Target grid specification
  target:
    resolution: 0.02  # Degrees (default approach)
    # OR explicit bounds (optional, overrides resolution):
    # lat_min: -60.0
    # lat_max: 60.0
    # lon_min: -150.0
    # lon_max: -30.0
    # lat_resolution: 0.02
    # lon_resolution: 0.02
```

Note that the "weights_dir" value can be overridden using the `GOES_WEIGHTS_DIR` [environment variable](#environment-variables).

#### store

```yaml
store:
  type: local # Storage backend type: local, zip, fsspec, memory, object
  path: null  # path on disk to use when the type is local or zip (or fsspec if fsspec is used to refer to a local store)
  object_store_backend: null
  storage_options: {} # additional keyword options to pass on to the store class
```

Note that the "path" value can be overridden using the `GOES_STORE_DIR` [environment variable](#environment-variables).

#### zarr

Settings used to configure the zarr store: 

```yaml
zarr:
  field: # default compression for 2D arrays and array creation arguments
    compressor: # defines the zarr codec used to compress data
      codec: 'zarr.codecs:BloscCodec' # python module path to the codec class to use
      kwargs: # keyword arguments used when initializing the codec
        cname: zstd
        clevel: 5
        shuffle: bitshuffle
    serializer: # defines the zarr codec used to serialize data
      codec: null
    filter: # defines the zarr codec used to filter data
      codec: null
    chunks: [1, 1024, 1024] # chunk shape
    shards: [1, 4096, 4096] # shard shape (must be a multiple of the chunk shape in every dimension)
    fill_value: null # value used to fill in nan values (fill_value itself cannot be .nan)

  coordinate: # default compression for 1D arrays and array creation arguments (see field above for value descriptions)
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
    chunks: [512]
    fill_value: null
```

Codecs are specified as `'module:ClassName'` strings. Setting `codec: null` disables that stage.

Additional settings groups can be added as long as they have unique names. They can then be referenced as when calling one of the 
functions that uses these methods. E.g.:

- `GOESPipelineOrchestrator.initialize_all`
- `GOESPipelineOrchestrator.initialize_store`
- `GOESZarrStore.initialize_region`
- ...

For example:

```python
from goesdatabuilder import GOESPipelineOrchestrator, set_config
new_zarr_setting = {
  "zarr": {
    "custom": {
      "compressor": {
        "codec": "numcodecs.gzip.GZip", 
        "kwargs": {"level": 2}
      }, 
      "serializer": {"codec": None}, 
      "filter": {
        "codec": "numcodecs.quantize.Quantize", 
        "kwargs": {"digits": 3, "dtype" "uint8"}
      }, 
      "chunks": [1, 1024, 1024], 
      "shards": [1, 4096, 4096], 
      "fill_value": None
    }
  }
}
set_config(config_dict=new_zarr_setting, validate=True)
GOESPipelineOrchestrator().initialize_all(store_path="./example.zarr", cmi_preset="custom")
```

Codecs supported by zarr can be found in the [numcodecs](https://numcodecs.readthedocs.io/en/stable/) project.

Please be aware when overriding the defaults that chunk and shard settings must match the dimensionality of the arrays they are being
applied to. In other words, 1 dimensional arrays will not behave well if divided into 3 dimensional chunks or shards and vice versa. 

#### goes

Settings used to set metadata and ensure consistency for the GOES data

```yaml
goes:
  # Platforms to initialize as top-level Zarr groups
  orbital_slots: ["GOES-East", "GOES-West", "GOES-Test", "GOES-Storage"]

  # Bands to process (example subset; all 16 have metadata)
  bands: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]

  # ---------------------------------------------------------------------------
  # Global metadata (ACDD + CF conventions)
  # Written as root-level Zarr group attributes by GOESZarrStore.initialize_store
  # ---------------------------------------------------------------------------
  global_metadata:
    ... # omitted from documentation due to size (see below for instructions on how to view)

  # ---------------------------------------------------------------------------
  # Band metadata (all 16 ABI bands)
  # Written as variable-level attributes in Zarr.
  # Reflective bands (1-6): units="1", standard_name=toa_bidirectional_reflectance
  # Emissive bands (7-16): units="K", standard_name=toa_brightness_temperature
  # ---------------------------------------------------------------------------
  band_metadata:
    ... # omitted from documentation due to size (see below for instructions on how to view)
```

The global metadata will be added to the zarr store at the root level, the band metadata will be added
to the zarr array metadata for each CMI band. 

The default values for band_metadata probably don't need to be modified as the defaults describe the 
GOES data adequately for most purposes. The default values for the global metadata should be updated
to reflect the creator and publisher details for the created store. We recommend updating the 
`institution`, `creator-*`, `publisher-*`, and `contributor-*` values for your specific use-case.

#### catalog

Settings used to create or load a catalog of data files. 

```yaml
catalog:
  # Catalog CSV output directory
  output_dir: null # default to: "output_path/catalog/" where output_path is set by the pipeline.output_path setting

  # Optional filters applied to catalog before processing
  orbital_slot: null  # "GOES-East", "GOES-West", or null for all
  scene_id: null      # "Full Disk", "CONUS", "Mesoscale", or null for all
```

#### pipeline

Settings used when orchestrating the creation of the zarr store using through the provided pipeline functions:

```yaml
pipeline:
  output_path: ./output # location to write checkpoints, logs, etc.

  #Error handling
  error_handling:
    continue_on_error: true   # Continue processing if single observation fails
    max_retries: 2            # Retry attempts for failed observations

  # Checkpointing
  checkpoints:
    enabled: true # enable checkpoints
    directory: null # default to: "${GOES_OUTPUT_PATH}/checkpoints/"
    interval: 500 # Save state every N observations
    keep_last_n: 5 # Keep n most recent checkpoints only (older checkpoints will be removed)

  # Progress tracking
  progress:
    show_progress: true  # Show tqdm progress bars
    log_interval: 100    # Log progress every N observations

  # Logging
  logging:
    level: "INFO"
    log_file: null # default to: "${GOES_OUTPUT_PATH}/logs/pipeline.log"
    format: "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    date_format: "%Y-%m-%d %H:%M:%S"
```

Note that the "output_path" value can be overridden using the `GOES_OUTPUT_DIR` [environment variable](#environment-variables).


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

Reduce xarray chunk sizes in the data config. Process in smaller batches via `pipeline.process_batch(start_idx=0, end_idx=100)`. Disable the Dask client if overhead is too high. Ensure spatial chunk dimensions are set to `-1` (full extent) for regridding.

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
