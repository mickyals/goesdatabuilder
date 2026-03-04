# GOESPipelineOrchestrator

## Overview

The `GOESPipelineOrchestrator` class coordinates end-to-end GOES ABI L2+ data processing from raw NetCDF files to CF-compliant Zarr stores. It manages the lifecycle of four core components (catalog, observation, regridder, store), handles error recovery with checkpointing, and supports optional Dask distributed computing.

### Pipeline Flow

```
Raw NetCDF Files -> GOESMetadataCatalog -> GOESMultiCloudObservation -> GeostationaryRegridder -> GOESZarrStore -> CF-compliant Zarr
```

### Design Principles

- Lazy component initialization (each component created on demand or via `initialize_all`)
- Validation before construction (parameters resolved before objects are instantiated)
- Data-driven region detection (orbital slot read from loaded files, not config ordering)
- Defensive cleanup (finalization tolerates partial failures)
- Single orbital slot per pipeline run (different projections require different regridders)

## Configuration

The orchestrator takes three configuration sources:

| Config | Accepts | Used By |
|--------|---------|---------|
| `obs_config` | File path or dict | `GOESMultiCloudObservation`, `GeostationaryRegridder` |
| `store_config` | File path only | `GOESZarrStore` (via `ZarrStoreBuilder._load_config`) |
| `pipeline_config` | File path or dict (optional) | Orchestrator internals (catalog, batching, checkpoints, Dask, logging) |

See the Configuration Files documentation for full YAML schemas.

## Initialization

```python
from goesdatabuilder.pipeline import GOESPipelineOrchestrator

# From config files
pipeline = GOESPipelineOrchestrator.from_configs(
    obs_config='configs/data/goesmulticloudnc.yaml',
    store_config='configs/store/goesmulticloudzarr.yaml',
    pipeline_config='configs/pipeline/goespipeline.yaml',
)

# Or directly
pipeline = GOESPipelineOrchestrator(
    obs_config='configs/data/goesmulticloudnc.yaml',
    store_config='configs/store/goesmulticloudzarr.yaml',
    pipeline_config='configs/pipeline/goespipeline.yaml',
    catalog=existing_catalog,  # optional pre-built catalog
)
```

### Constructor Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `obs_config` | str, Path, or dict | Yes | Data access and regridding configuration. |
| `store_config` | str or Path | Yes | Zarr store configuration. Must be a file path. |
| `pipeline_config` | str, Path, or dict | No | Orchestration configuration. Defaults to empty dict. |
| `catalog` | GOESMetadataCatalog | No | Pre-built catalog instance. |

### What Happens at Construction

1. All three configs are loaded (env vars expanded in raw text before YAML parsing)
2. Logging is configured from pipeline_config
3. Components are set to `None` (lazy initialization)
4. Processing state counters are zeroed
5. `_configured_regions` is read from `store_config['goes']['orbital_slots']`
6. `_default_region` is set to the first configured region (placeholder, overwritten by `initialize_observation`)
7. `_default_bands` is read from `store_config['goes']['bands']`

## Component Initialization

Components can be initialized individually or all at once. Each method is idempotent in the sense that it creates the component if not present, but will reinitialize if called again.

### initialize_all

```python
pipeline.initialize_all(
    store_path='/output/goes_east.zarr',
    overwrite=True,
    region=None,       # auto-detected from data
    bands=None,        # from store_config
    use_catalog=None,  # from pipeline_config
    use_dask_client=None,  # from pipeline_config
)
```

Initialization order: catalog (optional) -> observation -> regridder -> store -> Dask client (optional).

### initialize_catalog

```python
catalog = pipeline.initialize_catalog(force_rebuild=False)
```

Builds or loads a `GOESMetadataCatalog`. Requires `obs_config['data_access']['file_dir']` and `pipeline_config['catalog']['output_dir']`. If `observations.csv` exists in the catalog directory and `force_rebuild` is False, loads from CSV instead of rescanning.

### initialize_observation

```python
obs = pipeline.initialize_observation(
    file_list=None,      # explicit file list overrides catalog
    time_range=None,     # (start, end) tuple for catalog filtering
)
```

Creates a `GOESMultiCloudObservation` from the file list. If no file list is provided, files are sourced from the catalog (initialized automatically if needed) with optional filtering by `orbital_slot` and `scene_id` from pipeline_config.

After construction, the observed orbital slot is read from the first timestep and validated against `_configured_regions`. This sets `_default_region` to match the actual data, ensuring the regridder and store target the correct region.

### initialize_regridder

```python
regridder = pipeline.initialize_regridder(
    reference_band=None,   # override obs_config
    load_cached=None,      # override obs_config
    target_grid=None,      # explicit {'lat': array, 'lon': array}
)
```

Creates a `GeostationaryRegridder` from the observation's source coordinates and projection. Target grid resolution comes from `obs_config['regridding']['target']`. Weights are cached per orbital slot; do not share weights across slots.

### initialize_store

```python
store = pipeline.initialize_store(
    store_path='/output/goes.zarr',
    overwrite=False,
    region=None,   # defaults to _default_region (data-derived)
    bands=None,    # defaults to _default_bands (config-derived)
)
```

Creates a `GOESZarrStore`, initializes the store backend via `_resolve_store`, and creates a single region group with coordinate arrays and CMI/DQF arrays for all specified bands. Store path resolution (env var expansion, Path conversion for local/zip, string preservation for fsspec) is handled entirely by `ZarrStoreBuilder._resolve_store`.

If a store is already open, it is closed with a warning before replacement.

### initialize_dask_client

```python
pipeline.initialize_dask_client(
    n_workers=None,
    threads_per_worker=None,
    memory_limit=None,
    scheduler_address=None,
)
```

Creates a Dask distributed client. Connects to a remote scheduler if `scheduler_address` is provided, otherwise creates a local cluster. Gracefully handles missing `dask.distributed` package.

## Processing

### process_single_observation

```python
store_idx = pipeline.process_single_observation(
    time_idx=0,
    bands=None,
    region=None,
)
```

Processes one timestep: extracts metadata first (fails fast if malformed), then regrids CMI and DQF for each band, and appends to the Zarr store. Uses `get_cmi(band)` / `get_dqf(band)` directly rather than the stateful `.band` setter.

Returns the store time index where data was written.

### process_batch

```python
pipeline.process_batch(
    start_idx=0,
    end_idx=None,        # defaults to total_observations
    bands=None,
    region=None,
    show_progress=True,
    continue_on_error=None,  # defaults from pipeline_config
)
```

Processes a contiguous range of time indices. Delegates to `_process_loop` which handles progress bars, checkpointing, and error recovery.

### process_all

```python
pipeline.process_all(bands=None, region=None, show_progress=True, continue_on_error=None)
```

Convenience wrapper that calls `process_batch(start_idx=0, end_idx=None, ...)`.

### process_time_range

```python
pipeline.process_time_range(
    start_time='2024-01-01',
    end_time='2024-01-31',
    bands=None,
    region=None,
    show_progress=True,
    continue_on_error=None,
)
```

Finds time indices within the specified range using pandas datetime matching, then delegates to `_process_loop`. Accepts strings, datetime objects, or numpy datetime64 values.

### _process_loop (internal)

Shared processing loop used by `process_batch` and `process_time_range`. Handles tqdm progress bars (graceful fallback if tqdm not installed), automatic checkpointing at configured intervals (checkpoint failures are logged but don't halt processing), and error routing (`continue_on_error` controls whether exceptions propagate or are recorded).

## Error Recovery

### retry_failed

```python
pipeline.retry_failed(
    bands=None,
    region=None,
    show_progress=True,
    max_retries=None,  # defaults from pipeline_config['batching']['max_retries']
)
```

Retries previously failed observations. Uses `collections.Counter` on `_failed_indices` to count prior failures per index, enforcing `max_retries` across multiple calls. Indices exceeding the limit are skipped with a warning. Successfully retried observations increment `_processed_count`.

Note: Retried observations are appended to the end of the Zarr time axis, so the store may be out of chronological order after retries. Call `finalize_dataset()` (which can include time sorting) before distribution.

### skip_failed

```python
pipeline.skip_failed()
```

Clears the failed indices list, treating all failures as intentionally skipped.

### export_failed_indices / import_failed_indices

```python
pipeline.export_failed_indices('failed.json')
pipeline.import_failed_indices('failed.json')
```

JSON persistence for failed indices. `import_failed_indices` replaces the current list. *Append mode is under consideration.*

## Checkpointing

### save_checkpoint / load_checkpoint

```python
pipeline.save_checkpoint('checkpoint.json')
pipeline.load_checkpoint('checkpoint.json')
```

Checkpoints store: `processed_count`, `failed_count`, `failed_indices`, `last_processed_idx`, `start_time`, `success_rate`, `elapsed_seconds`, and a timestamp.

### resume_from_checkpoint

```python
pipeline.resume_from_checkpoint(
    checkpoint_path='checkpoint.json',
    store_path='/output/goes.zarr',
    continue_processing=True,
)
```

Loads checkpoint state, reinitializes observation and regridder, opens the existing Zarr store via `GOESZarrStore.from_existing` (not `create_store`), rebuilds the region cache, and optionally continues processing from `last_processed_idx + 1`.

### Automatic Checkpointing

When `pipeline_config['checkpoints']['enabled']` is true, checkpoints are saved automatically every `checkpoint_interval` observations. Old checkpoints are cleaned up based on `keep_last_n`.

## Properties

### Component State

| Property | Type | Description |
|----------|------|-------------|
| `is_initialized` | bool | True if observation, regridder, and store are all initialized. |
| `has_catalog` | bool | True if catalog is available. |
| `has_dask_client` | bool | True if Dask client is active (checks scheduler connectivity). |
| `total_observations` | int | Number of timesteps in the loaded dataset. 0 if observation not initialized. |

### Processing Metrics

| Property | Type | Description |
|----------|------|-------------|
| `processed_count` | int | Number of successfully processed observations. |
| `failed_count` | int | Total failure events (including retries of the same index). |
| `success_rate` | float | `processed_count / (processed_count + unique_failed_indices)`. Based on unique observations, not cumulative events. |

### Configuration (read-only copies)

| Property | Type | Description |
|----------|------|-------------|
| `obs_config` | dict | Deep copy of observation configuration. |
| `store_config` | dict | Deep copy of store configuration. |
| `pipeline_config` | dict | Deep copy of pipeline configuration. |
| `processing_state` | dict | Comprehensive state for checkpointing. |

## Diagnostics

### validate_setup

```python
results = pipeline.validate_setup()
# {'observation_initialized': True, 'regridder_initialized': True, ...}
```

Checks component initialization status and optionally verifies disk space.

### estimate_output_size

```python
estimates = pipeline.estimate_output_size()
# {'uncompressed_gb': 45.2, 'compressed_gb': 11.3, 'compression_ratio': 4.0, 'per_band_gb': 0.7}
```

Estimates based on grid dimensions, band count, and a hardcoded 4x compression ratio.

### summary / print_summary

```python
summary = pipeline.summary()
pipeline.print_summary()
```

Returns/prints status, configuration, processing metrics, elapsed time, and component details (observation timesteps, regridder shape/coverage).

## Finalization

### finalize

```python
pipeline.finalize()
```

Calls `finalize_store()`, `close_dask_client()`, and `close_store()` in sequence, each wrapped in try/except so a failure in one does not prevent cleanup of the others.

### finalize_store

```python
pipeline.finalize_store()
```

Delegates to `GOESZarrStore.finalize_dataset()`, which updates temporal coverage for all initialized regions and adds a final history entry.

### close_dask_client

```python
pipeline.close_dask_client()
```

Shuts down the Dask client. Sets `_dask_client = None` in a `finally` block to ensure cleanup even if `.close()` raises.

### Context Manager

```python
with GOESPipelineOrchestrator.from_configs(...) as pipeline:
    pipeline.initialize_all(store_path='output.zarr')
    pipeline.process_all()
# finalize() called automatically on exit
```

## Multi-Region Processing

Each pipeline run processes one orbital slot because all files in a `GOESMultiCloudObservation` must share the same geostationary projection. To process multiple regions into the same Zarr store:

```python
for slot_config in ['pipeline_east.yaml', 'pipeline_west.yaml']:
    pipeline = GOESPipelineOrchestrator.from_configs(
        obs_config='configs/data/goesmulticloudnc.yaml',
        store_config='configs/store/goesmulticloudzarr.yaml',
        pipeline_config=slot_config,
    )
    pipeline.initialize_all(store_path='/output/goes_data.zarr', overwrite=False)
    pipeline.process_all()
    pipeline.finalize()
```

The second run uses `overwrite=False`, preserving the first region while adding the second. Each pipeline config filters by orbital slot:

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

## Internal Methods

### _load_config

Loads YAML/JSON from file or passes through dicts. Expands env vars in raw text before parsing. Unknown file extensions default to YAML with a warning.

### _setup_logging

Configures console and optional file logging from pipeline_config. Console handler added only if no handlers exist. File handlers deduplicated by resolved path.

### _get_configured_regions

Reads `store_config['goes']['orbital_slots']`. Raises `ValueError` if empty or missing.

### _get_default_bands

Reads `store_config['goes']['bands']` with fallback to `multicloudconstants.BANDS`.

### _set_processing_defaults

Resolves `None` values for bands, region, and end_idx to their defaults. Used by all processing methods.

### _get_files_from_catalog

Queries the catalog's observations DataFrame with optional filters for time range, orbital slot, and scene ID (from pipeline_config).

### _increment_processed

Increments counter, logs milestones at configured intervals.

### _increment_failed

Increments counter, appends index to `_failed_indices`, logs error.

### _should_checkpoint

Returns true every `checkpoint_interval` observations when checkpointing is enabled.

### _auto_checkpoint

Saves checkpoint with UTC timestamp filename, cleans up old checkpoints based on `keep_last_n`.

### _cleanup_old_checkpoints

Removes checkpoint files beyond the retention limit.

## Error Handling

```python
from goesdatabuilder.pipeline import GOESPipelineOrchestrator
from goesdatabuilder.pipeline import ConfigError

# Missing config
try:
    pipeline = GOESPipelineOrchestrator(obs_config='missing.yaml', store_config='store.yaml')
except FileNotFoundError:
    pass

# Invalid orbital slot in data
try:
    pipeline.initialize_observation(file_list=mixed_slot_files)
except ConfigError as e:
    print(e)  # "Observed orbital slot 'GOES-West' not in configured regions ['GOES-East']"

# Pipeline not initialized
try:
    pipeline.process_all()
except RuntimeError:
    print("Call initialize_all() first")
```

## Dependencies

- **numpy**, **pandas**: Array operations and time handling
- **xarray**: Underlying data access (via GOESMultiCloudObservation)
- **yaml**, **json**: Configuration parsing
- **copy**: Deep copying for config isolation
- **logging**: Structured logging throughout
- **tqdm** (optional): Progress bars in processing loop, graceful fallback
- **dask.distributed** (optional): Distributed computing, graceful fallback
- **collections.Counter**: Failure deduplication in retry_failed

## Related Modules

- `GOESMetadataCatalog`: File discovery and metadata cataloging
- `GOESMultiCloudObservation`: Data loading and CF-compliant access
- `GeostationaryRegridder`: Geostationary to lat/lon transformation
- `GOESZarrStore`: CF-compliant Zarr storage
- `ZarrStoreBuilder`: Base store lifecycle management
- `multicloudconstants`: Validation sets, band metadata, DQF flags