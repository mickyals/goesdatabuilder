# Configuration Files

## Overview

The GOESDataBuilder pipeline uses three YAML configuration files, each governing a distinct concern:

1. **obs_config** (`goesmulticloudnc.yaml`): Data access, chunking, and regridding parameters
2. **store_config** (`goesmulticloudzarr.yaml`): Zarr store backend, compression pipelines, and GOES-specific metadata
3. **pipeline_config** (`goespipeline.yaml`): Orchestration, batching, checkpointing, Dask, and logging

This separation means you can change regridding targets without touching store compression, or adjust error handling without touching data access.

```
configs/
├── data/
│   └── goesmulticloudnc.yaml       # obs_config
├── store/
│   └── goesmulticloudzarr.yaml      # store_config
└── pipeline/
    └── goespipeline.yaml            # pipeline_config
```

All three configs support environment variable expansion (e.g., `${GOES_DATA_PATH}`). The obs_config and pipeline_config accept either file paths or dicts. The store_config must be a file path because `ZarrStoreBuilder._load_config` requires it.

---

## Observation Config (`goesmulticloudnc.yaml`)

Used by `GOESMultiCloudObservation` for data loading, `GOESMetadataCatalog` for file discovery, and `GeostationaryRegridder` for grid construction.

```yaml
data_access:
  file_dir: "${GOES_DATA}/GOES18/2024"
  recursive: true

  chunk_size:
    time: 1
    y: -1
    x: -1

  sample_size: 5
  sampling_type: 'even'
  # seed: 1234
  engine: netcdf4
  parallel: false

regridding:
  weights_dir: "${WEIGHTS_PATH}/GOES-East/"
  load_cached: true
  reference_band: 7
  decimals: 6

  target:
    resolution: 0.02
```

### Data Access

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `file_dir` | str | required | Directory containing GOES NetCDF files. Supports env vars. |
| `recursive` | bool | `true` | Search subdirectories for `.nc` files. |
| `chunk_size` | dict | `auto` | Dask chunk sizes per dimension. Spatial dims must be `-1` (full extent) for regridding. |
| `sample_size` | int | `5` | Number of files to validate on initialization. |
| `sampling_type` | str | `'even'` | How to select sample files: `'even'` (evenly spaced) or `'random'`. |
| `seed` | int | `42` | RNG seed when `sampling_type: random`. |
| `engine` | str | `'netcdf4'` | xarray backend engine. |
| `parallel` | bool | `false` | Whether `xr.open_mfdataset` opens files in parallel via `dask.delayed`. |

When using `files` instead of `file_dir` (e.g., from the orchestrator), provide a list of absolute paths. All files must match the GOES MCMIP filename pattern and belong to the same orbital slot.

### Regridding

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `weights_dir` | str | required | Directory for cached Delaunay triangulation weights. One directory per orbital slot. |
| `load_cached` | bool | `true` | Load existing weights if available. Set `false` to force recomputation (~40 min). |
| `reference_band` | int | `7` | Band used to extract source coordinates for weight computation. |
| `decimals` | int | `4` | Decimal places for `np.round` in target grid construction. |

### Target Grid

The target grid can be specified two ways.

Resolution only (auto-compute bounds from source data):
```yaml
target:
  resolution: 0.02
```

Explicit bounds (overrides auto-computation):
```yaml
target:
  lat_min: -60.0
  lat_max: 60.0
  lon_min: -150.0
  lon_max: -30.0
  lat_resolution: 0.02
  lon_resolution: 0.02
```

Separate `lat_resolution` and `lon_resolution` override the shared `resolution` value. Longitude arrays crossing the antimeridian are handled by `build_longitude_array`, which operates in 0-360 space internally.

The `weights_dir` should be per orbital slot (e.g., `GOES-East/`, `GOES-West/`) because each satellite has a different sub-satellite longitude, producing different Delaunay triangulations. Do not share weights across orbital slots.

---

## Store Config (`goesmulticloudzarr.yaml`)

Used by `GOESZarrStore` (via `ZarrStoreBuilder`) for Zarr store creation, compression pipeline resolution, and CF/ACDD metadata. Must be provided as a file path.

```yaml
store:
  type: local
  path: null

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
    shards: null
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

goes:
  orbital_slots: ["GOES-East", "GOES-West", "GOES-Test"]
  bands: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
  spatial_resolution: "2km at nadir"

  global_metadata:
    Conventions: "CF-1.13, ACDD-1.3"
    title: "GOES ABI L2+ Cloud and Moisture Imagery"
    # ... (ACDD fields populated at runtime)

  processing:
    software_name: "geolab"
    software_version: "0.1.0"

  band_metadata:
    1:
      wavelength: 0.47
      long_name: "ABI Cloud and Moisture Imagery reflectance factor"
      standard_name: "toa_bidirectional_reflectance"
      units: "1"
      valid_range: [0.0, 1.0]
      products: [...]
    # ... (all 16 bands)
```

### Store Backend

| Parameter | Type | Options | Description |
|-----------|------|---------|-------------|
| `store.type` | str | `local`, `zip`, `memory`, `fsspec`, `object` | Storage backend. |
| `store.path` | str | | Store location. Typically `null` in config, overridden at runtime. For `fsspec` this is a URL. For `memory` not required. |
| `store.storage_options` | dict | | Additional kwargs for `fsspec` stores (e.g., `anon: true`). |

Path resolution is handled entirely by `ZarrStoreBuilder._resolve_store`. The orchestrator passes the raw `store_path` argument through; `_resolve_store` expands env vars, converts to `Path` for local/zip stores, and preserves strings for fsspec/object stores.

### Zarr Array Pipelines

The `zarr` section defines named compression/chunking presets. `ZarrStoreBuilder.create_array` resolves these by preset name.

`default` is used for CMI arrays (float32, 3D: time x lat x lon). `secondary` is used for DQF arrays (uint8), coordinate arrays (lat, lon, time), and auxiliary arrays (platform_id, scan_mode).

Each preset specifies:

| Parameter | Type | Description |
|-----------|------|-------------|
| `compressor.codec` | str | Format: `'module:ClassName'` (e.g., `'zarr.codecs:BloscCodec'`). Set to `null` for no compression. |
| `compressor.kwargs` | dict | Arguments passed to the codec constructor. |
| `serializer.codec` | str | Byte serializer. `null` uses Zarr's default. |
| `filter.codec` | str | Array-to-array filter. `null` for none. |
| `chunks` | list/str | Chunk shape (e.g., `[1, 64, 64]`) or `'auto'`. |
| `shards` | list/null | Shard shape for Zarr V3 sharding (e.g., `[168, 512, 512]`). `null` disables sharding. |
| `fill_value` | any | Fill value for uninitialized chunks. Cannot be `NaN` for integer dtypes. |

Shard shapes are dimensionality-specific. A 3D shard config applied to a 1D coordinate array will cause a Zarr error. Coordinate arrays use `preset='secondary'` with `shards: null` to avoid this. The `create_array` debug log shows the resolved chunks/shards for troubleshooting.

Callers can override preset values per-array via `**overrides` in `create_array`. For example, coordinate arrays pass `chunks=(len(lat),)` to override the preset's chunk config.

### GOES Configuration

| Parameter | Description |
|-----------|-------------|
| `goes.orbital_slots` | List of regions to support (used as Zarr group names). The orchestrator validates the observed orbital slot against this list. |
| `goes.bands` | Default bands to process (1-16). |
| `goes.spatial_resolution` | Nominal resolution string for metadata. |
| `goes.global_metadata` | Root-level Zarr attributes following ACDD-1.3 conventions. Temporal and geospatial fields are populated at runtime by `GOESZarrStore`. |
| `goes.processing` | Software provenance metadata written to root group. |
| `goes.band_metadata` | Per-band CF attributes (wavelength, units, standard_name, valid_range, products). Falls back to `multicloudconstants.DEFAULT_BAND_METADATA` for any band not specified. |

### Region and Orbital Slot Relationship

The orchestrator determines the active region from the loaded data, not from config ordering. After `initialize_observation`, the `orbital_slot` attribute from the first timestep is read and validated against `goes.orbital_slots`. This ensures GOES-East files write to the `GOES-East` group and GOES-West files write to `GOES-West`. Mixed-slot file lists are rejected because different projections require different regridders.

Each pipeline run processes one orbital slot. To process multiple slots, run the pipeline once per slot, filtering via `pipeline_config.catalog.orbital_slot`.

---

## Pipeline Config (`goespipeline.yaml`)

Used by `GOESPipelineOrchestrator` for orchestration, error handling, checkpointing, and Dask configuration.

```yaml
pipeline:
  name: "GOES ABI L2+ Processing Pipeline"
  version: "1.0.1"
  use_catalog: true

catalog:
  output_dir: "${OUTPUT_PATH}/catalog/"
  orbital_slot: null
  scene_id: null

dask:
  enabled: false
  scheduler_address: null
  local:
    n_workers: 8
    threads_per_worker: 4
    memory_limit: "8GB"
  config:
    "distributed.worker.memory.target": 0.80
    "distributed.worker.memory.spill": 0.90
    "distributed.worker.memory.pause": 0.95
    "distributed.worker.memory.terminate": 0.98
    "distributed.comm.timeouts.connect": "60s"
    "distributed.comm.timeouts.tcp": "60s"

batching:
  checkpoint_interval: 500
  continue_on_error: true
  max_retries: 2

checkpoints:
  enabled: true
  directory: "${OUTPUT_PATH}/checkpoints/"
  keep_last_n: 5

progress:
  show_progress: true
  log_interval: 100

validation:
  check_disk_space: true
  required_free_space_gb: 100

logging:
  level: "INFO"
  log_file: "${OUTPUT_PATH}/logs/pipeline.log"
  format: "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
  date_format: "%Y-%m-%d %H:%M:%S"
```

### Pipeline Defaults

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `pipeline.use_catalog` | bool | `true` | Use metadata catalog for file discovery vs. explicit file list. |

### Catalog

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `catalog.output_dir` | str | `file_dir/catalog/` | Where catalog CSVs are written/loaded. |
| `catalog.orbital_slot` | str/null | `null` | Filter files by orbital slot before loading. Critical for ensuring single-slot processing. |
| `catalog.scene_id` | str/null | `null` | Filter by scene type (`"Full Disk"`, `"CONUS"`, `"Mesoscale"`). |

### Dask

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `dask.enabled` | bool | `false` | Enable Dask distributed client. |
| `dask.scheduler_address` | str/null | `null` | Connect to existing cluster. If null, creates local cluster. |
| `dask.local.n_workers` | int | `8` | Workers for local cluster. |
| `dask.local.threads_per_worker` | int | `4` | Threads per worker. |
| `dask.local.memory_limit` | str | `"8GB"` | Per-worker memory limit. |
| `dask.config` | dict | | Dask configuration overrides applied via `dask.config.set`. |

Spatial dimensions must not be chunked for regridding (set `chunk_size.y: -1, x: -1` in obs_config). Time-dimension chunking enables parallel regridding via `xr.apply_ufunc`.

### Batching and Error Handling

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `batching.checkpoint_interval` | int | `500` | Save processing state every N observations. |
| `batching.continue_on_error` | bool | `true` | Continue batch if a single observation fails. |
| `batching.max_retries` | int | `2` | Maximum retry attempts per failed observation (enforced across calls to `retry_failed` via failure count deduplication). |

### Checkpointing

Checkpoints save `processed_count`, `failed_count`, `failed_indices`, and `last_processed_idx` to JSON. On resume, the pipeline opens the existing Zarr store via `from_existing` (not `create_store`) and rebuilds the region cache before continuing from `last_processed_idx + 1`.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `checkpoints.enabled` | bool | `true` | Enable automatic checkpointing. |
| `checkpoints.directory` | str | `./checkpoints/` | Checkpoint output directory. |
| `checkpoints.keep_last_n` | int | `5` | Retain only the N most recent checkpoints. |

### Progress

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `progress.show_progress` | bool | `true` | Show tqdm progress bars (graceful fallback if tqdm not installed). |
| `progress.log_interval` | int | `100` | Log milestone every N processed observations. |

### Validation

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `validation.check_disk_space` | bool | `true` | Check available disk space before processing. |
| `validation.required_free_space_gb` | float | `100` | Minimum free space in GB. Returns true on error (does not block processing). |

### Logging

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `logging.level` | str | `"INFO"` | Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL). |
| `logging.log_file` | str/null | `null` | File handler path. `null` for console only. Duplicate file handlers are prevented by resolved path comparison. |
| `logging.format` | str | standard | Python logging format string. |
| `logging.date_format` | str/null | `null` | Date format for log timestamps. |

---

## Environment Variables

### Required

| Variable | Used By | Description |
|----------|---------|-------------|
| `GOES_DATA` | obs_config | Base directory containing GOES NetCDF files. |
| `WEIGHTS_PATH` | obs_config | Directory for cached regridding weights. |

### Optional

| Variable | Used By | Description |
|----------|---------|-------------|
| `OUTPUT_PATH` | pipeline_config | Base directory for checkpoints, logs, catalog. |

Env vars are expanded at two levels: `ZarrStoreBuilder._load_config` expands vars in the parsed config dict via `_expand_env_vars`, and `_resolve_store` expands vars in override paths passed at runtime.

---

## Typical Workflow

```python
from goesdatabuilder.pipeline import GOESPipelineOrchestrator

pipeline = GOESPipelineOrchestrator.from_configs(
    obs_config='configs/data/goesmulticloudnc.yaml',
    store_config='configs/store/goesmulticloudzarr.yaml',
    pipeline_config='configs/pipeline/goespipeline.yaml',
)

# Region is auto-detected from loaded data's orbital_slot attribute
pipeline.initialize_all(store_path='/output/goes_east.zarr', overwrite=True)

pipeline.process_all()
pipeline.retry_failed()
pipeline.finalize()
```

For multi-region processing, run the pipeline once per orbital slot with separate pipeline configs:

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
    pipeline = GOESPipelineOrchestrator.from_configs(
        obs_config='configs/data/goesmulticloudnc.yaml',
        store_config='configs/store/goesmulticloudzarr.yaml',
        pipeline_config=config,
    )
    pipeline.initialize_all(store_path='/output/goes_data.zarr', overwrite=False)
    pipeline.process_all()
    pipeline.finalize()
```

The second run uses `overwrite=False` so the existing store's GOES-East region is preserved while GOES-West is added.

---

## Troubleshooting

### Environment Variable Not Found

```bash
echo $GOES_DATA
export GOES_DATA="/path/to/goes/data"
```

### Invalid YAML Syntax

```bash
python -c "import yaml; yaml.safe_load(open('config.yaml'))"
```

### Shard/Chunk Dimensionality Mismatch

If you see errors like `ValueError: chunk_shape needs to be divisible by shard's inner chunk_shape`, check the debug logs from `create_array`. This typically means a 3D shard config from the `default` preset is being applied to a 1D coordinate array. Coordinate arrays should use `preset='secondary'` which has `shards: null`.

### Mixed Orbital Slot Files

If `initialize_observation` raises a `ConfigError` about the observed orbital slot not matching configured regions, your file list contains data from multiple satellites. Filter by orbital slot in the pipeline config:

```yaml
catalog:
  orbital_slot: "GOES-East"
```