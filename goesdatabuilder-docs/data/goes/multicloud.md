# GOESMultiCloudObservation

## Overview

The `GOESMultiCloudObservation` class provides a CF-compliant interface for accessing and processing GOES ABI L2+ CMI data with time-indexed structure. It handles both single-file and multi-file datasets, providing a unified interface for geostationary satellite data analysis.

### Key Features

- CF-compliant data structure with time-indexed variables following climate data conventions
- Single and multi-file support with automatic temporal concatenation along the time dimension
- Lazy evaluation using xarray/Dask for memory-efficient operations on large datasets
- Band selection with wavelength-based operations and validation
- Comprehensive metadata access through promoted global attributes from NetCDF files
- Flexible configuration system supporting YAML/JSON files or dictionaries
- Built-in validation for GOES naming conventions, orbital parameters, and multi-file consistency
- Export functionality for metadata cataloging via DataFrame or records

## Architecture

The class promotes NetCDF global attributes to time-indexed variables (defined in `multicloudconstants.PROMOTED_ATTRS`), enabling proper concatenation across files while maintaining per-observation provenance tracking.

### Data Processing Pipeline

```
Raw GOES Files -> Validation -> Preprocessing -> Time-Indexing -> CF-Compliant Dataset
```

Preprocessing steps per file:
1. Validate orbital slot against `VALID_ORBITAL_SLOTS`
2. Expand dataset with `time` dimension
3. Assign `time` coordinate from the `t` coordinate in the source file
4. Promote global attributes to time-indexed variables

### Pipeline Integration

```
GOESMultiCloudObservation (this) -> GeostationaryRegridder -> GOESZarrStore
```

This class provides raw geostationary-projected data. CMI/DQF arrays require regridding before storage. All files in a single observation must belong to the same orbital slot, as the regridder is built from a single satellite projection. The orchestrator validates this by reading `orbital_slot` from the first timestep after initialization.

## Class Structure

### Initialization

```python
from goesdatabuilder import GOESMultiCloudObservation

# Initial default arguments are determined by the configuration (set using set_config)
obs = GOESMultiCloudObservation()

# Initial arguments can also be directly passed to the initializer, this will override the configuration defaults
obs = GOESMultiCloudObservation(
    file_source = ["file1.nc", "file2.nc"],
    sampling_type = "random"
)
```

### Configuration

#### Configuration Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `file_source` | list/str | from configuration: data_access.file_source | see [below](#file-source) for details |
| `recursive` | bool | from configuration: data_access.recursive | Search subdirectories when `file_dir` refers to a directory. |
| `engine` | str | from configuration: data_access.engine | xarray backend engine. |
| `chunk_size` | dict/str | from configuration: data_access.chunk_size | Dask chunk sizes per dimension (e.g., `{time: 1, y: -1, x: -1}`). |
| `parallel` | bool | from configuration: data_access.parallel | Whether `xr.open_mfdataset` opens files in parallel via `dask.delayed`. |
| `sample_size` | int | from configuration: data_access.sample_size | Number of files to validate on initialization. *Under consideration for multifile handling refinement.* |
| `sampling_type` | str | from configuration: data_access.sampling_type | How to select validation sample: `'even'` (evenly spaced) or `'random'`. |
| `seed` | int | from configuration: data_access.seed | RNG seed when `sampling_type: 'random'`. |
| `validate` | bool | `True` | Validate the files loaded as determined by `file_source` |
| `valid_orbital_slots` | list[str] | from configuration: goes.orbital_slots | Which orbital slots should be considered when loading data | 
| `sort` | bool | `True` | Whether to sort the files by timestamp |

#### File source 

The configuration supports several modes for specifying input files.

Suppose there are two .nc files "file1.nc" and "file2.nc" in a directory "path/to" (relative to the current working directory).

To specify both these files:

##### File List

```yaml
data_access:
  file_source:
    - 'path/to/file1.nc'
    - 'path/to/file2.nc'
```

is equivalent to:

```python
obs = GOESMultiCloudObservation(
    file_source = ["path/to/file1.nc", "path/to/file2.nc"],
)
```

##### Directory (recursive or non-recursive)

```yaml
data_access:
  file_source: "path/to/"
  recursive: false
```

is equivalent to:

```python
obs = GOESMultiCloudObservation(
    file_source = "path/to/",
    recursive = False
)
```

If the recursive option is set to true then `file_source` could also simply be `path/` and the `to/` subdirectory would also
be recursively searched for valid .nc files. 

Note that other non-GOES .nc files may exist in these directories, only valid GOES files will be loaded by this class.

##### File as a file list

A list of files can also be specified in a newline delimited list in a file:

```
# contents of files.txt
path/to/file1.nc
path/to/file2.nc
```

```yaml
data_access:
  file_source: files.txt
```

is equivalent to:

```python
obs = GOESMultiCloudObservation(
    file_source = "files.txt",
)
```

##### Catalog

If there exists a catalog previously saved to csv files in the `file/output/catalog/` directory, the files in that catalog can be
specified as the file source:

```yaml
data_access:
  file_source: file/output/catalog/
```

is equivalent to:

```python
obs = GOESMultiCloudObservation(
    file_source = "file/output/catalog/",
)
```

For more information on catalogs see the [documentation](./multicloudcatalog.md)


#### Configuration Validation

The system validates:
- File existence and GOES filename pattern matching (`GOES_FILENAME_PATTERN`)
- Files are sorted by timestamp extracted from filenames
- Sampled files are opened to verify presence of `t` coordinate and `orbital_slot` attribute
- Orbital slot is validated against `VALID_ORBITAL_SLOTS` during preprocessing (per file)

Invalid configurations raise `ConfigError`.

## Lazy vs Eager Operations

Most properties return `xr.DataArray` objects without triggering Dask computation.

**Lazy properties** (no computation triggered):
- `cmi`, `dqf`: Imagery data `(time, y, x)` per band
- All promoted attributes: Metadata variables indexed by `(time,)`
- Coordinates: `time`, `y`, `x`

**Eager properties** (trigger computation):
- `time_range`: Computes min/max from `time_coverage_start`/`time_coverage_end`
- `first_timestamp`, `last_timestamp`: Accesses time coordinate values
- `band_wavelength`, `band_id`: Accesses scalar coordinate values
- `satellite_position`: Accesses scalar variables
- `validate_*` methods: Explicitly compute to check consistency
- `__repr__`: Computes `platform_id` and `orbital_slot` for display

For large-scale processing, use lazy properties and avoid calling eager properties in loops.

## Performance Considerations

### Memory Efficiency
- Uses xarray with Dask for out-of-core processing
- Lazy evaluation preserves memory until `.compute()` is called
- Configurable chunk sizes via `chunk_size` config parameter
- Spatial dimensions should be set to `-1` (full extent) when used with the regridder

### File Validation
- Only a sample of files are validated on init (configurable via `sample_size`)
- Even sampling provides temporal coverage; random sampling with seed for reproducibility
- Full orbital slot validation happens during preprocessing (per file)

### Best Practices
1. Use `get_cmi(band)` / `get_dqf(band)` for direct access in loops rather than the stateful `.band` setter
2. Avoid calling eager properties (`time_range`, `first_timestamp`, `__repr__`) in tight loops
3. Use `isel_time(idx)` for single-timestep extraction
4. Call `.close()` or use context manager when done to release file handles

## Dependencies

- **xarray**: Core data array handling and Dask integration
- **numpy**: Numerical operations
- **pandas**: Time series operations and DataFrame export
- **pathlib**: Cross-platform path handling
- **yaml/json**: Configuration file parsing

## Related Modules

- `multicloudconstants.py`: `PROMOTED_ATTRS`, `VALID_PLATFORMS`, `VALID_ORBITAL_SLOTS`, `VALID_SCENE_IDS`, `GOES_FILENAME_PATTERN`, `DQF_FLAGS`, `DEFAULT_BAND_METADATA`, `REGIONS`, `REFLECTANCE_BANDS`, `BANDS`
- `GOESMetadataCatalog`: Metadata cataloging and file scanning
- `GeostationaryRegridder`: Regridding from geostationary to lat/lon grid
- `GOESZarrStore`: CF-compliant Zarr storage for regridded data