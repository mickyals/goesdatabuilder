# Configuration Files

## Overview

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

- [data access](#data_access): access GOES input files
- [regridding](#regridding): regrid GOES input files to lat/lon
- [store](#store): save the regridded data to a zarr store
- [goes](#goes): GOES metadata and orbital slot, and band selection
- [pipeline](#pipeline): data pipeline orchestration

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

Settings used to configure the zarr store: 

```yaml
store:
  type: local # Storage backend type: local, zip, fsspec, memory, object
  path: null  # path on disk to use when the type is local or zip (or fsspec if fsspec is used to refer to a local store)
  object_store_backend: null
  storage_options: {} # additional keyword options to pass on to the store class
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

Note that the "path" value can be overridden using the `GOES_STORE_DIR` [environment variable](#environment-variables).

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

The values for each band contains:
- `wavelength`: Central wavelength in micrometers
- `long_name`: Descriptive name following GOES ABI conventions
- `standard_name`: CF standard name
- `units`: Physical units ('1' for reflectance, 'K' for temperature)
- `valid_range`: Expected data range as [min, max]

| Band | Wavelength (um) | Name | Type |
|------|----------------|------|------|
| 1 | 0.47 | Blue | Reflectance |
| 2 | 0.64 | Red | Reflectance |
| 3 | 0.86 | Veggie | Reflectance |
| 4 | 1.37 | Cirrus | Reflectance |
| 5 | 1.61 | Snow/Ice | Reflectance |
| 6 | 2.24 | Cloud Particle Size | Reflectance |
| 7 | 3.90 | Shortwave Window | Brightness Temp |
| 8 | 6.19 | Upper-Level Water Vapor | Brightness Temp |
| 9 | 6.93 | Mid-Level Water Vapor | Brightness Temp |
| 10 | 7.34 | Lower-Level Water Vapor | Brightness Temp |
| 11 | 8.44 | Cloud-Top Phase | Brightness Temp |
| 12 | 9.61 | Ozone | Brightness Temp |
| 13 | 10.33 | Clean Longwave Window | Brightness Temp |
| 14 | 11.21 | Longwave Window | Brightness Temp |
| 15 | 12.29 | Dirty Longwave Window | Brightness Temp |
| 16 | 13.28 | CO2 Longwave | Brightness Temp |

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
  worker_threads: 1 # number of threads used to simultaneously regrid bands (maximum is the number of bands)

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

## Troubleshooting

### ConfigError

After updating configuration settings if a `ConfigError` is raised, this indicates that the configuration settings are misconfigured
in some way. Configurations are validated using a JSON schema and if the configuration is invalid according to the schema an error
will be raised. Please check the error message which should indicate how the configuration is invalid.

For example:

```python
from goesdatabuilder import set_config
set_config(config_dict={"data_access": {"recursive": "maybe"}})
```

will raise:

```
goesdatabuilder.utils.config.ConfigError: Invalid Configuration: 'maybe' is not of type 'boolean'

Failed validating 'type' in schema['properties']['data_access']['properties']['recursive']:
    {'type': 'boolean'}

On instance['data_access']['recursive']:
    'maybe'
```

#### ConfigErrors at runtime

Certain `ConfigError`s may only be raised at runtime:

- no GOES files found:
  - indicates that the input files specified in `data_access.file_source` do not exist or are not valid GOES .nc files
  - solution: double check the file source and the .nc files that it refers to
- file not found:
  - indicates that a file specified in `data_access.file_source` cannot be found
  - solution: check the error message which should indicate which file cannot be found, update the path to that file 
    in the data source or remove that file path from the data source if it's not supposed to be there.
- missing 't' coordinate:
  - indicates that a given .nc file does not have a 't' (time) coordinate
  - solution: double check that the file contains valid GOES data. If only the t coordinate is missing, update the .nc file,
    otherwise the file may be corrupted.
- orbital slot mismatch:
  - indicates that the input files cover multiple orbital slots. Only data from a single orbital slot can be processed at a time.
  - solution: update `data_access.file_source` to only refer to files from one orbital slot at a time. If the file source refers
    to a catalog instance you can also set `catalog.orbital_slot` and that will only load files with the given orbital slot from
    the catalog
- others:
  - other `ConfigError`s should contain a descriptive error message that should give a hint as to how to resolve it.
  - if an error message is unclear please make an [issue](https://github.com:mickyals/goesdatabuilder/issues/new)

### Invalid JSON or Yaml Syntax

If the configuration files contain invalid JSON or Yaml an error message, usually a `json.JSONDecodeError` or `yaml.ScannerError`.
In that case please check the syntax of the configuration files and try again.
