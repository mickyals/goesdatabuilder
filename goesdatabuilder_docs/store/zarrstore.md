# ZarrStoreBuilder

## Overview

The `ZarrStoreBuilder` class provides a configuration-driven foundation for building and managing Zarr V3 datasets. It handles store lifecycle, group management, array operations, and metadata. This base class is designed to be extended by domain-specific implementations.

### Key Features

- **Configuration-driven** store creation and management
- **Multiple storage backends** (local, memory, cloud, zip, object)
- **Group and array management** with hierarchical organization
- **Metadata handling** with CF compliance support
- **Flexible array pipelines** with user-defined compression presets
- **Batch hierarchy creation** via `zarr.create_hierarchy`
- **Context manager support** for automatic resource cleanup

## Architecture

### Storage Backends

Supported storage types:
- **local**: File system storage via `LocalStore`
- **memory**: In-memory storage via `MemoryStore` (temporary)
- **zip**: Compressed archive storage via `ZipStore`
- **fsspec**: Cloud storage via `FsspecStore` (S3, GCS, Azure)
- **object**: Generic object storage via `ObjectStore` (experimental)

### Builder Pattern

```
YAML/JSON Config -> Validation -> Store Creation -> Operations
```

## Class Structure

### Initialization

```python
from goesdatabuilder.store.zarrstore import ZarrStoreBuilder

# Create new store
builder = ZarrStoreBuilder(config_path='./config.yaml')
builder.create_store(store_path='./data.zarr')

# Open existing store
builder = ZarrStoreBuilder.from_existing(
    store_path='./data.zarr',
    config_path='./config.yaml',
    mode='r+'
)
```

### Constructor

```python
ZarrStoreBuilder(config_path: str | Path)
```

**Parameters:**
- `config_path`: Path to YAML or JSON configuration file

**Raises:**
- `ConfigError`: If configuration is invalid
- `FileNotFoundError`: If configuration file does not exist

## Core Methods

### Store Management

```python
# Create new store
builder.create_store()
builder.create_store(store_path='./custom.zarr', overwrite=True)

# Create complete hierarchy from group specifications
from zarr.core.group import GroupMetadata

node_specs = {
    "": GroupMetadata(attributes={"Conventions": "CF-1.8", "title": "My Dataset"}),
    "observations": GroupMetadata(attributes={"description": "Observation data"}),
    "observations/temperature": GroupMetadata(attributes={"variable": "temperature"}),
}
hierarchy = builder.create_hierarchy(node_specs, store_path='./data.zarr')
# Arrays are created separately via create_array once dimensions are known

# Open existing store (class method)
builder = ZarrStoreBuilder.from_existing(
    store_path='./existing.zarr',
    config_path='./config.yaml',
    mode='r+'  # or 'r' for read-only
)

# Close store
builder.close_store()

# Context manager
with ZarrStoreBuilder(config_path) as builder:
    builder.create_store()
    # ... operations ...
    pass  # Automatically closed on exit
```

### Group Operations

```python
# Create groups
group = builder.create_group('observations')
nested = builder.create_group('observations/subgroup', attrs={'description': 'Nested group'})

# Access groups
group = builder.get_group('observations')
exists = builder.group_exists('observations')
groups = builder.list_groups()
subgroups = builder.list_groups('observations')
```

### Array Operations

```python
# Create arrays using pipeline presets from config
array = builder.create_array(
    path='observations/temperature',
    shape=(0, 512, 512),
    dtype='float32',
    attrs={'units': 'kelvin', 'long_name': 'Air Temperature'},
    preset='default',
    dimension_names=['time', 'lat', 'lon']
)

# Override preset settings via **overrides
array = builder.create_array(
    path='observations/pressure',
    shape=(0, 512, 512),
    dtype='float32',
    preset='secondary',
    chunks=(2, 512, 512),
    fill_value=-9999
)

# Inspect available pipeline presets
pipelines = builder.array_pipelines
print(f"Available presets: {list(pipelines.keys())}")

# Access arrays
array = builder.get_array('observations/temperature')
exists = builder.array_exists('observations/temperature')
arrays = builder.array_list('observations')

# Data operations
builder.write_array('observations/temperature', data)
builder.write_array('observations/temperature', data, selection=(slice(0, 50),))
start, end = builder.append_array('observations/temperature', new_data, axis=0, return_location=True)
builder.resize_array('observations/temperature', (150, 512, 512))
```

### Metadata Management

```python
# Set attributes (merge by default)
builder.set_attrs('/', {'title': 'My Dataset'})
builder.set_attrs('observations', {'description': 'Observation data'})

# Replace all attributes
builder.set_attrs('observations/temperature', {'units': 'kelvin'}, merge=False)

# Get attributes
attrs = builder.get_attrs('/')
array_attrs = builder.get_attrs('observations/temperature')

# Delete attributes
builder.del_attrs('observations/temperature', ['old_attr'])
```

### Information and Validation

```python
# Store information
print(builder.tree())
print(builder.info('observations/temperature'))
print(builder.info_complete('observations/temperature'))

# Validation
result = builder.validate()
if result['valid']:
    print('Store is valid')
else:
    for issue in result['issues']:
        print(f'Issue: {issue}')
```

## Configuration Schema

### Basic Structure

```yaml
store:
  type: "local"
  path: "./dataset.zarr"

zarr:
  zarr_format: 3

  # Default pipeline for primary data arrays
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

  # Secondary pipeline for coordinate/auxiliary arrays
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

  # Additional custom pipelines (optional, any name)
  high_compression:
    compressor:
      codec: 'zarr.codecs:GzipCodec'
      kwargs:
        level: 9
    serializer:
      codec: null
    filter:
      codec: null
    chunks: auto
    fill_value: -9999
```

### Codec Specification Format

Codecs are specified as `'module:ClassName'` strings with optional kwargs:

```yaml
compressor:
  codec: 'zarr.codecs:BloscCodec'
  kwargs:
    cname: zstd
    clevel: 5
    shuffle: bitshuffle
```

Setting `codec: null` disables that pipeline stage. Omitting the key entirely defaults to `"auto"`.

### Pipeline Stages

Each array pipeline has three codec stages applied in order:

1. **filter** (Array to Array): Pre-compression transforms (e.g., delta encoding, shuffle)
2. **serializer** (Array to Bytes): Serialization codec
3. **compressor** (Bytes to Bytes): Compression codec (e.g., Blosc, Gzip, Zstd)

Plus array creation parameters: `chunks`, `shards`, `fill_value`.

### Storage Backend Examples

**Local Storage:**
```yaml
store:
  type: "local"
  path: "./data.zarr"
```

**Memory Storage:**
```yaml
store:
  type: "memory"
```

**Cloud Storage (fsspec):**
```yaml
store:
  type: "fsspec"
  path: "s3://bucket/data.zarr"
  storage_options:
    key: "access-key"
    secret: "secret-key"
```

**Object Storage (obstore):**
```yaml
store:
  type: "object"
  backend: "s3"
  bucket: "my-bucket"
  region: "us-west-2"
  anonymous: false
```

**Zip Storage:**
```yaml
store:
  type: "zip"
  path: "./data.zip"
```

## Performance Considerations

### Chunking Guidelines

- Target 10-100 MB per chunk for data arrays
- Use larger chunks (512+) for small-element coordinate arrays
- Consider access patterns (time-series vs spatial slicing)
- Time dimension chunk of 1 is common for observation-at-a-time append workflows

### Compression Options

- **BloscCodec (zstd)**: Best overall balance of speed and ratio
- **GzipCodec**: Maximum compatibility
- **BloscCodec (lz4)**: Fastest compression/decompression
- **BloscCodec (bitshuffle)**: Good for floating point scientific data

### Storage Performance

- **Local**: Fastest access, best for development and HPC
- **Memory**: Fastest but volatile, good for testing
- **Cloud (fsspec)**: Scalable, network-dependent, consolidate metadata for performance
- **Zip**: Portable, good for archival, limited append support

## Error Handling

### Configuration Errors

```python
try:
    builder = ZarrStoreBuilder(config_path)
except ConfigError as e:
    print(f'Configuration error: {e}')
except FileNotFoundError as e:
    print(f'Config file not found: {e}')
```

### Store Lifecycle Errors

```python
try:
    builder.create_store(store_path='./data.zarr')
except FileExistsError:
    print('Store already exists, use overwrite=True')
```

### Runtime Errors

```python
# Operations on closed stores raise RuntimeError
try:
    builder.create_array('path', shape=(10,), dtype='float32')
except RuntimeError as e:
    print(f'Store not open: {e}')

# Missing nodes raise KeyError
try:
    builder.get_array('nonexistent/path')
except KeyError as e:
    print(f'Not found: {e}')
```

## Usage Examples

### Basic Usage

```python
from goesdatabuilder.store.zarrstore import ZarrStoreBuilder

with ZarrStoreBuilder('./config.yaml') as builder:
    builder.create_store(store_path='./output.zarr')

    builder.create_group('observations')
    builder.create_array(
        'observations/temperature',
        shape=(0, 512, 512),
        dtype='float32',
        dimension_names=['time', 'lat', 'lon'],
    )

    builder.append_array('observations/temperature', data, axis=0)
```

### Batch Hierarchy Creation

```python
from zarr.core.group import GroupMetadata

with ZarrStoreBuilder('./config.yaml') as builder:
    specs = {
        "": GroupMetadata(attributes={"Conventions": "CF-1.8"}),
        "GOES-East": GroupMetadata(attributes={"platform_ID": "G16"}),
        "GOES-East/CMI_C01": GroupMetadata(attributes={"band": 1}),
        "GOES-West": GroupMetadata(attributes={"platform_ID": "G18"}),
        "GOES-West/CMI_C01": GroupMetadata(attributes={"band": 1}),
    }
    created = builder.create_hierarchy(specs, store_path='./goes.zarr')
    # Arrays added later once grid dimensions are known
```

### Domain-Specific Subclass

```python
class GOESZarrDatasetBuilder(ZarrStoreBuilder):
    def __init__(self, config_path):
        super().__init__(config_path)

    def initialize_platform(self, platform, lat, lon):
        """Set up coordinate arrays for a platform group."""
        self.create_array(
            f"{platform}/lat", shape=lat.shape, dtype='float64',
            dimension_names=['lat'], preset='secondary',
            attrs={'standard_name': 'latitude', 'units': 'degrees_north'},
        )
        self.write_array(f"{platform}/lat", lat)
        # ... lon, time, CMI bands, DQF arrays ...
```

## API Reference

### Constructor
```python
ZarrStoreBuilder(config_path: str | Path)
```

### Class Methods
```python
from_existing(store_path: str | Path, config_path: str | Path, mode: str = "r+") -> ZarrStoreBuilder
```

### Store Lifecycle
```python
create_store(store_path: str | Path = None, overwrite: bool = False) -> None
create_hierarchy(node_specs: dict, store_path: str | Path = None, overwrite: bool = False) -> dict
close_store() -> None
```

### Group Operations
```python
create_group(path: str, attrs: dict = None) -> zarr.Group
get_group(path: str) -> zarr.Group
group_exists(path: str) -> bool
list_groups(path: str = "/") -> list[str]
```

### Array Operations
```python
create_array(path: str, shape: tuple, dtype, attrs: dict = None, preset: str = "default",
             dimension_names: list = None, **overrides) -> zarr.Array
get_array(path: str) -> zarr.Array
array_exists(path: str) -> bool
array_list(path: str = "/") -> list[str]
resize_array(path: str, new_shape: tuple) -> None
append_array(path: str, data, axis: int = 0, return_location: bool = False) -> tuple[int, int] | None
write_array(path: str, data, selection: tuple = None) -> None
```

### Metadata Operations
```python
get_attrs(path: str = "/") -> dict
set_attrs(path: str, attrs: dict, merge: bool = True) -> None
del_attrs(path: str, keys: list[str]) -> None
```

### Information and Validation
```python
tree(path: str = "/") -> str
info(path: str = "/") -> str
info_complete(path: str) -> str
validate() -> dict[str, bool | list[str]]
```

### Properties
```
store -> zarr.Store              # Underlying store instance
root -> zarr.Group               # Root group of the store
config -> dict                   # Deep copy of configuration
array_pipelines -> dict          # All available pipeline presets
is_open -> bool                  # Whether store is initialized
store_path -> Optional[Path]     # Path to store if initialized, None otherwise
```

**Add note about `create_hierarchy`:**

After the `create_hierarchy` method description:
```
Note: `create_hierarchy` uses `zarr.create_hierarchy` which takes `GroupMetadata`/`ArrayMetadata` specs.
It does not resolve array pipeline presets. Arrays requiring preset-based codec resolution should be
created separately via `create_array`. *Batch array creation with preset support is under consideration.*
```

**Add note about `_resolve_store` env var handling** in the Store Management section or Configuration Schema:
```
Store path resolution is centralized in `_resolve_store`. This method:
- Falls back to `config['store']['path']` if no path argument is provided
- Expands environment variables in override paths (config paths are already expanded at load time by `_load_config`)
- Converts to `Path` for local and zip stores, preserves strings for fsspec and object stores
- Handles overwrite logic (shutil.rmtree for local, existence check for zip)

### Context Manager
```python
__enter__() -> ZarrStoreBuilder
__exit__(exc_type, exc_val, exc_tb) -> None
```