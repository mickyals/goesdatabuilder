# ZarrStoreBuilder

## Overview

The `ZarrStoreBuilder` class provides a configuration-driven foundation for building and managing Zarr V3 datasets. It handles store lifecycle, group management, array operations, and metadata. This base class is designed to be extended by domain-specific implementations.

### Key Features

- **Configuration-driven** store creation and management
- **Multiple storage backends** (local, memory, cloud, zip, object)
- **Group and array management** with hierarchical organization
- **Metadata handling** with CF compliance support
- **Advanced compression pipelines** with codec configuration
- **Batch hierarchy creation** for efficient setup
- **Context manager support** for automatic resource cleanup

## Architecture

### Storage Backends

Supported storage types:
- **local**: File system storage
- **memory**: In-memory storage (temporary)
- **zip**: Compressed archive storage
- **fsspec**: Cloud storage (S3, GCS, Azure)
- **object**: Generic object storage

### Builder Pattern

Configuration-driven approach:
```
YAML Config → Validation → Store Creation → Operations
```

## Class Structure

### Initialization

```python
from goesdatabuilder.store.zarrstore import ZarrStoreBuilder

# Create new store
builder = ZarrStoreBuilder(config_path='./config.yaml')

# Open existing store
builder = ZarrStoreBuilder.from_existing(
    store_path='./data.zarr',
    config_path='./config.yaml'
)
```

### Constructor

```python
ZarrStoreBuilder(config_path: str | Path)
```

**Parameters:**
- `config_path`: Path to YAML configuration file

**Raises:**
- `ConfigError`: If configuration is invalid

## Core Methods

### Store Management

```python
# Create new store
builder.create_store()
builder.create_store(store_path='./custom.zarr', overwrite=True)

# Create complete hierarchy from specifications
node_specs = {
    "": zarr.GroupMetadata(attributes={"title": "My Dataset"}),
    "observations": zarr.GroupMetadata(attributes={"description": "Data"}),
    "observations/temperature": zarr.ArrayMetadata(
        shape=(100, 512, 512),
        dtype="float32",
        chunks=(1, 256, 256)
    )
}
hierarchy = builder.create_hierarchy(node_specs)

# Open existing store
builder = ZarrStoreBuilder.from_existing(
    store_path='./existing.zarr',
    config_path='./config.yaml'
)

# Close store
builder.close_store()

# Context manager
with ZarrStoreBuilder(config_path) as builder:
    builder.create_store()
    # Operations
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
# Create arrays with compression presets
array = builder.create_array(
    path='observations/temperature',
    shape=(100, 512, 512),
    dtype='float32',
    attrs={'units': 'kelvin', 'long_name': 'Air Temperature'},
    preset='default',  # Uses compression config from YAML
    dimension_names=['time', 'lat', 'lon']
)

# Override compression settings
array = builder.create_array(
    path='observations/pressure',
    shape=(100, 512, 512),
    dtype='float32',
    preset='secondary',
    chunks=(2, 512, 512),  # Override preset chunks
    fill_value=-9999
)

# Access arrays
array = builder.get_array('observations/temperature')
exists = builder.array_exists('observations/temperature')
arrays = builder.array_list('observations')

# Data operations
builder.write_array('observations/temperature', data)
builder.write_array('observations/temperature', data, selection=slice(0, 50))
builder.append_array('observations/temperature', new_data, axis=0, return_location=True)
builder.resize_array('observations/temperature', (150, 512, 512))
```

### Metadata Management

```python
# Set attributes
builder.set_attrs('/', {'title': 'My Dataset'})
builder.set_attrs('observations', {'description': 'Observation data'})
builder.set_attrs('observations/temperature', {'units': 'kelvin'})

# Get attributes
attrs = builder.get_attrs('/')
array_attrs = builder.get_attrs('observations/temperature')

# Delete attributes
builder.del_attrs('observations/temperature', ['old_attr'])
```

### Information and Validation

```python
# Store information
print(builder.tree())  # Hierarchical view
print(builder.info('observations/temperature'))  # Basic info
print(builder.info_complete('observations/temperature'))  # Detailed info

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
  type: "local"  # local, memory, zip, fsspec, object
  path: "./dataset.zarr"
  # For fsspec:
  # storage_options:
  #   key: "access-key"
  #   secret: "secret-key"
  # For object:
  # backend: "s3"  # s3, gcs, azure, memory
  # bucket: "my-bucket"
  # region: "us-west-2"  # for S3
  # container: "my-container"  # for Azure
  # account: "my-account"  # for Azure
  # anonymous: false  # for S3

zarr:
  zarr_format: 3
  
  # Default compression pipeline
  default:
    compressor:
      codec: "numcodecs:Zstd"
      kwargs:
        level: 3
    serializer:
      codec: "numcodecs:VLenUTF8"
    filter:
      codec: "numcodecs:AdaptiveChunkShuffle"
      kwargs:
        elemsize: 4
    chunks: "auto"
    shards: {}
    fill_value: null
    
  # Secondary compression pipeline (optional)
  secondary:
    compressor:
      codec: "numcodecs:Blosc"
      kwargs:
        cname: "zstd"
        clevel: 5
        shuffle: 1
    serializer:
      codec: "numcodecs:VLenUTF8"
    filter:
      codec: "numcodecs:AdaptiveChunkShuffle"
    chunks: [2, 256, 256]
    fill_value: -9999
```

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

- Target 10-100 MB per chunk
- Consider access patterns
- Balance compression vs I/O performance

### Compression Options

- **zstd**: Best overall performance
- **gzip**: Maximum compatibility
- **lz4**: Fastest compression

### Storage Performance

- **Local**: Fastest access
- **Memory**: Fastest but volatile
- **Cloud**: Scalable but network-dependent
- **Zip**: Good compression, portable

## Error Handling

### Configuration Errors

```python
try:
    builder = ZarrStoreBuilder(config_path)
except ConfigError as e:
    print(f'Configuration error: {e}')
```

### Runtime Errors

```python
try:
    builder.create_array('path', data)
except Exception as e:
    print(f'Operation failed: {e}')
```

### Validation

```python
result = builder.validate()
if not result['valid']:
    for issue in result['issues']:
        print(f'Issue: {issue}')
```

## Usage Examples

### Basic Usage

```python
from goesdatabuilder.store.zarrstore import ZarrStoreBuilder

# Create and initialize store
with ZarrStoreBuilder('./config.yaml') as builder:
    builder.create_store()
    
    # Create structure
    builder.create_group('observations')
    builder.create_array(
        'observations/temperature',
        shape=(100, 512, 512),
        dtype='float32'
    )
    
    # Write data
    builder.write_array('observations/temperature', data)
```

### Custom Subclass

```python
class MyDataBuilder(ZarrStoreBuilder):
    def __init__(self, config_path):
        super().__init__(config_path)
        self.setup_custom_logic()
    
    def process_data(self, data):
        processed = self.transform_data(data)
        self.write_array('data/processed', processed)
    
    def transform_data(self, data):
        return data * 2.0
```

## API Reference

### Constructor
```python
ZarrStoreBuilder(config_path: str | Path)
```

### Class Methods
```python
from_existing(store_path: str | Path, config_path: str | Path, mode: str = "r+") -> 'ZarrStoreBuilder'
```

### Store Management
```python
create_store(store_path: Optional[str | Path] = None, overwrite: bool = False) -> None
create_hierarchy(node_specs: dict, store_path: Optional[str | Path] = None, overwrite: bool = False) -> dict
close_store() -> None
validate() -> dict[str, bool | list[str]]
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

### Metadata Management
```python
get_attrs(path: str = "/") -> dict
set_attrs(path: str, attrs: dict, merge: bool = True) -> None
del_attrs(path: str, keys: list[str]) -> None
```

### Information and Utilities
```python
tree(path: str = "/") -> str
info(path: str = "/") -> str
info_complete(path: str) -> str
```

### Properties
```python
store: zarr.Store
root: zarr.Group
config: dict
default_compression: dict
secondary_compression: dict
is_open: bool
```

### Context Manager
```python
__enter__() -> 'ZarrStoreBuilder'
__exit__(exc_type, exc_val, exc_tb) -> None
```
