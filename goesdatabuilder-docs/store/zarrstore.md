# ZarrStoreBuilder

## Overview

The `ZarrStoreBuilder` class provides a configuration-driven foundation for building and managing Zarr V3 datasets. It handles store lifecycle, group management, array operations, and metadata. This base class is designed to be extended by domain-specific implementations.

### Key Features

- **Configuration-driven** store creation and management
- **Multiple storage backends** (local, memory, cloud, zip)
- **Group and array management** with hierarchical organization
- **Metadata handling** with CF compliance support
- **Chunking and compression** optimization

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
builder.create_store(store_path='./custom.zarr')

# Open existing store
builder.open_store('./existing.zarr')

# Close store
builder.close()

# Context manager
with ZarrStoreBuilder(config_path) as builder:
    # Operations
    pass
```

### Group Operations

```python
# Create groups
group = builder.create_group('observations')
nested = builder.create_group('observations/subgroup')

# Access groups
group = builder.get_group('observations')
exists = builder.has_group('observations')
groups = builder.list_groups()
```

### Array Operations

```python
# Create arrays
array = builder.create_array(
    path='observations/temperature',
    shape=(100, 512, 512),
    chunks=(1, 256, 256),
    dtype='float32'
)

# Access arrays
array = builder.get_array('observations/temperature')
exists = builder.has_array('observations/temperature')
arrays = builder.list_arrays('observations')

# Data operations
builder.write_array('observations/temperature', data)
data = builder.read_array('observations/temperature')
builder.append_array('observations/temperature', new_data)
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
  path: "./dataset.zarr"
  storage_type: "local"  # local, memory, zip, fsspec, object
  storage_options: {}
  attributes:
    title: "Dataset Title"
    description: "Dataset description"
    conventions: "CF-1.13"

# Groups and arrays
groups:
  observations:
    attributes:
      description: "Observation data"
    arrays:
      temperature:
        shape: [time, lat, lon]
        chunks: [1, 512, 512]
        dtype: "float32"
        compressor:
          id: "zstd"
          level: 3
        attributes:
          long_name: "Air Temperature"
          units: "kelvin"
```

### Storage Backend Examples

**Local Storage:**
```yaml
store:
  storage_type: "local"
  path: "./data.zarr"
```

**Memory Storage:**
```yaml
store:
  storage_type: "memory"
  path: "memory"
```

**Cloud Storage:**
```yaml
store:
  storage_type: "fsspec"
  path: "s3://bucket/data.zarr"
  storage_options:
    key: "access-key"
    secret: "secret-key"
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
from_existing(store_path: str | Path, config_path: str | Path) -> 'ZarrStoreBuilder'
```

### Store Management
```python
create_store(store_path: Optional[str | Path] = None, overwrite: bool = False) -> None
open_store(store_path: Optional[str | Path] = None, mode: str = "r+") -> None
close() -> None
validate() -> dict
```

### Group Operations
```python
create_group(path: str, attrs: dict = None) -> zarr.Group
get_group(path: str) -> zarr.Group
has_group(path: str) -> bool
list_groups(path: str = "/") -> list[str]
```

### Array Operations
```python
create_array(path: str, shape: tuple, dtype, chunks: tuple = None, 
             compressor = None, fill_value = None, 
             attrs: dict = None, preset: str = "default") -> zarr.Array
get_array(path: str) -> zarr.Array
has_array(path: str) -> bool
list_arrays(path: str = "/") -> list[str]
resize_array(path: str, new_shape: tuple) -> None
append_array(path: str, data, axis: int = 0, return_location: bool = False) -> tuple[int, int] | None
write_array(path: str, data, selection: tuple = None) -> None
read_array(path: str, selection: tuple = None) -> np.ndarray
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
is_open: bool
```

### Context Manager
```python
__enter__() -> 'ZarrStoreBuilder'
__exit__(exc_type, exc_val, exc_tb) -> None
```
