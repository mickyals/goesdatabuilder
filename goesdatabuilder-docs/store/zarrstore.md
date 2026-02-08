# ZarrStoreBuilder

## Overview

The `ZarrStoreBuilder` class provides a configurable, domain-agnostic foundation for building Zarr V3 datasets. It handles the complete lifecycle of Zarr stores including creation, group management, array creation, coordinate setup, and metadata management. This base class is designed to be extended by domain-specific implementations like `GOESZarrStore`.

## Key Features

- **Configuration-Driven**: YAML-based configuration for all aspects of store creation
- **Zarr V3 Support**: Full support for Zarr V3 storage format and features
- **Flexible Storage Backends**: Support for local, zip, memory, and cloud storage
- **Metadata Management**: Comprehensive attribute and metadata handling
- **Group Hierarchy**: Support for nested group structures
- **Array Management**: Efficient creation and manipulation of Zarr arrays
- **Validation**: Built-in configuration validation and error handling

## Architecture

### Design Philosophy

The `ZarrStoreBuilder` follows a **builder pattern** with these principles:

1. **Configuration First**: All store properties defined in YAML configuration
2. **Domain Agnostic**: Base class provides generic Zarr operations
3. **Extensible**: Subclasses add domain-specific semantics and logic
4. **Validation**: Comprehensive validation at initialization and runtime

### Storage Backend Support

**Supported Storage Types:**
- **LocalStore**: File system-based storage
- **ZipStore**: Single-file compressed archives
- **MemoryStore**: In-memory storage for testing and temporary data
- **FsspecStore**: Cloud storage via fsspec (S3, GCS, Azure)
- **ObjectStore**: Generic object storage interface

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

**Configuration Requirements:**
```yaml
# Basic configuration structure
store:
  path: "./data.zarr"
  storage_type: "local"  # local, zip, memory, fsspec, object
  
  # Storage-specific options
  storage_options:
    # Local: none
    # Zip: compression level, etc.
    # Fsspec: cloud credentials, etc.
    
  # Store-level metadata
  attributes:
    title: "Dataset Title"
    description: "Dataset description"
    conventions: "CF-1.13"
    
# Group definitions
groups:
  group_name:
    attributes:
      description: "Group description"
    arrays:
      array_name:
        shape: [time, lat, lon]
        chunks: [1, 512, 512]
        dtype: "float32"
        compressor:
          id: "zstd"
          level: 3
        attributes:
          long_name: "Variable name"
          units: "units"
```

### Constructor Parameters

```python
ZarrStoreBuilder(config_path: str | Path)
```

**Parameters:**
- `config_path`: Path to YAML configuration file

**Raises:**
- `ConfigError`: If configuration file is invalid or missing required sections

## Core Methods

### Store Lifecycle Management

#### Store Creation

```python
# Initialize new store
builder.create_store()

# Create with custom path
builder.create_store(store_path="./custom.zarr")
```

#### Store Opening

```python
# Open existing store
builder = ZarrStoreBuilder.from_existing(
    store_path="./existing.zarr",
    config_path="./config.yaml"
)
```

#### Store Closing

```python
# Close store and flush changes
builder.close()

# Context manager usage
with ZarrStoreBuilder(config_path) as builder:
    # Store operations
    pass  # Automatically closed
```

### Group Management

#### Group Creation

```python
# Create specific group
builder.create_group("observations")

# Create nested groups
builder.create_group("observations/level1/level2")
```

#### Group Access

```python
# Get group object
group = builder.get_group("observations")

# Check group existence
exists = builder.has_group("observations")

# List groups
groups = builder.list_groups()
```

### Array Management

#### Array Creation

```python
# Create specific array
builder.create_array("observations/temperature")

# Create array with custom parameters
builder.create_array(
    path="custom/array",
    shape=(100, 512, 512),
    chunks=(1, 256, 256),
    dtype="float32",
    compressor={"id": "zstd", "level": 5}
)
```

#### Array Access

```python
# Get array object
array = builder.get_array("observations/temperature")

# Check array existence
exists = builder.has_array("observations/temperature")

# List arrays in group
arrays = builder.list_arrays("observations")
```

#### Data Operations

```python
# Write data
builder.write_array(
    path="observations/temperature",
    data=temperature_data,
    selection=slice(0, 10)
)

# Read data
data = builder.read_array(
    path="observations/temperature",
    selection=slice(0, 10)
)

# Append data (along first dimension)
builder.append_array(
    path="observations/temperature",
    data=new_data
)
```

#### Array Resizing and Management

```python
# Resize array to new dimensions
builder.resize_array("observations/temperature", (150, 512, 512))

# Append data with location tracking
location = builder.append_array(
    path="observations/temperature", 
    data=new_data, 
    return_location=True
)
```

### Metadata Management

#### Attribute Operations

```python
# Set store attributes
builder.set_attrs("/", {
    "title": "My Dataset",
    "description": "A sample dataset",
    "history": "Created on 2024-01-01"
})

# Set group attributes
builder.set_attrs("observations", {
    "description": "Observation data"
})

# Set array attributes
builder.set_attrs("observations/temperature", {
    "units": "kelvin", 
    "long_name": "Temperature"
})

# Delete specific attributes
builder.del_attrs("observations/temperature", ["old_attribute"])
```

#### Attribute Access

```python
# Get store attributes
attrs = builder.get_attrs("/")

# Get group attributes
group_attrs = builder.get_attrs("observations")

# Get array attributes
array_attrs = builder.get_attrs("observations/temperature")
```

### Information and Utilities

#### Store Information

```python
# Generate tree view of store hierarchy
print(builder.tree())

# Get basic information about a node
print(builder.info("observations/temperature"))

# Get detailed storage statistics for an array
print(builder.info_complete("observations/temperature"))

# Validate store integrity
result = builder.validate()
if result['valid']:
    print("Store validation passed")
else:
    for issue in result['issues']:
        print(f"Issue: {issue}")
```

## Configuration Schema

### Complete Configuration Example

```yaml
# Store configuration
store:
  path: "./dataset.zarr"
  storage_type: "local"
  storage_options:
    # Storage-specific options
    
  attributes:
    title: "Climate Dataset"
    description: "Sample climate data"
    conventions: "CF-1.13, ACDD-1.3"
    institution: "Example Organization"
    source: "Model simulation"
    history: "Created on 2024-01-01"
    license: "CC-BY-4.0"

# Zarr configuration
zarr:
  zarr_format: 3
  compression:
    default:
      compressor:
        codec: blosc
        cname: zstd
        clevel: 5
        shuffle: bitshuffle
      chunks: auto
      fill_value: NaN
    secondary:
      compressor:
        codec: blosc
        cname: zstd
        clevel: 3
        shuffle: bitshuffle
      chunks: auto
      fill_value: 0

# Group definitions
groups:
  observations:
    attributes:
      description: "Observation data"
      source: "Satellite measurements"
      
    arrays:
      temperature:
        shape: [time, lat, lon]
        chunks: [1, 512, 512]
        dtype: "float32"
        fill_value: "NaN"
        compressor:
          id: "zstd"
          level: 3
        attributes:
          standard_name: "air_temperature"
          long_name: "Air Temperature"
          units: "kelvin"
          coordinates: "time lat lon"
          
      precipitation:
        shape: [time, lat, lon]
        chunks: [1, 512, 512]
        dtype: "float32"
        fill_value: 0.0
        compressor:
          id: "zstd"
          level: 3
        attributes:
          standard_name: "precipitation_flux"
          long_name: "Precipitation"
          units: "kg m-2 s-1"
          coordinates: "time lat lon"

# Coordinate definitions
coordinates:
  time:
    data: ["2024-01-01T00:00:00", "2024-01-01T01:00:00", ...]
    attributes:
      standard_name: "time"
      units: "seconds since 1970-01-01T00:00:00"
      calendar: "gregorian"
      
  lat:
    data: [-90, -89.9, ..., 89.9, 90]
    attributes:
      standard_name: "latitude"
      units: "degrees_north"
      axis: "Y"
      
  lon:
    data: [-180, -179.9, ..., 179.9, 180]
    attributes:
      standard_name: "longitude"
      units: "degrees_east"
      axis: "X"
```

### Storage Backend Configuration

#### Local Storage

```yaml
store:
  storage_type: "local"
  path: "./data.zarr"
  storage_options: {}
```

#### Memory Storage

```yaml
store:
  storage_type: "memory"
  path: "memory"
  storage_options: {}
```

#### Zip Storage

```yaml
store:
  storage_type: "zip"
  path: "./data.zip"
  storage_options:
    compression: 6  # Zip compression level
```

#### Cloud Storage (Fsspec)

```yaml
store:
  storage_type: "fsspec"
  path: "s3://my-bucket/data.zarr"
  storage_options:
    key: "your-access-key"
    secret: "your-secret-key"
    client_kwargs:
      region_name: "us-west-2"
```

#### Object Storage

```yaml
store:
  storage_type: "object"
  backend: "s3"
  bucket: "my-bucket"
  region: "us-west-2"
  anonymous: false
```

### Compression Options

**Supported Compressors:**
```yaml
compressor:
  id: "zstd"        # Zstandard (recommended)
  level: 3          # Compression level (1-22)
  
compressor:
  id: "gzip"        # Gzip compression
  level: 6          # Compression level (1-9)
  
compressor:
  id: "lz4"         # LZ4 compression (fast)
  
compressor:
  id: "blosc"       # Blosc compression
  cname: "zstd"     # Internal compressor
  clevel: 5         # Compression level
  shuffle: "bit"    # Shuffle strategy
```

## Performance Optimization

### Chunking Guidelines

**Optimal Chunk Size:**
- Target: 10-100 MB per chunk
- Consider access patterns
- Balance compression vs. I/O

**Memory Usage:**
- Larger chunks = better compression
- Smaller chunks = faster partial reads
- Consider available memory

### Compression Selection

**Zstandard (zstd):**
- Best overall performance
- Good compression ratio
- Fast compression/decompression

**Gzip:**
- Maximum compatibility
- Good compression ratio
- Slower than zstd

**LZ4:**
- Fastest compression/decompression
- Lower compression ratio
- Good for real-time applications

## Error Handling

### Configuration Validation

```python
try:
    builder = ZarrStoreBuilder(config_path)
except ConfigError as e:
    print(f"Configuration error: {e}")
    # Handle configuration issues
```

### Runtime Errors

```python
try:
    builder.create_array("new/array", data=array_data)
except Exception as e:
    print(f"Array creation failed: {e}")
    # Handle runtime errors
```

### Validation Methods

```python
# Validate configuration
is_valid = builder.validate()

# Validate store integrity
validation_result = builder.validate()
if validation_result['valid']:
    print("Store validation passed")
else:
    for issue in validation_result['issues']:
        print(f"Issue: {issue}")
```

## Integration Examples

### Basic Usage

```python
from goesdatabuilder.store.zarrstore import ZarrStoreBuilder

# Create store from configuration
builder = ZarrStoreBuilder("./config.yaml")

# Initialize store structure
builder.create_store()

# Create groups and arrays
group = builder.create_group("observations")
array = builder.create_array(
    path="observations/temperature",
    shape=(100, 512, 512),
    dtype="float32"
)

# Write data
builder.write_array("observations/temperature", temperature_data)

# Close store
builder.close()
```

### Context Manager Usage

```python
with ZarrStoreBuilder("./config.yaml") as builder:
    builder.create_store()
    builder.create_group("observations")
    builder.create_array("observations/temperature", (100, 512, 512), "float32")
    
    # Write data
    for timestamp, data in data_stream:
        builder.append_array("observations/temperature", data)
```

### Custom Subclass

```python
class MyDatasetBuilder(ZarrStoreBuilder):
    def __init__(self, config_path):
        super().__init__(config_path)
        self._setup_custom_logic()
    
    def _setup_custom_logic(self):
        # Custom initialization
        pass
    
    def process_data(self, input_data):
        # Custom data processing
        processed = self._transform_data(input_data)
        self.write_array("observations/data", processed)
    
    def _transform_data(self, data):
        # Custom transformation logic
        return data * 2.0  # Example transformation
```

## API Reference

### Constructor

```python
ZarrStoreBuilder(config_path: str | Path)
```

### Class Methods

```python
ZarrStoreBuilder.from_existing(store_path: str | Path, config_path: str | Path) -> 'ZarrStoreBuilder'
```

### Store Management

```python
create_store(store_path: Optional[str | Path] = None, overwrite: bool = False) -> None
open_store(store_path: Optional[str | Path] = None, mode: str = "r+") -> None
close() -> None
validate() -> dict
```

### Group Management

```python
create_group(path: str, attrs: dict = None) -> zarr.Group
get_group(path: str) -> zarr.Group
has_group(path: str) -> bool
list_groups(path: str = "/") -> list[str]
```

### Array Management

```python
create_array(path: str, shape: tuple, dtype, chunks: tuple = None, 
             shards: tuple = None, compressor = None, fill_value = None, 
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
store: zarr.Store  # The underlying Zarr store object
root: zarr.Group   # The root group of Zarr store
config: dict        # Deep copy of configuration dictionary
default_compression: dict  # Default compression pipeline configuration
secondary_compression: dict  # Secondary compression pipeline configuration
is_open: bool      # Whether store is currently open and ready for operations
```

### Context Manager

```python
__enter__() -> 'ZarrStoreBuilder'
__exit__(exc_type, exc_val, exc_tb) -> None
```
