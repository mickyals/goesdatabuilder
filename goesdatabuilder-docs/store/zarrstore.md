# ZarrDatasetBuilder Class Documentation

The `ZarrDatasetBuilder` class provides a config-driven, high-level interface for creating and managing Zarr V3 datasets. It handles the complete lifecycle of Zarr stores including creation, opening, group/array management, metadata handling, and storage operations. This class is designed to be domain-agnostic, with subclasses adding semantic meaning for specific data types.

## Installation

Make sure you have the required dependencies installed:

```bash
pip install zarr>=3.0.0 numpy>=1.24.0 yaml>=6.0
```

## Quick Start

```python
from goesdatabuilder.store import ZarrDatasetBuilder

# Create a new Zarr store with configuration
builder = ZarrDatasetBuilder('config.yaml')
builder.create_store('/path/to/dataset.zarr')

# Create groups and arrays
group = builder.create_group('satellite/goes18')
array = builder.create_array(
    path='satellite/goes18/radiance',
    shape=(1000, 512, 512),
    dtype='float32',
    attrs={'units': 'W m^-2 sr^-1 μm^-1'}
)

# Write data
data = np.random.random((1000, 512, 512)).astype('float32')
builder.write_array('satellite/goes18/radiance', data)

# Use context manager for automatic cleanup
with ZarrDatasetBuilder('config.yaml') as builder:
    builder.create_store('/path/to/dataset.zarr')
    # ... operations ...
# Store automatically closed
```

---

## Class Overview

### Constructor

```python
ZarrDatasetBuilder(config_path: str | Path)
```

**Parameters:**
- `config_path` (str | Path): Path to the YAML or JSON configuration file

**What it does:**
- Loads and validates the configuration file
- Expands environment variables in configuration values
- Initializes internal state for store management
- Validates required configuration keys and structure

**Configuration Format:**
```yaml
store:
  type: local  # local, memory, zip, fsspec, object
  path: $HOME/data/dataset.zarr

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
```

---

## Alternative Constructor

### `from_existing`

```python
@classmethod
from_existing(cls, store_path: str | Path, config_path: str | Path) -> ZarrDatasetBuilder
```

**Description:** Opens an existing Zarr store with the given configuration

**Parameters:**
- `store_path` (str | Path): Path to the existing Zarr store
- `config_path` (str | Path): Path to the configuration file

**Returns:** Configured `ZarrDatasetBuilder` instance with opened store

**Usage:**
```python
# Open existing store
builder = ZarrDatasetBuilder.from_existing(
    '/existing/dataset.zarr', 
    'config.yaml'
)

# Store is ready for read/write operations
array = builder.get_array('satellite/radiance')
```

---

## Core Properties

### `store`

**Type:** Property (read-only)

**Description:** The underlying Zarr store object

**Returns:** Zarr store instance (LocalStore, MemoryStore, etc.)

**Usage:**
```python
store = builder.store
print(f"Store type: {type(store).__name__}")
```

---

### `root`

**Type:** Property (read-only)

**Description:** The root group of the Zarr store

**Returns:** `zarr.Group` object representing the root

**Usage:**
```python
root = builder.root
print(f"Root path: {root.path}")
print(f"Groups: {list(root.groups())}")
```

---

### `config`

**Type:** Property (read-only)

**Description:** Deep copy of the configuration dictionary

**Returns:** Dictionary containing the complete configuration

**Usage:**
```python
config = builder.config
print(f"Store type: {config['store']['type']}")
print(f"Zarr format: {config['zarr']['zarr_format']}")
```

---

### `default_compression`

**Type:** Property (read-only)

**Description:** Default compression pipeline configuration

**Returns:** Dictionary with compression settings for default preset

**Usage:**
```python
compression = builder.default_compression
print(f"Codec: {compression['compressor']['codec']}")
print(f"Level: {compression['compressor']['clevel']}")
```

---

### `secondary_compression`

**Type:** Property (read-only)

**Description:** Secondary compression pipeline configuration

**Returns:** Dictionary with compression settings for secondary preset

**Usage:**
```python
compression = builder.secondary_compression
print(f"Codec: {compression['compressor']['codec']}")
```

---

### `is_open`

**Type:** Property (read-only)

**Description:** Whether the store is currently open and ready for operations

**Returns:** Boolean indicating store state

**Usage:**
```python
if builder.is_open:
    print("Store is ready for operations")
else:
    print("Store is closed")
```

---

## Store Lifecycle Methods

### `create_store`

```python
create_store(store_path: str | Path = None, overwrite: bool = False)
```

**Description:** Creates a new Zarr store based on configuration

**Parameters:**
- `store_path` (str | Path, optional): Override path from config
- `overwrite` (bool): Whether to overwrite existing store (default: False)

**Raises:**
- `ConfigError`: If store type is invalid
- `ValueError`: If required path is missing
- `FileExistsError`: If store exists and overwrite=False

**Store Types:**
- **local**: Filesystem-based store
- **memory**: In-memory store (no path needed)
- **zip**: Compressed zip file store
- **fsspec**: Remote storage (S3, GCS, etc.)
- **object**: Object storage backend

**Usage:**
```python
# Create with config path
builder.create_store()

# Create with custom path
builder.create_store('/custom/path/dataset.zarr')

# Overwrite existing store
builder.create_store(overwrite=True)
```

---

### `open_store`

```python
open_store(store_path: str | Path = None, mode: str = "r+")
```

**Description:** Opens an existing Zarr store for read/write operations

**Parameters:**
- `store_path` (str | Path, optional): Override path from config
- `mode` (str): Open mode - "r" (read-only) or "r+" (read-write)

**Raises:**
- `ValueError`: If store type doesn't support opening
- `FileNotFoundError`: If store doesn't exist

**Usage:**
```python
# Open for read-write
builder.open_store('/existing/dataset.zarr')

# Open read-only
builder.open_store('/existing/dataset.zarr', mode="r")
```

---

### `close_store`

```python
close_store()
```

**Description:** Closes the store and releases system resources

**Usage:**
```python
builder.close_store()
print(f"Store open: {builder.is_open}")  # False
```

---

### Context Manager Support

```python
# Automatic cleanup with context manager
with ZarrDatasetBuilder('config.yaml') as builder:
    builder.create_store('/data/dataset.zarr')
    # Perform operations
# Store automatically closed when exiting context
```

---

## Group Management Methods

### `create_group`

```python
create_group(path: str, attrs: dict = None) -> zarr.Group
```

**Description:** Creates a new group in the store

**Parameters:**
- `path` (str): Path for the new group
- `attrs` (dict, optional): Group attributes

**Returns:** The newly created `zarr.Group`

**Raises:**
- `RuntimeError`: If store is not open
- `ValueError`: If group already exists

**Usage:**
```python
# Create simple group
group = builder.create_group('satellite')

# Create group with attributes
attrs = {
    'platform': 'GOES-18',
    'instrument': 'ABI',
    'created': '2024-01-01'
}
group = builder.create_group('satellite/goes18', attrs)

# Nested groups automatically created
group = builder.create_group('satellite/goes18/bands')
```

---

### `get_group`

```python
get_group(path: str) -> zarr.Group
```

**Description:** Retrieves an existing group from the store

**Parameters:**
- `path` (str): Path to the group

**Returns:** The `zarr.Group` at the specified path

**Raises:**
- `RuntimeError`: If store is not open
- `KeyError`: If group doesn't exist or path is not a group

**Usage:**
```python
group = builder.get_group('satellite/goes18')
print(f"Group path: {group.path}")
print(f"Attributes: {dict(group.attrs)}")
```

---

### `group_exists`

```python
group_exists(path: str) -> bool
```

**Description:** Checks if a group exists at the specified path

**Parameters:**
- `path` (str): Path to check

**Returns:** True if group exists, False otherwise

**Raises:**
- `RuntimeError`: If store is not open

**Usage:**
```python
if builder.group_exists('satellite/goes18'):
    group = builder.get_group('satellite/goes18')
else:
    print("Group does not exist")
```

---

### `list_groups`

```python
list_groups(path: str = "/") -> list[str]
```

**Description:** Lists all groups within a parent group

**Parameters:**
- `path` (str): Parent group path (default: "/")

**Returns:** List of group names

**Raises:**
- `RuntimeError`: If store is not open

**Usage:**
```python
# List root groups
root_groups = builder.list_groups()
print(f"Root groups: {root_groups}")

# List groups in specific path
satellite_groups = builder.list_groups('satellite')
print(f"Satellite groups: {satellite_groups}")
```

---

## Array Management Methods

### `create_array`

```python
create_array(path: str, shape: tuple, dtype, chunks: tuple = None, 
             shards: tuple = None, compressor=None, fill_value=None, 
             attrs: dict = None, preset: str = "default") -> zarr.Array
```

**Description:** Creates a new array in the store

**Parameters:**
- `path` (str): Array path
- `shape` (tuple): Array dimensions
- `dtype`: NumPy data type
- `chunks` (tuple, optional): Chunk size (overrides config)
- `shards` (tuple, optional): Shard size (overrides config)
- `compressor`: Custom compressor (overrides config)
- `fill_value`: Fill value for empty chunks
- `attrs` (dict, optional): Array attributes
- `preset` (str): Compression preset ("default" or "secondary")

**Returns:** The newly created `zarr.Array`

**Raises:**
- `RuntimeError`: If store is not open
- `ValueError`: If array already exists

**Usage:**
```python
# Simple array
array = builder.create_array(
    path='temperature',
    shape=(100, 512, 512),
    dtype='float32'
)

# Array with custom settings
array = builder.create_array(
    path='satellite/goes18/radiance',
    shape=(1000, 512, 512),
    dtype='float32',
    chunks=(100, 256, 256),
    attrs={
        'units': 'W m^-2 sr^-1 μm^-1',
        'long_name': 'Channel 8 Radiance',
        'band': 8
    }
)
```

---

### `get_array`

```python
get_array(path: str) -> zarr.Array
```

**Description:** Retrieves an existing array from the store

**Parameters:**
- `path` (str): Array path

**Returns:** The `zarr.Array` at the specified path

**Raises:**
- `RuntimeError`: If store is not open
- `KeyError`: If array doesn't exist or path is not an array

**Usage:**
```python
array = builder.get_array('satellite/goes18/radiance')
print(f"Shape: {array.shape}")
print(f"Dtype: {array.dtype}")
print(f"Attributes: {dict(array.attrs)}")
```

---

### `array_exists`

```python
array_exists(path: str) -> bool
```

**Description:** Checks if an array exists at the specified path

**Parameters:**
- `path` (str): Array path to check

**Returns:** True if array exists, False otherwise

**Raises:**
- `RuntimeError`: If store is not open

**Usage:**
```python
if builder.array_exists('satellite/goes18/radiance'):
    array = builder.get_array('satellite/goes18/radiance')
else:
    print("Array does not exist")
```

---

### `array_list`

```python
array_list(path: str = "/") -> list[str]
```

**Description:** Lists all arrays within a parent group

**Parameters:**
- `path` (str): Parent group path (default: "/")

**Returns:** List of array names

**Raises:**
- `RuntimeError`: If store is not open

**Usage:**
```python
# List all arrays in root
arrays = builder.array_list()
print(f"All arrays: {arrays}")

# List arrays in specific group
satellite_arrays = builder.array_list('satellite/goes18')
print(f"GOES-18 arrays: {satellite_arrays}")
```

---

### `resize_array`

```python
resize_array(path: str, new_shape: tuple)
```

**Description:** Resizes an existing array to new dimensions

**Parameters:**
- `path` (str): Array path
- `new_shape` (tuple): New array dimensions

**Raises:**
- `RuntimeError`: If store is not open
- `KeyError`: If array doesn't exist

**Usage:**
```python
# Extend time dimension
builder.resize_array('temperature', (150, 512, 512))

# Resize all dimensions
builder.resize_array('data', (200, 256, 256))
```

---

### `append_array`

```python
append_array(path: str, data, axis: int = 0, return_location: bool = False) -> tuple[int, int] | None
```

**Description:** Dynamically grows an array and appends data along specified axis

**Parameters:**
- `path` (str): Array path
- `data`: Data to append (NumPy array)
- `axis` (int): Axis along which to append (default: 0)
- `return_location` (bool): Whether to return write indices

**Returns:** Tuple of (start_idx, end_idx) if return_location=True, otherwise None

**Raises:**
- `RuntimeError`: If store is not open
- `KeyError`: If array doesn't exist

**Usage:**
```python
# Append new time slice
new_data = np.random.random((10, 512, 512)).astype('float32')
location = builder.append_array('temperature', new_data, return_location=True)
print(f"Data written to indices {location}")

# Append along different axis
new_bands = np.random.random((100, 2, 512)).astype('float32')
builder.append_array('radiance', new_bands, axis=1)
```

---

### `write_array`

```python
write_array(path: str, data, selection: tuple = None)
```

**Description:** Writes data to an array (entire array or specific region)

**Parameters:**
- `path` (str): Array path
- `data`: Data to write
- `selection` (tuple, optional): Region to write (slicing syntax)

**Raises:**
- `RuntimeError`: If store is not open
- `KeyError`: If array doesn't exist

**Usage:**
```python
# Write entire array
data = np.random.random((100, 512, 512)).astype('float32')
builder.write_array('temperature', data)

# Write to specific region
subset = np.random.random((10, 256, 256)).astype('float32')
builder.write_array('temperature', subset, selection=(slice(0,10), slice(0,256), slice(0,256)))

# Write single time slice
time_slice = np.random.random((512, 512)).astype('float32')
builder.write_array('temperature', time_slice, selection=(50, :, :))
```

---

## Coordinate Convenience Methods

### `create_coordinate`

```python
create_coordinate(path: str, data, attrs: dict = None, 
                  chunks: tuple = None, shards: tuple = None, 
                  preset: str = "secondary") -> zarr.Array
```

**Description:** Creates a 1D coordinate array and immediately writes data

**Parameters:**
- `path` (str): Coordinate path
- `data`: 1D coordinate data
- `attrs` (dict, optional): Coordinate attributes
- `chunks` (tuple, optional): Chunk size
- `shards` (tuple, optional): Shard size
- `preset` (str): Compression preset (default: "secondary")

**Returns:** The created coordinate array

**Raises:**
- `ValueError`: If data is not 1D

**Usage:**
```python
# Time coordinate
time_coords = np.arange(100, dtype='datetime64[s]')
time_coord = builder.create_coordinate(
    path='time',
    data=time_coords,
    attrs={'units': 'seconds since 1970-01-01', 'calendar': 'gregorian'}
)

# Spatial coordinate
lats = np.linspace(-90, 90, 180)
lat_coord = builder.create_coordinate(
    path='latitude',
    data=lats,
    attrs={'units': 'degrees_north', 'axis': 'Y'}
)
```

---

### `create_empty_coordinate`

```python
create_empty_coordinate(path: str, dtype, attrs: dict = None,
                       chunks: tuple = None, shards: tuple = None,
                       preset: str = "secondary") -> zarr.Array
```

**Description:** Creates a zero-length 1D coordinate array for growing data

**Parameters:**
- `path` (str): Coordinate path
- `dtype`: NumPy data type
- `attrs` (dict, optional): Coordinate attributes
- `chunks` (tuple, optional): Chunk size
- `shards` (tuple, optional): Shard size
- `preset` (str): Compression preset

**Returns:** The created empty coordinate array

**Usage:**
```python
# Create empty time coordinate for streaming data
time_coord = builder.create_empty_coordinate(
    path='time',
    dtype='datetime64[s]',
    attrs={'units': 'seconds since 1970-01-01'}
)

# Later append time values
new_times = np.arange(10, 20, dtype='datetime64[s]')
builder.append_array('time', new_times)
```

---

## Metadata Management Methods

### `get_attrs`

```python
get_attrs(path: str = "/") -> dict
```

**Description:** Gets attributes from a node (group or array)

**Parameters:**
- `path` (str): Node path (default: "/")

**Returns:** Dictionary of node attributes

**Raises:**
- `RuntimeError`: If store is not open

**Usage:**
```python
# Get root attributes
root_attrs = builder.get_attrs()

# Get group attributes
group_attrs = builder.get_attrs('satellite/goes18')

# Get array attributes
array_attrs = builder.get_attrs('satellite/goes18/radiance')
```

---

### `set_attrs`

```python
set_attrs(path: str, attrs: dict, merge: bool = True)
```

**Description:** Sets attributes on a node

**Parameters:**
- `path` (str): Node path
- `attrs` (dict): Attributes to set
- `merge` (bool): Whether to merge with existing attributes (default: True)

**Raises:**
- `RuntimeError`: If store is not open

**Usage:**
```python
# Merge attributes
builder.set_attrs('satellite/goes18', {
    'platform': 'GOES-18',
    'status': 'active'
})

# Replace all attributes
builder.set_attrs('satellite/goes18', {
    'platform': 'GOES-18',
    'status': 'active',
    'updated': '2024-01-01'
}, merge=False)
```

---

### `del_attrs`

```python
del_attrs(path: str, keys: list[str])
```

**Description:** Deletes specific attributes from a node

**Parameters:**
- `path` (str): Node path
- `keys` (list[str]): Attribute names to delete

**Raises:**
- `RuntimeError`: If store is not open

**Usage:**
```python
# Delete specific attributes
builder.del_attrs('satellite/goes18', ['status', 'updated'])

# Missing attributes are ignored (no error)
builder.del_attrs('satellite/goes18', ['missing_attr', 'another_missing'])
```

---

## Information and Utility Methods

### `tree`

```python
tree(path: str = "/") -> str
```

**Description:** Generates a tree view of the store hierarchy

**Parameters:**
- `path` (str): Starting path (default: "/")

**Returns:** String representation of the hierarchy

**Raises:**
- `RuntimeError`: If store is not open

**Usage:**
```python
# View entire store structure
print(builder.tree())

# View specific subtree
print(builder.tree('satellite'))
```

**Example Output:**
```
/
├── satellite/
│   ├── goes18/
│   │   ├── radiance [float32, (1000, 512, 512)]
│   │   └── temperature [float32, (500, 512, 512)]
│   └── goes16/
│       └── radiance [float32, (800, 512, 512)]
├── time [datetime64[s], (1000,)]
└── latitude [float64, (512,)]
```

---

### `info`

```python
info(path: str = "/") -> str
```

**Description:** Gets basic information about a node

**Parameters:**
- `path` (str): Node path

**Returns:** String with node information

**Raises:**
- `RuntimeError`: If store is not open

**Usage:**
```python
# Get store info
print(builder.info())

# Get array info
print(builder.info('satellite/goes18/radiance'))
```

---

### `info_complete`

```python
info_complete(path: str) -> str
```

**Description:** Gets detailed storage statistics for an array

**Parameters:**
- `path` (str): Array path

**Returns:** String with detailed array information

**Raises:**
- `RuntimeError`: If store is not open
- `TypeError`: If path is not an array

**Note:** Can be slow for large arrays as it traverses all chunks

**Usage:**
```python
# Detailed array statistics
print(builder.info_complete('satellite/goes18/radiance'))
```

---

### `validate`

```python
validate() -> dict
```

**Description:** Checks store integrity and validates all nodes

**Returns:** Dictionary with validation result and issues

**Returns Structure:**
```python
{
    'valid': bool,           # Overall validation result
    'issues': list[str]      # List of issues found
}
```

**Usage:**
```python
result = builder.validate()
if result['valid']:
    print("Store validation passed")
else:
    print("Issues found:")
    for issue in result['issues']:
        print(f"  - {issue}")
```

---

### `__repr__`

```python
repr(builder) -> str
```

**Description:** String representation of the builder state

**Returns:** Summary string with store information

**Usage:**
```python
print(builder)
# Output: ZarrDatasetBuilder(store=/data/dataset.zarr, groups=3, arrays=15)

# When not initialized
print(ZarrDatasetBuilder('config.yaml'))
# Output: ZarrDatasetBuilder(not initialized)
```

---

## Configuration

### Environment Variable Expansion

The configuration system supports environment variable expansion:

```yaml
store:
  type: local
  path: $HOME/data/goes_dataset.zarr

zarr:
  compression:
    default:
      compressor:
        cname: ${COMPRESSION_CODEC:-zstd}  # Default to zstd
```

**Supported Patterns:**
- `$VAR` or `${VAR}`: Simple expansion
- `${VAR:-default}`: Expansion with default value
- Works in nested dictionaries and lists

### Store Types and Configuration

#### Local Store
```yaml
store:
  type: local
  path: /path/to/dataset.zarr
```

#### Memory Store
```yaml
store:
  type: memory
  # No path needed
```

#### Zip Store
```yaml
store:
  type: zip
  path: /path/to/dataset.zip
```

#### FSSpec Store (Remote)
```yaml
store:
  type: fsspec
  path: s3://my-bucket/dataset.zarr
  storage_options:
    key: $AWS_ACCESS_KEY_ID
    secret: $AWS_SECRET_ACCESS_KEY
    client_kwargs:
      region_name: us-west-2
```

#### Object Store
```yaml
store:
  type: object
  backend: s3
  bucket: my-bucket
  region: us-west-2
  anonymous: false
```

### Compression Configuration

```yaml
zarr:
  compression:
    default:
      compressor:
        codec: blosc
        cname: zstd
        clevel: 5
        shuffle: bitshuffle  # noshuffle, shuffle, bitshuffle
      serializer:
        codec: null
        cname: null
      filter:
        codec: null
        cname: null
      chunks: auto  # auto, null, or [256, 256]
      shards: null  # must be multiple of chunk size
      fill_value: NaN
```

---

## Complete Usage Examples

### Example 1: Creating a GOES Dataset

```python
from goesdatabuilder.store import ZarrDatasetBuilder
import numpy as np
from datetime import datetime, timedelta

# Initialize builder
builder = ZarrDatasetBuilder('goes_config.yaml')

# Create store
builder.create_store('/data/goes18_dataset.zarr')

# Create satellite group with metadata
satellite_attrs = {
    'platform': 'GOES-18',
    'instrument': 'ABI',
    'launch_date': '2022-03-01',
    'status': 'operational'
}
sat_group = builder.create_group('satellite/goes18', satellite_attrs)

# Create coordinates
# Time coordinate (100 time steps)
time_start = datetime(2024, 1, 1)
times = [time_start + timedelta(minutes=i) for i in range(100)]
time_coords = np.array(times, dtype='datetime64[s]')
builder.create_coordinate('time', time_coords, {
    'standard_name': 'time',
    'units': 'seconds since 1970-01-01'
})

# Spatial coordinates
lats = np.linspace(-90, 90, 512)
lons = np.linspace(-180, 180, 512)
builder.create_coordinate('latitude', lats, {'units': 'degrees_north'})
builder.create_coordinate('longitude', lons, {'units': 'degrees_east'})

# Create radiance array for band 8
radiance_attrs = {
    'long_name': 'Channel 8 Radiance',
    'units': 'W m^-2 sr^-1 μm^-1',
    'band_number': 8,
    'central_wavelength': 6.185  # μm
}
radiance = builder.create_array(
    path='satellite/goes18/band08_radiance',
    shape=(100, 512, 512),
    dtype='float32',
    attrs=radiance_attrs
)

# Create quality flag array
dqf_attrs = {
    'long_name': 'Data Quality Flags',
    'flag_meanings': 'good conditionally_usable out_of_range no_value',
    'flag_values': [0, 1, 2, 3]
}
dqf = builder.create_array(
    path='satellite/goes18/band08_dqf',
    shape=(100, 512, 512),
    dtype='uint8',
    attrs=dqf_attrs,
    preset='secondary'  # Use secondary compression for flags
)

print(f"Created dataset with {len(builder.array_list())} arrays")
print(builder.tree())
```

---

### Example 2: Streaming Data Ingestion

```python
from goesdatabuilder.store import ZarrDatasetBuilder
import numpy as np

# Open existing store for appending
builder = ZarrDatasetBuilder.from_existing('/data/streaming_dataset.zarr', 'config.yaml')

# Create empty arrays for streaming data
time_coord = builder.create_empty_coordinate('time', 'datetime64[s]')
radiance = builder.create_array('radiance', shape=(0, 512, 512), dtype='float32')

# Simulate incoming data stream
for batch_num in range(10):
    # Generate new data (e.g., from satellite feed)
    batch_size = 5
    new_times = np.arange(batch_num * batch_size, (batch_num + 1) * batch_size, dtype='datetime64[s]')
    new_radiance = np.random.random((batch_size, 512, 512)).astype('float32')
    
    # Append time coordinates
    builder.append_array('time', new_times)
    
    # Append radiance data
    location = builder.append_array('radiance', new_radiance, return_location=True)
    
    print(f"Batch {batch_num}: appended data to indices {location}")

# Final validation
result = builder.validate()
print(f"Store validation: {'PASSED' if result['valid'] else 'FAILED'}")
if not result['valid']:
    for issue in result['issues']:
        print(f"  Issue: {issue}")
```

---

### Example 3: Multi-Platform Dataset

```python
from goesdatabuilder.store import ZarrDatasetBuilder
import numpy as np

# Create multi-platform dataset
builder = ZarrDatasetBuilder('multi_platform_config.yaml')
builder.create_store('/data/multi_platform.zarr')

# Platform configurations
platforms = {
    'goes16': {'position': 75.0, 'status': 'operational'},
    'goes17': {'position': 137.0, 'status': 'operational'}, 
    'goes18': {'position': 89.0, 'status': 'operational'}
}

# Create structure for each platform
for platform, config in platforms.items():
    # Platform group
    platform_attrs = {
        'platform': f'GOES-{platform[-2:]}',
        'longitude': config['position'],
        'status': config['status']
    }
    builder.create_group(f'satellite/{platform}', platform_attrs)
    
    # Common coordinates for this platform
    lats = np.linspace(-90, 90, 1024)
    lons = np.linspace(-180, 180, 1024)
    builder.create_coordinate(f'satellite/{platform}/latitude', lats)
    builder.create_coordinate(f'satellite/{platform}/longitude', lons)
    
    # Create arrays for multiple bands
    for band in [2, 8, 13]:  # Representative bands
        band_attrs = {
            'band_number': band,
            'platform': platform,
            'long_name': f'Band {band} Radiance'
        }
        builder.create_array(
            path=f'satellite/{platform}/band{band:02d}_radiance',
            shape=(100, 1024, 1024),  # 100 time steps
            dtype='float32',
            attrs=band_attrs
        )

print("Multi-platform dataset structure:")
print(builder.tree())

# Store statistics
print(f"\nTotal groups: {len(builder.list_groups())}")
print(f"Total arrays: {len(builder.array_list())}")

# Validate the complete dataset
validation = builder.validate()
print(f"\nValidation: {'PASSED' if validation['valid'] else 'FAILED'}")
```

---

## Error Handling

The class includes comprehensive error handling for common scenarios:

### RuntimeError
Raised when operations are attempted on closed stores:
```python
builder = ZarrDatasetBuilder('config.yaml')
builder.create_array('data', (100, 100), 'float32')  # RuntimeError: Store not open
```

### ConfigError
Raised for configuration validation failures:
```python
# Missing required keys
# Invalid store type
# Invalid compression settings
```

### ValueError
Raised for invalid parameters:
```python
# Creating existing group/array
# Invalid data shapes
# Invalid coordinate dimensions
```

### KeyError
Raised for missing paths:
```python
builder.get_group('nonexistent')  # KeyError: Group not found
builder.get_array('missing')      # KeyError: Array not found
```

### FileExistsError
Raised when attempting to overwrite existing stores:
```python
builder.create_store('/existing/path')  # FileExistsError
```

---

## Performance Considerations

### Chunking Strategy
- Choose chunk sizes based on access patterns
- Time series: chunk along time dimension
- Spatial data: 2D spatial chunks
- Consider compression efficiency

### Memory Usage
- Large arrays are chunked and compressed
- Coordinate arrays loaded fully into memory
- Use streaming for very large datasets

### Compression Presets
- **Default**: Balanced compression for scientific data
- **Secondary**: Higher compression for metadata/flags
- Custom compressors available via configuration

### Best Practices
```python
# Good: Use context managers
with ZarrDatasetBuilder('config.yaml') as builder:
    builder.create_store('/data/dataset.zarr')
    # ... operations ...

# Good: Batch operations where possible
builder.create_array('data', (1000, 512, 512), 'float32')
# Write all data at once if possible
builder.write_array('data', large_dataset)

# Good: Choose appropriate chunk sizes
# For time series access: (time_chunk, spatial_chunk, spatial_chunk)
# For spatial access: (full_time, spatial_chunk, spatial_chunk)
```

---

## Integration with Subclasses

The `ZarrDatasetBuilder` is designed to be extended by domain-specific subclasses:

```python
class GOESZarrDataset(ZarrDatasetBuilder):
    def __init__(self, config_path: str | Path):
        super().__init__(config_path)
    
    def create_goes_array(self, band: int, shape: tuple, **kwargs):
        """Create GOES-specific array with standard metadata"""
        attrs = {
            'platform': 'GOES',
            'band_number': band,
            **kwargs.get('attrs', {})
        }
        return self.create_array(
            path=f'bands/band{band:02d}',
            shape=shape,
            attrs=attrs,
            **{k: v for k, v in kwargs.items() if k != 'attrs'}
        )
```

This design pattern allows for domain-specific convenience methods while maintaining the robust core functionality.