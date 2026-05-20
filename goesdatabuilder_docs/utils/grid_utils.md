# Grid Utilities

## Overview

The `goesdatabuilder.utils.grid_utils` module provides utility functions for handling geographic coordinate transformations, particularly for longitude arrays that cross the antimeridian. These functions are used throughout the GOES data builder pipeline for grid construction and validation.

### Key Features

- **Antimeridian-safe longitude array construction**
- **Monotonicity validation for geographic coordinates**
- **Automatic handling of longitude wraparound issues**
- **Support for both -180/180 and 0-360 longitude conventions**

## Functions

### `build_longitude_array`

Builds a 1D longitude array that properly handles antimeridian crossing.

```python
from goesdatabuilder.utils.grid_utils import build_longitude_array

# Standard case (no antimeridian crossing)
lon = build_longitude_array(-120.0, -60.0, 0.02)
# Returns: [-120.0, -119.98, -119.96, ..., -60.0]

# Antimeridian crossing case
lon = build_longitude_array(165.0, -115.0, 0.02)
# Returns: [165.0, 165.02, ..., 178.0, -178.0, ..., -115.0]
```

**Parameters:**
- `lon_min` (float): Western bound in degrees (-180 to 180)
- `lon_max` (float): Eastern bound in degrees (-180 to 180)  
- `resolution` (float): Grid spacing in degrees
- `decimals` (int, optional): Decimal places for rounding (default: 4)

**Returns:**
- `np.ndarray`: 1D longitude array in -180/180 convention

**Behavior:**
- When `lon_min <= lon_max`: Creates standard monotonic array
- When `lon_min > lon_max`: Handles antimeridian crossing by working in 0-360 space, then converting back

### `is_antimeridian_crossing`

Detects if a longitude array crosses the antimeridian.

```python
from goesdatabuilder.utils.grid_utils import is_antimeridian_crossing

# Standard array
lon_standard = np.array([-120.0, -110.0, -100.0])
print(is_antimeridian_crossing(lon_standard))  # False

# Antimeridian crossing array
lon_crossing = np.array([170.0, 175.0, -175.0, -170.0])
print(is_antimeridian_crossing(lon_crossing))  # True
```

**Parameters:**
- `lon` (np.ndarray): 1D longitude array in -180/180 convention

**Returns:**
- `bool`: True if the array crosses the antimeridian

**Detection Logic:**
- Looks for large negative jumps (> 180 degrees) between consecutive values
- Indicates transition from ~+180 to ~-180 degrees

### `validate_longitude_monotonic`

Validates that a longitude array is monotonic, allowing antimeridian crossing.

```python
from goesdatabuilder.utils.grid_utils import validate_longitude_monotonic

# Standard monotonic array
lon_standard = np.array([-120.0, -110.0, -100.0])
print(validate_longitude_monotonic(lon_standard))  # True

# Antimeridian crossing monotonic array
lon_crossing = np.array([170.0, 175.0, -175.0, -170.0])
print(validate_longitude_monotonic(lon_crossing))  # True

# Non-monotonic array
lon_bad = np.array([-120.0, -110.0, -120.0])
print(validate_longitude_monotonic(lon_bad))  # False
```

**Parameters:**
- `lon` (np.ndarray): 1D longitude array in -180/180 convention

**Returns:**
- `bool`: True if monotonically increasing (in 0-360 if crossing)

**Validation Logic:**
- For antimeridian-crossing arrays: Checks monotonicity in 0-360 space
- For standard arrays: Checks monotonicity in -180/180 space
- Allows both increasing and decreasing monotonic sequences

## Usage Examples

### GOES-West Grid Construction

```python
import numpy as np
from goesdatabuilder.utils.grid_utils import build_longitude_array, validate_longitude_monotonic

# GOES-West crosses antimeridian
lon_min, lon_max = 165.0, -115.0
resolution = 0.02

# Build longitude array
lon = build_longitude_array(lon_min, lon_max, resolution, decimals=4)

# Validate monotonicity
if validate_longitude_monotonic(lon):
    print(f"Valid longitude array: {len(lon)} points")
    print(f"Range: {lon.min():.1f} to {lon.max():.1f}")
else:
    raise ValueError("Longitude array is not monotonic")
```

### Grid Validation

```python
from goesdatabuilder.utils.grid_utils import is_antimeridian_crossing, validate_longitude_monotonic

def validate_grid(lat, lon):
    """Validate latitude and longitude arrays"""
    
    # Check latitude monotonicity
    if not (np.all(np.diff(lat) > 0) or np.all(np.diff(lat) < 0)):
        raise ValueError("Latitude array must be monotonic")
    
    # Check longitude monotonicity (handles antimeridian)
    if not validate_longitude_monotonic(lon):
        raise ValueError("Longitude array must be monotonic")
    
    # Report antimeridian crossing
    if is_antimeridian_crossing(lon):
        print("Grid crosses antimeridian - using special handling")
    
    return True
```

## Integration with Pipeline

These utilities are used in several key places:

### GeostationaryRegridder
```python
# In GeostationaryRegridder.__init__
self._target_lon = build_longitude_array(
    float(valid_lons.min()),
    float(valid_lons.max()),
    target_resolution,
    decimals=decimals
)
```

### GOESPipelineOrchestrator
```python
# In initialize_regridder method
target_lon = build_longitude_array(
    target_config['lon_min'],
    target_config['lon_max'], 
    lon_res,
    decimals=decimals
)
```

### GOESZarrStore
```python
# In initialize_region method
if not validate_longitude_monotonic(lon):
    raise ValueError("Longitude array must be monotonic")
```

## Technical Details

### Coordinate System Handling

The utilities work primarily with the -180/180 longitude convention but internally use 0-360 space for antimeridian-crossing arrays to ensure proper monotonicity checking.

### Precision Control

The `decimals` parameter allows control over coordinate precision:
- Higher values (more decimals): Better precision, larger file sizes
- Lower values (fewer decimals): Smaller file sizes, potential precision loss
- Typical GOES data: 4-6 decimal places

### Performance Considerations

- Functions are vectorized for efficient processing of large arrays
- Minimal memory overhead - works with array views when possible
- Suitable for real-time grid validation in processing pipelines

## Error Handling

The functions include built-in validation:

```python
# Invalid range (lon_min > lon_max without antimeridian crossing)
try:
    lon = build_longitude_array(-60.0, -120.0, 0.02)
except ValueError as e:
    print(f"Invalid range: {e}")

# Empty array
lon_empty = np.array([])
print(is_antimeridian_crossing(lon_empty))  # False
print(validate_longitude_monotonic(lon_empty))  # False
```

## Best Practices

1. **Always validate** longitude arrays before using them in grid operations
2. **Use consistent precision** (decimals) across all coordinate arrays
3. **Check for antimeridian crossing** when working with Pacific-region data
4. **Document coordinate conventions** in your processing pipeline
5. **Test edge cases** (180/-180 boundaries) in your validation routines
