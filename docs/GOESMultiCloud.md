# GOESMultiCloud Class Documentation

The `GOESMultiCloud` class provides a convenient interface for accessing and analyzing GOES (Geostationary Operational Environmental Satellite) multi-cloud NetCDF data files. It encapsulates the complex data structure and provides easy-to-use properties for accessing specific bands, statistics, and metadata.

## Installation

Make sure you have the required dependencies installed:

```bash
pip install xarray>=2024.7.0 netcdf4>=1.7.2
```

## Quick Start

```python
from goes2zarr.data.goesmulticloud import GOESMultiCloud

# Load a multi-cloud NetCDF file
gmc = GOESMultiCloud('path/to/multi_cloud_file.nc')

# Set the band you want to analyze (1-16)
gmc.band = 2

# Access the Cloud and Moisture Imagery for band 2
cmi_data = gmc.CMI
print(f"CMI shape: {cmi_data['values'].shape}")

# Get statistics for the current band
stats = gmc.CMI_stats
print(f"Reflectance stats: {stats}")
```

## Class Overview

### Constructor

```python
GOESMultiCloud(multi_cloud_nc_file)
```

**Parameters:**
- `multi_cloud_nc_file` (str): Path to the multi-cloud NetCDF file

**What it does:**
- Opens the NetCDF file using xarray
- Extracts coordinates, variables, and attributes
- Initializes internal data structures for efficient access

---

## Core Properties

### `band`

**Type:** Property (getter/setter)

**Description:** Gets or sets the current GOES band number (1-16)

**Usage:**
```python
# Get current band
current_band = gmc.band

# Set band 5 (reflectance band)
gmc.band = 5

# Set band 14 (brightness temperature band)
gmc.band = 14
```

**Valid Values:**
- Bands 1-6: Reflectance bands (visible/near-infrared)
- Bands 7-16: Brightness temperature bands (infrared)

**Raises:** `ValueError` if band number is outside 1-16 range

---

## Data Access Properties

### `variable_keys`

**Type:** Property (read-only)

**Description:** Returns a list of all variable keys available in the dataset

**Usage:**
```python
variables = gmc.variable_keys
print(f"Available variables: {variables}")
```

### `CMI`

**Type:** Property (read-only)

**Description:** Returns Cloud and Moisture Imagery data for the currently selected band

**Returns:** Dictionary with:
- `values`: NumPy array containing the CMI data
- `attributes`: Dictionary of variable attributes

**Usage:**
```python
gmc.band = 3
cmi = gmc.CMI
data_array = cmi['values']  # 2D numpy array
attributes = cmi['attributes']  # metadata
print(f"Data shape: {data_array.shape}")
print(f"Units: {attributes.get('units', 'N/A')}")
```

**Note:** Returns `None` if no band is currently selected

### `DQF`

**Type:** Property (read-only)

**Description:** Returns Data Quality Flag for the currently selected band

**Returns:** Dictionary with:
- `values`: NumPy array containing quality flags
- `attributes`: Dictionary of variable attributes

**Usage:**
```python
gmc.band = 7
dqf = gmc.DQF
quality_flags = dqf['values']  # 2D numpy array
print(f"Unique quality values: {np.unique(quality_flags)}")
```

**Note:** Returns `None` if no band is currently selected

---

## Statistical Properties

### `CMI_stats`

**Type:** Property (read-only)

**Description:** Returns comprehensive statistics for the current band

**Returns:** Dictionary containing relevant statistics based on band type:

**For Reflectance Bands (1-6):**
- `min_reflectance`: Minimum reflectance factor
- `max_reflectance`: Maximum reflectance factor  
- `mean_reflectance`: Mean reflectance factor
- `std_reflectance`: Standard deviation of reflectance

**For Brightness Temperature Bands (7-16):**
- `min_brightness_temp`: Minimum brightness temperature
- `max_brightness_temp`: Maximum brightness temperature
- `mean_brightness_temp`: Mean brightness temperature
- `std_brightness_temp`: Standard deviation of brightness temperature

**For All Bands:**
- `outlier_count`: Number of outlier pixels detected

**Usage:**
```python
gmc.band = 2  # Reflectance band
stats = gmc.CMI_stats
print(f"Reflectance range: {stats['min_reflectance']} - {stats['max_reflectance']}")
print(f"Mean reflectance: {stats['mean_reflectance']:.3f}")

gmc.band = 14  # Brightness temperature band
stats = gmc.CMI_stats
print(f"Temperature range: {stats['min_brightness_temp']} - {stats['max_brightness_temp']} K")
```

---

## Satellite and Projection Properties

### `goes_imager_projection`

**Type:** Property (read-only)

**Description:** Returns GOES imager projection information

**Returns:** Dictionary containing projection parameters

**Usage:**
```python
projection = gmc.goes_imager_projection
print(f"Projection info: {projection['attributes']}")
```

**Raises:** `KeyError` if projection data is not found

### `satellite_height`

**Type:** Property (read-only)

**Description:** Returns nominal satellite height above Earth's surface

**Returns:** Dictionary with height information

**Usage:**
```python
height = gmc.satellite_height
print(f"Satellite height: {height['values']} km")
```

**Raises:** `KeyError` if height data is not found

### `subpoint_lon`

**Type:** Property (read-only)

**Description:** Returns longitude of satellite subpoint (where satellite is directly overhead)

**Returns:** Dictionary with longitude information

**Usage:**
```python
lon = gmc.subpoint_lon
print(f"Subpoint longitude: {lon['values']}°")
```

**Raises:** `KeyError` if subpoint data is not found

### `subpoint_lat`

**Type:** Property (read-only)

**Description:** Returns latitude of satellite subpoint

**Returns:** Dictionary with latitude information

**Usage:**
```python
lat = gmc.subpoint_lat
print(f"Subpoint latitude: {lat['values']}°")
```

**Raises:** `KeyError` if subpoint data is not found

---

## Data Quality Properties

### `data_quality_metrics`

**Type:** Property (read-only)

**Description:** Returns transmission error percentages for data quality assessment

**Returns:** Dictionary with:
- `grb_errors`: Percentage of uncorrectable GRB (GOES Rebroadcast) errors
- `l0_errors`: Percentage of uncorrectable L0 (Level 0) errors

**Usage:**
```python
quality = gmc.data_quality_metrics
print(f"GRB error rate: {quality['grb_errors']['values']}%")
print(f"L0 error rate: {quality['l0_errors']['values']}%")
```

---

## Metadata Properties

### `source_data_files`

**Type:** Property (read-only)

**Description:** Returns information about the input L1b radiance files used to create this L2 product

**Returns:** Dictionary mapping input file patterns to their descriptions, or `None` if not available

**Usage:**
```python
source_files = gmc.source_data_files
if source_files:
    for pattern, description in source_files.items():
        print(f"{pattern}: {description}")
```

### `product_version_info`

**Type:** Property (read-only)

**Description:** Returns algorithm and product version information

**Returns:** Dictionary with:
- `algorithm_version`: Algorithm version string (e.g., "OR_ABI-L2-ALG-CMIP_v01r00.zip")
- `product_version`: Product version string (e.g., "v01r00")
- `major_release`: Major version number (e.g., 1)
- `minor_revision`: Minor revision number (e.g., 0)

**Usage:**
```python
version_info = gmc.product_version_info
if version_info:
    print(f"Algorithm: {version_info['algorithm_version']}")
    print(f"Product version: {version_info['product_version']}")
    print(f"Release {version_info['major_release']}.{version_info['minor_revision']}")
```

---

## Complete Usage Example

```python
from goes2zarr.data.goesmulticloud import GOESMultiCloud
import numpy as np
import matplotlib.pyplot as plt

# Load the data
gmc = GOESMultiCloud('multi_cloud_data.nc')

# Print basic information
print(f"Available variables: {gmc.variable_keys}")
print(f"Satellite position: {gmc.subpoint_lon['values']}°, {gmc.subpoint_lat['values']}°")
print(f"Satellite height: {gmc.satellite_height['values']} km")

# Analyze different bands
for band in [2, 5, 14]:  # Mix of reflectance and temperature bands
    gmc.band = band
    
    print(f"\n=== Band {band} ===")
    
    # Get data
    cmi = gmc.CMI
    dqf = gmc.DQF
    stats = gmc.CMI_stats
    
    print(f"Data shape: {cmi['values'].shape}")
    print(f"Data type: {cmi['values'].dtype}")
    
    # Print statistics
    if band <= 6:  # Reflectance bands
        print(f"Reflectance: {stats['mean_reflectance']:.3f} ± {stats['std_reflectance']:.3f}")
        print(f"Range: {stats['min_reflectance']:.3f} - {stats['max_reflectance']:.3f}")
    else:  # Temperature bands
        print(f"Temperature: {stats['mean_brightness_temp']:.1f} ± {stats['std_brightness_temp']:.1f} K")
        print(f"Range: {stats['min_brightness_temp']:.1f} - {stats['max_brightness_temp']:.1f} K")
    
    print(f"Outlier pixels: {stats['outlier_count']}")
    
    # Quality check
    unique_dqf = np.unique(dqf['values'])
    print(f"Quality flags: {unique_dqf}")

# Data quality assessment
quality = gmc.data_quality_metrics
print(f"\n=== Data Quality ===")
print(f"GRB error rate: {quality['grb_errors']['values']}%")
print(f"L0 error rate: {quality['l0_errors']['values']}%")

# Version information
version = gmc.product_version_info
if version:
    print(f"\n=== Version Info ===")
    print(f"Algorithm: {version['algorithm_version']}")
    print(f"Product: {version['product_version']}")

# Visualization example (if matplotlib is available)
try:
    gmc.band = 2  # Use a visible band
    cmi_data = gmc.CMI['values']
    
    plt.figure(figsize=(10, 8))
    plt.imshow(cmi_data, cmap='gray')
    plt.title(f'GOES Band {gmc.band} - CMI')
    plt.colorbar(label='Reflectance')
    plt.show()
except ImportError:
    print("Matplotlib not available for visualization")
```

## Error Handling

The class includes built-in error handling:

- **Invalid band numbers:** Raises `ValueError` for bands outside 1-16
- **Missing data:** Raises `KeyError` for essential satellite/projection data
- **No band selected:** Returns `None` for band-dependent properties when no band is set

## Performance Notes

- The NetCDF file is only opened once during initialization
- All data is loaded into memory for fast access
- Consider memory usage for very large files
- Properties return references to internal data structures for efficiency

## Data Structure

The class organizes data into three main categories:
1. **Coordinates:** Spatial and temporal coordinate information
2. **Variables:** Science data arrays with their attributes
3. **Attributes:** Global metadata about the dataset

Each property provides convenient access to specific parts of this structure without requiring knowledge of the underlying NetCDF variable naming conventions.
