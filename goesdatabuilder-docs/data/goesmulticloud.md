# GOESMultiCloud Class Documentation

The `GOESMultiCloud` class provides a convenient, property-based interface for accessing and analyzing GOES (Geostationary Operational Environmental Satellite) multi-cloud NetCDF data files. It encapsulates the complex CF-compliant data structure and provides easy-to-use properties for accessing specific bands, statistics, coordinates, and metadata. This class works with `Full-Disk`, `Mesoscale` and `CONUS` scenes for all `ABI-L2-MCMIP` NetCDF data products.

## Installation

Make sure you have the required dependencies installed:

```bash
pip install xarray>=2024.7.0 netcdf4>=1.7.2 numpy>=1.24.0
```

## Quick Start

```python
from goesdatabuilder.data import GOESMultiCloud

# Load a multi-cloud NetCDF file
gmc = GOESMultiCloud('OR_ABI-L2-MCMIPF-M6_G18_s20230022200205_e20230022209525_c20230022210006.nc')

# Inspect the object
print(gmc)  # GOESMultiCloud(dataset='OR_ABI-L2-MCMIPF-M6_G18_...', no band selected, platform=G18, scene=Full Disk)

# Set the band you want to analyze (1-16)
gmc.band = 8

# Access the Cloud and Moisture Imagery for band 8
cmi_data = gmc.CMI
print(f"CMI shape: {cmi_data['values'].shape}")
print(f"CMI units: {cmi_data['attributes'].get('units')}")

# Get statistics for the current band
stats = gmc.CMI_stats
print(f"Mean brightness temperature: {stats['mean_brightness_temp']['values'].item():.2f} K")

# Access satellite information
print(f"Satellite: {gmc.platform_id} at {gmc.orbital_slot}")
print(f"Scene: {gmc.scene_id}")
```

---

## Class Overview

### Constructor

```python
GOESMultiCloud(multi_cloud_nc_file)
```

**Parameters:**
- `multi_cloud_nc_file` (str): Path to the multi-cloud NetCDF file

**What it does:**
- Opens the NetCDF file using xarray with netcdf4 engine
- Extracts all coordinates, variables, and global attributes into memory
- Initializes internal data structures for efficient property-based access
- Closes the NetCDF file after loading data

**Memory Note:** All data is loaded into memory during initialization for fast access. Consider available RAM when working with large files.

### Object Representation

```python
repr(gmc)
# Returns: GOESMultiCloud(dataset='filename.nc', band=8, platform=G18, scene=Full Disk)
```

The `__repr__` method provides a quick overview of the dataset, current band selection, platform ID, and scene type.

---

## Core Properties

### `dataset_id`

**Type:** Property (read-only)

**Description:** Universally Unique Identifier (UUID) for this specific product instance

**Returns:** String containing the UUID

**Usage:**
```python
uuid = gmc.dataset_id
print(f"Product UUID: {uuid}")
```

---

### `dataset_name`

**Type:** Property (read-only)

**Description:** Filename of the NetCDF dataset following GOES naming conventions

**Returns:** String with the dataset filename

**Usage:**
```python
filename = gmc.dataset_name
print(f"Dataset: {filename}")
# Output: OR_ABI-L2-MCMIPF-M6_G18_s20230022200205_e20230022209525_c20230022210006.nc
```

---

### `band`

**Type:** Property (getter/setter)

**Description:** Gets or sets the current GOES ABI band number (1-16)

**Usage:**
```python
# Get current band
current_band = gmc.band

# Set band 2 (visible red reflectance)
gmc.band = 2

# Set band 13 (clean IR longwave window)
gmc.band = 13
```

**Valid Values:**
- **Bands 1-6:** Reflectance bands (visible/near-infrared)
  - Band 1: Blue (0.47 μm)
  - Band 2: Red (0.64 μm)
  - Band 3: Veggie (0.86 μm)
  - Band 4: Cirrus (1.37 μm)
  - Band 5: Snow/Ice (1.6 μm)
  - Band 6: Cloud Particle Size (2.2 μm)
- **Bands 7-16:** Brightness temperature bands (infrared)
  - Band 7: Shortwave Window (3.9 μm)
  - Band 8: Upper Level Water Vapor (6.2 μm)
  - Band 9: Mid Level Water Vapor (6.9 μm)
  - Band 10: Low Level Water Vapor (7.3 μm)
  - Band 11: Cloud-Top Phase (8.4 μm)
  - Band 12: Ozone (9.6 μm) 
  - BAnd 13: Clean Longwave (10.3 μm)
  - Band 14: Longwave Window (11.2 μm)
  - Band 15: Dirty Longwave (12.3 μm)
  - Band 16: Carbon Dioxide (13.3 μm)

**Raises:** `ValueError` if band number is outside 1-16 range

---

## Global Metadata Properties

### `global_attribute_keys`

**Type:** Property (read-only)

**Description:** Returns a list of all global attribute keys available in the dataset

**Usage:**
```python
attrs = gmc.global_attribute_keys
print(f"Global attributes: {attrs}")
```

---

### `product_version_info`

**Type:** Property (read-only)

**Description:** Returns algorithm and product version information

**Returns:** Dictionary with:
- `algorithm_version`: Algorithm package filename (e.g., "OR_ABI-L2-ALG-CMIP_v01r00.zip")
- `product_version`: Product version string (e.g., "v01r00")
- `major_release`: Major version number extracted from version string
- `minor_revision`: Minor revision number extracted from version string

**Usage:**
```python
version_info = gmc.product_version_info
if version_info:
    print(f"Algorithm: {version_info['algorithm_version']}")
    print(f"Product: {version_info['product_version']}")
    print(f"Version {version_info['major_release']}.{version_info['minor_revision']}")
```

**Note:** Returns `None` if version container is not present in the dataset

---

## Production Metadata Properties

### `production_site`

**Type:** Property (read-only)

**Description:** Facility where the product was generated

**Possible Values:** NSOF, ESPC, WCDAS

**Usage:**
```python
site = gmc.production_site
print(f"Produced at: {site}")
```

---

### `production_environment`

**Type:** Property (read-only)

**Description:** Production environment designation

**Possible Values:**
- `OE`: Operational Environment
- `DE`: Development Environment

**Usage:**
```python
env = gmc.production_environment
print(f"Environment: {env}")
```

---

### `production_data_source`

**Type:** Property (read-only)

**Description:** Source type of the production data

**Possible Values:** Realtime, Simulated, Playback, Test

**Usage:**
```python
source = gmc.production_data_source
print(f"Data source: {source}")
```

---

### `processing_level`

**Type:** Property (read-only)

**Description:** NASA processing level designation

**Returns:** String (e.g., "National Aeronautics and Space Administration (NASA) L2")

**Usage:**
```python
level = gmc.processing_level
print(f"Processing level: {level}")
```

---

### `production_info`

**Type:** Property (read-only)

**Description:** Consolidated dictionary of all production metadata

**Returns:** Dictionary containing:
- `site`: Production site
- `environment`: Production environment
- `data_source`: Data source type
- `processing_level`: Processing level

**Usage:**
```python
prod_info = gmc.production_info
print(f"Production: {prod_info['site']} ({prod_info['environment']})")
print(f"Source: {prod_info['data_source']}")
```

---

## Temporal Properties

### `date_created`

**Type:** Property (read-only)

**Description:** Product creation timestamp in ISO 8601 format

**Format:** `YYYY-MM-DDTHH:MM:SS.sZ`

**Usage:**
```python
created = gmc.date_created
print(f"Product created: {created}")
# Output: 2023-01-02T22:10:00.6Z
```

---

### `time_coverage_start`

**Type:** Property (read-only)

**Description:** Observation start time in ISO 8601 format

**Format:** `YYYY-MM-DDTHH:MM:SS.sZ`

**Usage:**
```python
start = gmc.time_coverage_start
print(f"Observation start: {start}")
```

---

### `time_coverage_end`

**Type:** Property (read-only)

**Description:** Observation end time in ISO 8601 format

**Format:** `YYYY-MM-DDTHH:MM:SS.sZ`

**Usage:**
```python
end = gmc.time_coverage_end
print(f"Observation end: {end}")
```

---

### `temporal_info`

**Type:** Property (read-only)

**Description:** Consolidated dictionary of all temporal metadata

**Returns:** Dictionary containing:
- `created`: Product creation timestamp
- `start`: Observation start time
- `end`: Observation end time

**Usage:**
```python
times = gmc.temporal_info
print(f"Observed from {times['start']} to {times['end']}")
print(f"Product created at {times['created']}")
```

---

## Coordinate Properties

### `coordinate_keys`

**Type:** Property (read-only)

**Description:** Returns a list of all coordinate keys available in the dataset

**Usage:**
```python
coords = gmc.coordinate_keys
print(f"Available coordinates: {coords}")
```

---

### `band_wavelength`

**Type:** Property (read-only)

**Description:** Central wavelength of the currently selected band

**Returns:** DataArray with wavelength value and attributes, or `None` if no band selected

**Usage:**
```python
gmc.band = 2
wavelength = gmc.band_wavelength
print(f"Band 2 wavelength: {wavelength.values} {wavelength.attrs.get('units', '')}")
# Output: Band 2 wavelength: 0.64 μm
```

---

### `band_id`

**Type:** Property (read-only)

**Description:** Numeric identifier for the currently selected band

**Returns:** DataArray with band ID, or `None` if no band selected

**Usage:**
```python
gmc.band = 8
band_id = gmc.band_id
print(f"Band ID: {band_id.values}")
```

---

### `time`

**Type:** Property (read-only)

**Description:** Time coordinate (mid-scan time)

**Returns:** DataArray with time coordinate, or `None` if not present

**Usage:**
```python
t = gmc.time
print(f"Mid-scan time: {t.values}")
```

---

### `time_bounds`

**Type:** Property (read-only)

**Description:** Time boundaries for the observation period

**Returns:** Dictionary with values and attributes, or `None` if not present

**Usage:**
```python
bounds = gmc.time_bounds
if bounds:
    print(f"Time bounds: {bounds['values']}")
```

---

### `x_axis`

**Type:** Property (read-only)

**Description:** X-coordinate axis in the satellite projection (east-west in radians)

**Returns:** DataArray with x coordinates, or `None` if not present

**Usage:**
```python
x = gmc.x_axis
print(f"X-axis shape: {x.shape}")
print(f"X-axis range: {x.values.min():.6f} to {x.values.max():.6f} rad")
```

---

### `x_bounds`

**Type:** Property (read-only)

**Description:** Boundaries of the x-coordinate dimension

**Returns:** Dictionary with values and attributes, or `None` if not present

---

### `x_center`

**Type:** Property (read-only)

**Description:** Center point of the x-coordinate dimension

**Returns:** DataArray with x center, or `None` if not present

---

### `y_axis`

**Type:** Property (read-only)

**Description:** Y-coordinate axis in the satellite projection (north-south in radians)

**Returns:** DataArray with y coordinates, or `None` if not present

**Usage:**
```python
y = gmc.y_axis
print(f"Y-axis shape: {y.shape}")
print(f"Y-axis range: {y.values.min():.6f} to {y.values.max():.6f} rad")
```

---

### `y_bounds`

**Type:** Property (read-only)

**Description:** Boundaries of the y-coordinate dimension

**Returns:** Dictionary with values and attributes, or `None` if not present

---

### `y_center`

**Type:** Property (read-only)

**Description:** Center point of the y-coordinate dimension

**Returns:** DataArray with y center, or `None` if not present

---

## Variable Properties

### `variable_keys`

**Type:** Property (read-only)

**Description:** Returns a list of all variable keys (data variables) available in the dataset

**Usage:**
```python
variables = gmc.variable_keys
print(f"Available variables ({len(variables)}): {variables[:5]}...")
```

---

### `CMI`

**Type:** Property (read-only)

**Description:** Cloud and Moisture Imagery data for the currently selected band

**Returns:** Dictionary with:
- `values`: NumPy array containing the CMI data
- `attributes`: Dictionary of variable attributes (units, long_name, valid_range, etc.)

**Returns `None`** if no band is currently selected

**Raises:** `KeyError` if a band is selected but the corresponding CMI data is not found in the dataset

**Usage:**
```python
gmc.band = 2
cmi = gmc.CMI

# Access data
data_array = cmi['values']  # 2D numpy array
print(f"Data shape: {data_array.shape}")
print(f"Data type: {data_array.dtype}")

# Access metadata
attrs = cmi['attributes']
print(f"Units: {attrs.get('units', 'N/A')}")
print(f"Long name: {attrs.get('long_name', 'N/A')}")
print(f"Valid range: {attrs.get('valid_range', 'N/A')}")

# Data analysis
print(f"Min: {data_array.min():.3f}")
print(f"Max: {data_array.max():.3f}")
print(f"Mean: {data_array.mean():.3f}")
```

**Band-Specific Units:**
- Bands 1-6 (Reflectance): Dimensionless reflectance factor [0, 1]
- Bands 7-16 (Brightness Temperature): Kelvin [K]

---

### `DQF`

**Type:** Property (read-only)

**Description:** Data Quality Flag for the currently selected band

**Returns:** Dictionary with:
- `values`: NumPy array containing quality flags
- `attributes`: Dictionary of variable attributes

**Returns `None`** if no band is currently selected

**Raises:** `KeyError` if a band is selected but the corresponding DQF data is not found in the dataset

**Quality Flag Values:**
- `0`: Good quality pixels
- `1`: Conditionally usable pixels
- `2`: Out of range pixels
- `3`: No value pixels
- `4`: Focal plane temperature threshold exceeded

**Usage:**
```python
gmc.band = 8
dqf = gmc.DQF

quality_flags = dqf['values']
unique_flags, counts = np.unique(quality_flags[~np.isnan(quality_flags)], return_counts=True)

print(f"Quality flag distribution:")
for flag, count in zip(unique_flags, counts):
    pct = 100 * count / counts.sum()
    print(f"  Flag {int(flag)}: {count:,} pixels ({pct:.1f}%)")

# Create quality mask (good quality only)
good_quality_mask = (quality_flags == 0)
```

---

### `CMI_stats`

**Type:** Property (read-only)

**Description:** Pre-computed statistics for the currently selected band from the NetCDF file

**Returns:** Dictionary containing relevant statistics based on band type, or `None` if no band selected

**For Reflectance Bands (1-6):**
- `min_reflectance`: Dictionary with minimum reflectance factor
- `max_reflectance`: Dictionary with maximum reflectance factor  
- `mean_reflectance`: Dictionary with mean reflectance factor
- `std_reflectance`: Dictionary with standard deviation of reflectance

**For Brightness Temperature Bands (7-16):**
- `min_brightness_temp`: Dictionary with minimum brightness temperature
- `max_brightness_temp`: Dictionary with maximum brightness temperature
- `mean_brightness_temp`: Dictionary with mean brightness temperature
- `std_brightness_temp`: Dictionary with standard deviation of brightness temperature

**For All Bands:**
- `outlier_count`: Dictionary with number of outlier pixels detected

**Usage:**
```python
# Reflectance band example
gmc.band = 2
stats = gmc.CMI_stats
print(f"Reflectance range: [{stats['min_reflectance']['values'].item():.3f}, "
      f"{stats['max_reflectance']['values'].item():.3f}]")
print(f"Mean ± Std: {stats['mean_reflectance']['values'].item():.3f} ± "
      f"{stats['std_reflectance']['values'].item():.3f}")
print(f"Outliers: {stats['outlier_count']['values'].item():.0f} pixels")

# Brightness temperature band example
gmc.band = 14
stats = gmc.CMI_stats
print(f"Temperature range: [{stats['min_brightness_temp']['values'].item():.1f}, "
      f"{stats['max_brightness_temp']['values'].item():.1f}] K")
print(f"Mean ± Std: {stats['mean_brightness_temp']['values'].item():.1f} ± "
      f"{stats['std_brightness_temp']['values'].item():.1f} K")
```

**Note:** These statistics are computed only from good and conditionally usable quality pixels during product generation.

---

## Satellite and Projection Properties

### Satellite Identification

#### `orbital_slot`

**Type:** Property (read-only)

**Description:** GOES orbital position designation

**Possible Values:** GOES-East, GOES-West, GOES-Test, GOES-Storage

**Usage:**
```python
slot = gmc.orbital_slot
print(f"Orbital slot: {slot}")
```

---

#### `platform_id`

**Type:** Property (read-only)

**Description:** Satellite platform identifier

**Possible Values:** G16, G17, G18, G19

**Usage:**
```python
platform = gmc.platform_id
print(f"Satellite: GOES-{int(platform[1:])} ({platform})")
# Output: Satellite: GOES-18 (G18)
```

---

#### `instrument_type`

**Type:** Property (read-only)

**Description:** Instrument type designation

**Returns:** String (e.g., "GOES-R Series Advanced Baseline Imager (ABI)")

**Usage:**
```python
instrument = gmc.instrument_type
print(f"Instrument: {instrument}")
```

---

#### `instrument_id`

**Type:** Property (read-only)

**Description:** Serial number of the instrument (Flight Model number)

**Possible Values:** FM1, FM2, FM3, FM4, FM5, FM6

**Usage:**
```python
serial = gmc.instrument_id
print(f"Instrument S/N: {serial}")
```

---

#### `satellite_info`

**Type:** Property (read-only)

**Description:** Consolidated dictionary of all satellite identification metadata

**Returns:** Dictionary containing:
- `orbital_slot`: Orbital position
- `platform_id`: Platform identifier
- `instrument_type`: Instrument type
- `instrument_id`: Instrument serial number

**Usage:**
```python
sat_info = gmc.satellite_info
print(f"Satellite: {sat_info['platform_id']} at {sat_info['orbital_slot']}")
print(f"Instrument: {sat_info['instrument_type']} (S/N: {sat_info['instrument_id']})")
```

---

#### `scan_mode`

**Type:** Property (read-only)

**Description:** ABI scanning mode (timeline) during observation

**Possible Values:**
- `ABI Mode 3`: ABI flex mode
- `ABI Mode 4`: ABI continuous FD mode
- `ABI Mode 6`: ABI super flex mode

**Usage:**
```python
mode = gmc.scan_mode
print(f"Scan mode: {mode}")
```

---

#### `scene_id`

**Type:** Property (read-only)

**Description:** Scene/sector type of the observation

**Possible Values:**
- `Full Disk`: Full Earth disk view
- `CONUS`: Continental United States
- `Mesoscale`: Mesoscale sector (1000 km x 1000 km)

**Usage:**
```python
scene = gmc.scene_id
print(f"Scene: {scene}")
```

---

### Projection Parameters

#### `goes_imager_projection`

**Type:** Property (read-only)

**Description:** GOES fixed grid projection information following CF conventions

**Returns:** Dictionary with values and attributes containing projection parameters

**Raises:** `KeyError` if projection data is not found in dataset

**Usage:**
```python
proj = gmc.goes_imager_projection
proj_attrs = proj['attributes']

print(f"Projection: {proj_attrs.get('grid_mapping_name')}")
print(f"Perspective point height: {proj_attrs.get('perspective_point_height')} m")
print(f"Longitude of projection origin: {proj_attrs.get('longitude_of_projection_origin')}°")
print(f"Semi-major axis: {proj_attrs.get('semi_major_axis')} m")
print(f"Semi-minor axis: {proj_attrs.get('semi_minor_axis')} m")
print(f"Sweep angle axis: {proj_attrs.get('sweep_angle_axis')}")
```

---

#### `satellite_height`

**Type:** Property (read-only)

**Description:** Nominal satellite height above Earth's surface (at the Equator)

**Returns:** Dictionary with height information

**Raises:** `KeyError` if height data is not found

**Usage:**
```python
height = gmc.satellite_height
h_value = height['values'].item()
h_units = height['attributes'].get('units', '')
print(f"Satellite height: {h_value:,.0f} {h_units}")
# Output: Satellite height: 35,786,023 m
```

---

#### `subpoint_longitude`

**Type:** Property (read-only)

**Description:** Longitude of the satellite subpoint (nadir point, where satellite is directly overhead)

**Returns:** Dictionary with longitude information

**Raises:** `KeyError` if subpoint data is not found

**Usage:**
```python
lon = gmc.subpoint_longitude
lon_value = lon['values'].item()
lon_units = lon['attributes'].get('units', '')
print(f"Subpoint longitude: {lon_value}° {lon_units}")
```

---

#### `subpoint_latitude`

**Type:** Property (read-only)

**Description:** Latitude of the satellite subpoint (typically 0° for geostationary satellites)

**Returns:** Dictionary with latitude information

**Raises:** `KeyError` if subpoint data is not found

**Usage:**
```python
lat = gmc.subpoint_latitude
lat_value = lat['values'].item()
lat_units = lat['attributes'].get('units', '')
print(f"Subpoint latitude: {lat_value}° {lat_units}")
```

---

#### `spatial_resolution`

**Type:** Property (read-only)

**Description:** Nominal spatial resolution at nadir

**Returns:** String (e.g., "2km at nadir")

**Usage:**
```python
resolution = gmc.spatial_resolution
print(f"Spatial resolution: {resolution}")
```

---

## Data Quality Properties

### `data_quality_metrics`

**Type:** Property (read-only)

**Description:** Transmission error percentages for data quality assessment

**Returns:** Dictionary with:
- `grb_errors`: Dictionary containing percentage of uncorrectable GRB (GOES Rebroadcast) errors
- `l0_errors`: Dictionary containing percentage of uncorrectable L0 (Level 0) errors

**Usage:**
```python
quality = gmc.data_quality_metrics

grb_errors = quality['grb_errors']
if grb_errors:
    grb_pct = grb_errors['values'].item()
    print(f"GRB transmission errors: {grb_pct:.4f}%")

l0_errors = quality['l0_errors']
if l0_errors:
    l0_pct = l0_errors['values'].item()
    print(f"L0 transmission errors: {l0_pct:.4f}%")
```

**Interpretation:**
- Values close to 0% indicate high-quality data transmission
- Higher percentages indicate data loss during satellite-to-ground communication
- These metrics help assess overall data reliability

---

### `source_data_files`

**Type:** Property (read-only)

**Description:** Information about the input L1b radiance files used to create this L2 product

**Returns:** Dictionary mapping attribute names to file patterns, or `None` if not available

**Usage:**
```python
source_files = gmc.source_data_files
if source_files:
    print("Source L1b files:")
    for key, pattern in source_files.items():
        if pattern and pattern != 'null':
            print(f"  {key}: {pattern}")
```

**Example Output:**
```
Source L1b files:
  input_ABI_L1b_radiance_band_2_half_km_data: OR_ABI-L1b-RADF-M6C02_G18_s20230022200205_e20230022209525_c*.nc
  input_ABI_L1b_radiance_band_8_2km_data: OR_ABI-L1b-RADF-M6C08_G18_s20230022200205_e20230022209525_c*.nc
  ...
```

---

## Complete Usage Examples

### Example 1: Basic Data Exploration

```python
from goesdatabuilder.data import GOESMultiCloud
import numpy as np

# Load the dataset
gmc = GOESMultiCloud('OR_ABI-L2-MCMIPF-M6_G18_s20230022200205_e20230022209525_c20230022210006.nc')

# Print object representation
print(gmc)
print()

# Dataset information
print("=" * 60)
print("DATASET INFORMATION")
print("=" * 60)
print(f"Dataset ID: {gmc.dataset_id}")
print(f"Filename: {gmc.dataset_name}")
print()

# Satellite information
print("=" * 60)
print("SATELLITE INFORMATION")
print("=" * 60)
sat_info = gmc.satellite_info
print(f"Platform: {sat_info['platform_id']}")
print(f"Orbital slot: {sat_info['orbital_slot']}")
print(f"Instrument: {sat_info['instrument_type']}")
print(f"Serial number: {sat_info['instrument_id']}")
print(f"Scan mode: {gmc.scan_mode}")
print(f"Scene: {gmc.scene_id}")
print()

# Temporal information
print("=" * 60)
print("TEMPORAL INFORMATION")
print("=" * 60)
temporal = gmc.temporal_info
print(f"Observation start: {temporal['start']}")
print(f"Observation end:   {temporal['end']}")
print(f"Product created:   {temporal['created']}")
print()

# Production information
print("=" * 60)
print("PRODUCTION INFORMATION")
print("=" * 60)
prod = gmc.production_info
print(f"Site: {prod['site']}")
print(f"Environment: {prod['environment']}")
print(f"Data source: {prod['data_source']}")
print(f"Processing level: {prod['processing_level']}")
print()

# Projection information
print("=" * 60)
print("PROJECTION INFORMATION")
print("=" * 60)
height = gmc.satellite_height
print(f"Satellite height: {height['values'].item():,.0f} {height['attributes'].get('units')}")
lon = gmc.subpoint_longitude
print(f"Subpoint longitude: {lon['values'].item()}°")
lat = gmc.subpoint_latitude
print(f"Subpoint latitude: {lat['values'].item()}°")
print(f"Spatial resolution: {gmc.spatial_resolution}")
print()

# Data quality
print("=" * 60)
print("DATA QUALITY METRICS")
print("=" * 60)
quality = gmc.data_quality_metrics
grb_pct = quality['grb_errors']['values'].item()
l0_pct = quality['l0_errors']['values'].item()
print(f"GRB transmission errors: {grb_pct:.6f}%")
print(f"L0 transmission errors: {l0_pct:.6f}%")
```

---

### Example 2: Multi-Band Analysis

```python
from goesdatabuilder.data import GOESMultiCloud
import numpy as np

# Load data
gmc = GOESMultiCloud('multi_cloud_data.nc')

# Analyze multiple bands
bands_to_analyze = [2, 5, 8, 14]  # Mix of reflectance and temperature bands

print("=" * 80)
print("MULTI-BAND ANALYSIS")
print("=" * 80)

for band_num in bands_to_analyze:
    gmc.band = band_num
    
    print(f"\n{'=' * 80}")
    print(f"BAND {band_num}")
    print(f"{'=' * 80}")
    
    # Band information
    wavelength = gmc.band_wavelength
    print(f"Central wavelength: {wavelength.values} {wavelength.attrs.get('units', '')}")
    
    # Get CMI data
    cmi = gmc.CMI
    data = cmi['values']
    attrs = cmi['attributes']
    
    print(f"Long name: {attrs.get('long_name', 'N/A')}")
    print(f"Data shape: {data.shape}")
    print(f"Data type: {data.dtype}")
    print(f"Units: {attrs.get('units', 'N/A')}")
    
    # Statistics from file
    stats = gmc.CMI_stats
    
    if band_num <= 6:  # Reflectance bands
        print(f"\nReflectance Statistics:")
        print(f"  Min:  {stats['min_reflectance']['values'].item():.4f}")
        print(f"  Max:  {stats['max_reflectance']['values'].item():.4f}")
        print(f"  Mean: {stats['mean_reflectance']['values'].item():.4f}")
        print(f"  Std:  {stats['std_reflectance']['values'].item():.4f}")
    else:  # Temperature bands
        print(f"\nBrightness Temperature Statistics:")
        print(f"  Min:  {stats['min_brightness_temp']['values'].item():.2f} K")
        print(f"  Max:  {stats['max_brightness_temp']['values'].item():.2f} K")
        print(f"  Mean: {stats['mean_brightness_temp']['values'].item():.2f} K")
        print(f"  Std:  {stats['std_brightness_temp']['values'].item():.2f} K")
    
    print(f"  Outliers: {stats['outlier_count']['values'].item():.0f} pixels")
    
    # Quality flags
    dqf = gmc.DQF
    quality_flags = dqf['values']
    
    # Remove NaN values before counting
    valid_flags = quality_flags[~np.isnan(quality_flags)]
    unique_flags, counts = np.unique(valid_flags, return_counts=True)
    
    print(f"\nData Quality Flag Distribution:")
    total_valid = counts.sum()
    for flag, count in zip(unique_flags, counts):
        pct = 100 * count / total_valid
        quality_desc = {
            0: "Good",
            1: "Conditionally usable",
            2: "Out of range",
            3: "No value",
            4: "Temp threshold exceeded"
        }.get(int(flag), "Unknown")
        print(f"  Flag {int(flag)} ({quality_desc}): {count:,} pixels ({pct:.2f}%)")
    
    # Count NaN pixels
    nan_count = np.isnan(quality_flags).sum()
    if nan_count > 0:
        nan_pct = 100 * nan_count / quality_flags.size
        print(f"  NaN (space/missing): {nan_count:,} pixels ({nan_pct:.2f}%)")
```

---

Error Handling

The class includes comprehensive error handling:

### ValueError
Raised when setting invalid band numbers:
```python
gmc.band = 20  # ValueError: Band must be 1-16, got 20
```

### KeyError
Raised when essential projection/satellite data is missing, or when a selected band's data is not found:
```python
height = gmc.satellite_height  # KeyError if data not found

gmc.band = 5
cmi = gmc.CMI  # KeyError if CMI_C05 not in dataset
dqf = gmc.DQF  # KeyError if DQF_C05 not in dataset
```

### None Returns
Band-dependent properties return `None` when no band is selected:
```python
gmc = GOESMultiCloud('file.nc')
cmi = gmc.CMI  # Returns None (no band set)

gmc.band = 8
cmi = gmc.CMI  # Returns dictionary with data
```

---

## Performance Notes

### Memory Considerations
- All data is loaded into memory during initialization for fast access
- A typical Full Disk multi-cloud file (~400 MB on disk) will use similar RAM
- Each band's CMI data is a 2D array (typical Full Disk: 5424 x 5424 pixels)
- Consider available RAM when processing multiple files simultaneously

### Access Patterns
- Properties perform lightweight dictionary lookups (O(1) complexity)
- No redundant file I/O after initialization
- Band switching is instantaneous (no data reloading)

### Best Practices
```python
# Efficient: Set band once, access multiple properties
gmc.band = 8
cmi = gmc.CMI
dqf = gmc.DQF
stats = gmc.CMI_stats
wavelength = gmc.band_wavelength

# Less efficient but acceptable: Multiple band switches
for band in [2, 8, 14]:
    gmc.band = band
    process_band(gmc.CMI, gmc.DQF)
```

---

## Data Organization

The class organizes NetCDF data into three internal structures:

1. **`self.coordinates`** - Dictionary of coordinate arrays (x, y, time, wavelength, etc.)
2. **`self.variables`** - Dictionary of data variables, each containing:
   - `values`: NumPy array of the data
   - `attributes`: Dictionary of variable metadata
3. **`self.attributes`** - Dictionary of global file attributes

Properties provide convenient, documented access to this structure without requiring knowledge of CF conventions or NetCDF internals.

---

## CF Compliance

This class works with CF-1.7 compliant GOES ABI L2+ multi-cloud products that follow:
- CF-1.7 (Climate and Forecast metadata conventions)
- Unidata Dataset Discovery v1.0
- GOES-R Series Product Definition and Users' Guide (PUG) Volume 5

---

## Additional Resources

- **GOES-R Resources:** [GOES-R Documentation](https://www.goes-r.gov/resources/docs.html)
- **GOES-R Product Definitions:** [Product Overview](https://www.goes-r.gov/products/overview.html)
- **GOES-R Series ABI Reprocessed L1b Product User Guide:** [Download PDF](https://www.ospo.noaa.gov/resources/documents/PDFs/GOES-R_ABI_Reprocessed_L1b_User_Guide-v1.1.pdf)
- **Product Users' Guide:** [GOES-R Series Product Definition and Users' Guide Volume 5 (L2+ Products)](https://www.ospo.noaa.gov/resources/documents/PUG/GS%20Series%20416-R-PUG-L2%20Plus-0349%20Vol%205%20v3.0%20final.pdf)
- **AWS Open Data:** [NOAA GOES on AWS](https://registry.opendata.aws/noaa-goes/)
---

## Version History

- **v1.0.0** - Initial release with comprehensive property-based interface
- Supports all GOES-16, GOES-17, GOES-18 ABI L2 MCMIP products
- Compatible with Full Disk, CONUS, and Mesoscale scenes