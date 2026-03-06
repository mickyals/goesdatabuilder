# MultiCloud Constants

## Overview

The `multicloudconstants.py` module defines constants, metadata, and validation patterns used throughout the GOES ABI L2+ data processing pipeline. It serves as the single source of truth for band characteristics, quality flags, filename patterns, validation sets, and CF-compliant metadata mappings.

## Contents

- `PROMOTED_ATTRS`: NetCDF attribute to variable name mappings
- `VALID_ORBITAL_SLOTS`, `VALID_PLATFORMS`, `VALID_SCENE_IDS`: Validation sets
- `GOES_FILENAME_PATTERN`: Compiled regex for filename parsing
- `REFLECTANCE_BANDS`, `BRIGHTNESS_TEMP_BANDS`, `ALL_BANDS`: Band classification lists
- `DEFAULT_BAND_METADATA`: Per-band metadata for all 16 ABI bands
- `REGIONS`: Supported platform region identifiers
- `DQF_FLAGS`: Extended data quality flag definitions (dict, flags 0-6)
- `DQF_GOOD` through `DQF_NAN_SOURCE`: Named integer constants for flag values

## Attribute Mappings

### PROMOTED_ATTRS

Maps NetCDF global attributes to standardized variable names. Used by `GOESMultiCloudObservation` to promote file-level attributes into time-indexed variables for proper multi-file concatenation and provenance tracking.

```python
PROMOTED_ATTRS = {
    # NetCDF attribute    ->   Variable name
    'id'                  :    'observation_id',
    'dataset_name'        :    'dataset_name',
    'platform_ID'         :    'platform_id',
    'orbital_slot'        :    'orbital_slot',
    'timeline_id'         :    'scan_mode',
    'time_coverage_start' :    'time_coverage_start',
    'time_coverage_end'   :    'time_coverage_end',
    # ... (30 total mappings across identity, satellite, scene,
    #      temporal, production, standards, and documentation categories)
}
```

Categories: identity (3), satellite/instrument (4), scene/mode (3), temporal (3), production (4), standards (3), documentation (10).

## Validation Sets

### VALID_ORBITAL_SLOTS

```python
{'GOES-East', 'GOES-West', 'GOES-Test', 'GOES-Storage'}
```

- **GOES-East**: Operational eastern satellite (75.2 W)
- **GOES-West**: Operational western satellite (137.2 W)
- **GOES-Test**: Test/checkout position (89.5 W)
- **GOES-Storage**: In-orbit storage

### VALID_PLATFORMS

```python
{'G16', 'G17', 'G18', 'G19'}
```

GOES-R series satellite identifiers. G16 launched 2016, G17 2018, G18 2022, G19 2024.

### VALID_SCENE_IDS

```python
{'Full Disk', 'CONUS', 'Mesoscale'}
```

ABI scan region types: full Earth disk, continental US, or regional mesoscale sectors.

## Filename Pattern

### GOES_FILENAME_PATTERN

Compiled regex for parsing GOES ABI L2+ MCMIP filenames.

```
OR_ABI-L2-MCMIP{scene}-M{mode}_G{satellite}_s{start}_e{end}_c{created}.nc
```

Named groups: `scene` (F/C/M), `mode` (scan mode digit), `satellite` (2-digit number), `start`/`end`/`created` (14-digit timestamps as YYYYDDDHHMMSSt where DDD is day-of-year and t is tenths of second).

Example:
```
OR_ABI-L2-MCMIPF-M6_G18_s20240030200212_e20240030209521_c20240030210015.nc
```

## Band Classification

### REFLECTANCE_BANDS

```python
[1, 2, 3, 4, 5, 6]
```

Solar reflected radiation bands. Dimensionless reflectance factor (0-1). Standard name: `toa_bidirectional_reflectance`.

### BRIGHTNESS_TEMP_BANDS

```python
[7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
```

Thermal emission bands. Units: Kelvin. Standard name: `toa_brightness_temperature`.

### BANDS

```python
[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
```

Combined list of all 16 ABI bands (`REFLECTANCE_BANDS + BRIGHTNESS_TEMP_BANDS`). Used by `GOESPipelineOrchestrator._get_default_bands` as the fallback when the store config does not specify a band list.

## Band Metadata

### DEFAULT_BAND_METADATA

Default metadata for all 16 ABI bands, used as fallback when config does not provide band-specific overrides. Keyed by band number (int).

Each entry contains:
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

## Regions

### REGIONS

```python
['GOES-East', 'GOES-West', 'GOES-Test', 'GOES-Storage']
```

Supported platform region identifiers for the Zarr store hierarchy. Used by `GOESZarrStore` to define top-level groups in the store structure and validate region parameters.

## Quality Flags

### DQF_FLAGS

Extended Data Quality Flag definitions for regridded GOES ABI data. The original ABI L2 product defines flags 0-4. Flags 5-6 are added by the regridding pipeline to track interpolation quality.

```python
DQF_FLAGS = {
    0: {"name": "GOOD",                       "meaning": "good_pixels_qf"},
    1: {"name": "CONDITIONALLY_USABLE",        "meaning": "conditionally_usable_pixels_qf"},
    2: {"name": "OUT_OF_RANGE",                "meaning": "out_of_range_pixels_qf"},
    3: {"name": "NO_VALUE",                    "meaning": "no_value_pixels_qf"},
    4: {"name": "FOCAL_PLANE_TEMP_EXCEEDED",   "meaning": "focal_plane_temperature_threshold_exceeded_qf"},
    5: {"name": "INTERPOLATED",                "meaning": "interpolated_qf"},
    6: {"name": "NAN_SOURCE",                  "meaning": "nan_source"},
}
```

**Original flags (0-4):** Preserved from source GOES ABI L2 DQF arrays.

**Extended flags (5-6):** Assigned during barycentric interpolation in `GeostationaryRegridder`:
- Flag 5 (INTERPOLATED): Target pixel was interpolated from source pixels with mixed quality flags.
- Flag 6 (NAN_SOURCE): Some source pixels within the interpolation triangle contained NaN values.

**Assignment logic in regridder:**
- Direct hit (barycentric weight >= 0.999): Preserve original DQF from nearest source pixel
- Interpolated from mixed sources: Set DQF = 5
- NaN detected in source hull: Set DQF = 6
- Outside convex hull: Set DQF = 3

Used by `GOESZarrStore._cf_dqf_attrs()` and `GeostationaryRegridder.dqf_attrs()` to generate CF-compliant `flag_values`, `flag_meanings`, and `valid_range` attributes.

### Named DQF Constants

Integer constants matching `DQF_FLAGS` keys, provided for readable access across modules without dict lookups:

```python
DQF_GOOD = 0
DQF_CONDITIONALLY_USABLE = 1
DQF_OUT_OF_RANGE = 2
DQF_NO_VALUE = 3
DQF_FOCAL_PLANE_TEMP_EXCEEDED = 4
DQF_INTERPOLATED = 5
DQF_NAN_SOURCE = 6
```

These are the canonical way to reference DQF values in code. `GeostationaryRegridder._classify_dqf_2d` uses `multicloudconstants.DQF_NO_VALUE`, `multicloudconstants.DQF_INTERPOLATED`, and `multicloudconstants.DQF_NAN_SOURCE` directly.

## Usage

### Importing

```python
from goesdatabuilder.data.goes import multicloudconstants

# Named DQF constants
fill_value = multicloudconstants.DQF_NO_VALUE
interp_flag = multicloudconstants.DQF_INTERPOLATED
nan_flag = multicloudconstants.DQF_NAN_SOURCE

# Or import specific items
from goesdatabuilder.data.goes.multicloudconstants import (
    PROMOTED_ATTRS, DQF_FLAGS, DEFAULT_BAND_METADATA,
    REGIONS, REFLECTANCE_BANDS, BRIGHTNESS_TEMP_BANDS, ALL_BANDS,
    GOES_FILENAME_PATTERN,
    VALID_PLATFORMS, VALID_ORBITAL_SLOTS,
    DQF_GOOD, DQF_NO_VALUE, DQF_INTERPOLATED, DQF_NAN_SOURCE,
)
```

### Validation

```python
if platform_id not in multicloudconstants.VALID_PLATFORMS:
    raise ValueError(f"Invalid platform: {platform_id}")

if region not in multicloudconstants.REGIONS:
    raise ValueError(f"Invalid region: {region}")
```

### Filename Parsing

```python
match = multicloudconstants.GOES_FILENAME_PATTERN.match(filename)
if match:
    satellite = match.group('satellite')
    start_time = match.group('start')
    scene_code = match.group('scene')
```

### DQF Attributes for CF Metadata

```python
flag_values = list(multicloudconstants.DQF_FLAGS.keys())
flag_meanings = " ".join(
    v["meaning"] for v in multicloudconstants.DQF_FLAGS.values()
)
valid_range = [min(multicloudconstants.DQF_FLAGS), max(multicloudconstants.DQF_FLAGS)]
```

### Band Metadata

```python
band_meta = multicloudconstants.DEFAULT_BAND_METADATA[7]
# {'wavelength': 3.90, 'long_name': '...Shortwave Window',
#  'standard_name': 'toa_brightness_temperature', 'units': 'K',
#  'valid_range': [197.30, 411.86]}

is_reflectance = band in multicloudconstants.REFLECTANCE_BANDS
```

## Consumers

- **GOESMultiCloudObservation**: Uses `PROMOTED_ATTRS`, `VALID_ORBITAL_SLOTS`, `GOES_FILENAME_PATTERN` for config validation and preprocessing
- **GOESMetadataCatalog**: Uses `PROMOTED_ATTRS`, `VALID_PLATFORMS`, `VALID_ORBITAL_SLOTS`, `VALID_SCENE_IDS`, `GOES_FILENAME_PATTERN` for metadata extraction, file scanning, and orbital consistency validation
- **GOESZarrStore**: Uses `REGIONS`, `DEFAULT_BAND_METADATA`, `DQF_FLAGS`, `REFLECTANCE_BANDS` for store initialization and CF metadata generation
- **GeostationaryRegridder**: Uses `DQF_NO_VALUE`, `DQF_INTERPOLATED`, `DQF_NAN_SOURCE` for quality flag assignment during regridding, and `DQF_FLAGS` via `dqf_attrs()` for CF metadata
- **GOESPipelineOrchestrator**: Uses `BANDS` as default band list fallback, `VALID_ORBITAL_SLOTS` for region validation

## Dependencies

- `re`: Regular expression compilation for `GOES_FILENAME_PATTERN`