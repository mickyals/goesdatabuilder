# MultiCloud Constants

## Overview

The `multicloudconstants.py` module defines essential constants, mappings, and validation patterns used throughout the GOES data processing pipeline. It provides standardized attribute mappings, validation sets, and filename parsing patterns for GOES ABI L2+ data files.

## Purpose

This module serves as the central configuration hub for:
- NetCDF attribute to DataFrame column mappings
- Validation rules for GOES metadata
- Filename pattern parsing and validation
- Standardized naming conventions

## Constants

### PROMOTED_ATTRS

A comprehensive mapping dictionary that translates NetCDF global attributes to standardized DataFrame column names. This mapping ensures consistent naming across the catalog and facilitates downstream processing.

#### Structure

```python
PROMOTED_ATTRS = {
    # NetCDF attribute: DataFrame column name
}
```

#### Attribute Categories

**Identity Attributes:**
- `id` → `observation_id`: Unique observation identifier
- `dataset_name` → `dataset_name`: Name of the dataset
- `naming_authority` → `naming_authority`: Organization responsible for naming

**Satellite/Instrument Attributes:**
- `platform_ID` → `platform_id`: GOES satellite identifier (G16, G17, G18, G19)
- `orbital_slot` → `orbital_slot`: Orbital position (GOES-East, GOES-West, etc.)
- `instrument_type` → `instrument_type`: Instrument classification
- `instrument_ID` → `instrument_id`: Unique instrument identifier

**Scene Attributes:**
- `scene_id` → `scene_id`: Scene type (Full Disk, CONUS, Mesoscale)
- `timeline_id` → `scan_mode`: Scanning mode/timeline
- `spatial_resolution` → `spatial_resolution`: Spatial resolution information

**Temporal Attributes:**
- `time_coverage_start` → `time_coverage_start`: Observation start time
- `time_coverage_end` → `time_coverage_end`: Observation end time
- `date_created` → `date_created`: File creation timestamp

**Production Attributes:**
- `production_site` → `production_site`: Processing location
- `production_environment` → `production_environment`: Processing environment
- `production_data_source` → `production_data_source`: Source data information
- `processing_level` → `processing_level`: Data processing level

**Standards Attributes:**
- `Conventions` → `conventions`: CF/ACDD conventions used
- `Metadata_Conventions` → `metadata_conventions`: Metadata standards
- `standard_name_vocabulary` → `standard_name_vocabulary`: Variable naming standards

**Documentation Attributes:**
- `title` → `title`: Dataset title
- `summary` → `summary`: Dataset description
- `institution` → `institution`: Responsible institution
- `project` → `project`: Associated project
- `license` → `license`: Data license information
- `keywords` → `keywords`: Search keywords
- `keywords_vocabulary` → `keywords_vocabulary`: Keyword standard
- `cdm_data_type` → `cdm_data_type`: Common Data Model type
- `iso_series_metadata_id` → `iso_series_metadata_id`: ISO metadata identifier

### Validation Sets

#### VALID_ORBITAL_SLOTS
```python
{'GOES-East', 'GOES-West', 'GOES-Test', 'GOES-Storage'}
```

Valid orbital slot designations for GOES satellites:
- **GOES-East**: Operational eastern satellite (75°W)
- **GOES-West**: Operational western satellite (137°W)
- **GOES-Test**: Test/backup satellite
- **GOES-Storage**: In-storage/standby satellite

#### VALID_PLATFORMS
```python
{'G16', 'G17', 'G18', 'G19'}
```

Valid GOES satellite platform identifiers:
- **G16**: GOES-16 (launched 2016)
- **G17**: GOES-17 (launched 2018)
- **G18**: GOES-18 (launched 2022)
- **G19**: GOES-19 (future/planned)

#### VALID_SCENE_IDS
```python
{'Full Disk', 'CONUS', 'Mesoscale'}
```

Valid scene types for GOES ABI:
- **Full Disk**: Full Earth disk image
- **CONUS**: Continental United States
- **Mesoscale**: Regional/mesoscale sectors

### Filename Pattern

#### GOES_FILENAME_PATTERN

A compiled regular expression for parsing and validating GOES ABI L2+ filenames.

```python
GOES_FILENAME_PATTERN = re.compile(
    r'OR_ABI-L2-MCMIP(?P<scene>[FCM])-M(?P<mode>\d)_G(?P<satellite>\d{2})_s(?P<start>\d{14})_e(?P<end>\d{14})_c(?P<created>\d{14})\.nc'
)
```

#### Pattern Components

**Base Structure:**
```
OR_ABI-L2-MCMIP{scene}-M{mode}_G{satellite}_s{start}_e{end}_c{created}.nc
```

**Named Groups:**
- `scene`: Scene type code (F=Full Disk, C=CONUS, M=Mesoscale)
- `mode`: Scan mode number (1-6)
- `satellite`: Satellite number (16, 17, 18, 19)
- `start`: Observation start time (YYYYMMDDHHMMSS)
- `end`: Observation end time (YYYYMMDDHHMMSS)
- `created`: File creation time (YYYYMMDDHHMMSS)

#### Filename Examples

**Full Disk:**
```
OR_ABI-L2-MCMIPF-M6_G18_s20240101120000_e20240101125959_c20240101130115.nc
```

**CONUS:**
```
OR_ABI-L2-MCMIPC-M3_G17_s20240101120000_e20240101125959_c20240101130115.nc
```

**Mesoscale:**
```
OR_ABI-L2-MCMIPM-M2_G16_s20240101120000_e20240101125959_c20240101130115.nc
```

## Usage Examples

### Attribute Mapping

```python
from multicloudconstants import PROMOTED_ATTRS

# Map NetCDF attribute to DataFrame column
ncdf_attr = 'platform_ID'
df_column = PROMOTED_ATTRS[ncdf_attr]  # 'platform_id'

# Reverse lookup (find attribute for column)
reverse_map = {v: k for k, v in PROMOTED_ATTRS.items()}
attr_name = reverse_map['platform_id']  # 'platform_ID'
```

### Validation

```python
from multicloudconstants import VALID_PLATFORMS, VALID_ORBITAL_SLOTS, VALID_SCENE_IDS

# Validate platform
platform = 'G18'
if platform in VALID_PLATFORMS:
    print(f"Valid platform: {platform}")

# Validate orbital slot
orbital_slot = 'GOES-East'
if orbital_slot in VALID_ORBITAL_SLOTS:
    print(f"Valid orbital slot: {orbital_slot}")

# Validate scene ID
scene_id = 'Full Disk'
if scene_id in VALID_SCENE_IDS:
    print(f"Valid scene: {scene_id}")
```

### Filename Parsing

```python
from multicloudconstants import GOES_FILENAME_PATTERN

filename = 'OR_ABI-L2-MCMIPF-M6_G18_s20240101120000_e20240101125959_c20240101130115.nc'

match = GOES_FILENAME_PATTERN.match(filename)
if match:
    scene_code = match.group('scene')  # 'F'
    mode = match.group('mode')          # '6'
    satellite = match.group('satellite') # '18'
    start_time = match.group('start')   # '20240101120000'
    end_time = match.group('end')       # '20240101125959'
    created_time = match.group('created') # '20240101130115'
    
    # Convert scene code to full name
    scene_map = {'F': 'Full Disk', 'C': 'CONUS', 'M': 'Mesoscale'}
    scene_name = scene_map[scene_code]
```

### Integration with GOESMetadataCatalog

```python
from multicloudconstants import PROMOTED_ATTRS, VALID_PLATFORMS, GOES_FILENAME_PATTERN
from goesdatabuilder.data.goes.multicloudcatalog import GOESMetadataCatalog

# Constants are used internally by GOESMetadataCatalog
catalog = GOESMetadataCatalog(output_dir='./catalog')

# The catalog uses these constants for:
# 1. Attribute mapping in _extract_global_attrs()
# 2. Validation in _validate_orbital_consistency()
# 3. Filename validation in _validate_file()
```

## Design Principles

### Standardization
- Consistent naming conventions across the entire pipeline
- CF/ACDD compliance for metadata standards
- ISO-compliant timestamp formats

### Validation
- Comprehensive validation sets prevent invalid data processing
- Filename pattern matching ensures file integrity
- Type-safe constant definitions

### Extensibility
- Easy to add new platforms or orbital slots
- Flexible attribute mapping system
- Modular design for future enhancements

## Dependencies

- **re**: Regular expression operations for filename pattern matching
- **typing**: Type hints (if used in future versions)

## Version Information

- **Module**: multicloudconstants.py
- **Purpose**: GOES data processing constants and validation
- **Maintainer**: GOES Data Builder Team
- **Compatibility**: GOES ABI L2+ data products

## Related Modules

- `multicloudcatalog.py`: Uses these constants for metadata extraction and validation
- `multicloud.py`: May reference these constants for data processing
- Other GOES data processing modules in the pipeline

## Best Practices

1. **Import Constants**: Always import from this module rather than hardcoding values
2. **Validation**: Use validation sets before processing data
3. **Pattern Matching**: Use the compiled regex pattern for filename validation
4. **Attribute Mapping**: Use PROMOTED_ATTRS for consistent column naming
5. **Extensibility**: Add new constants here rather than scattering them across modules

## Future Enhancements

Potential areas for expansion:
- Additional validation sets for new GOES satellites
- Extended filename patterns for new data products
- Additional attribute mappings for new metadata fields
- Configuration file support for dynamic constants
