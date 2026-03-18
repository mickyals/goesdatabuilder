"""
GOES ABI Multi-Cloud Constants and Metadata

This module contains constants, metadata, and validation patterns for GOES ABI L2+ data
processing in the multi-cloud pipeline. It defines band characteristics, quality flags,
filename patterns, and CF-compliant metadata mappings.

Key Components:
- Band metadata (wavelength, units, valid ranges)
- Quality flag definitions and meanings
- Filename parsing patterns
- Valid platform/scene/orbital configurations
- CF attribute name mappings
"""

import re

##################################################################################################################
########################### METADATA ATTRIBUTE MAPPINGS ########################################################
##################################################################################################################

# CF-compliant attribute name mappings for GOES ABI data
# Maps GOES-specific attribute names to standard CF/ACDD conventions

PROMOTED_ATTRS = {
        # Identity
        'id': 'observation_id',
        'dataset_name': 'dataset_name',
        'naming_authority': 'naming_authority',

        # Satellite/Instrument
        'platform_ID': 'platform_id',
        'orbital_slot': 'orbital_slot',
        'instrument_type': 'instrument_type',
        'instrument_ID': 'instrument_id',

        # Scene
        'scene_id': 'scene_id',
        'timeline_id': 'scan_mode',
        'spatial_resolution': 'spatial_resolution',

        # Temporal
        'time_coverage_start': 'time_coverage_start',
        'time_coverage_end': 'time_coverage_end',
        'date_created': 'date_created',

        # Production
        'production_site': 'production_site',
        'production_environment': 'production_environment',
        'production_data_source': 'production_data_source',
        'processing_level': 'processing_level',

        # Standards
        'Conventions': 'conventions',
        'Metadata_Conventions': 'metadata_conventions',
        'standard_name_vocabulary': 'standard_name_vocabulary',

        # Documentation
        'title': 'title',
        'summary': 'summary',
        'institution': 'institution',
        'project': 'project',
        'license': 'license',
        'keywords': 'keywords',
        'keywords_vocabulary': 'keywords_vocabulary',
        'cdm_data_type': 'cdm_data_type',
        'iso_series_metadata_id': 'iso_series_metadata_id',
    }

##################################################################################################################
########################### VALIDATION CONSTANTS ############################################################
##################################################################################################################

# Valid values for GOES ABI platform, orbital slot, and scene configurations
# Used for input validation and error messaging

VALID_ORBITAL_SLOTS = {'GOES-East', 'GOES-West', 'GOES-Test', 'GOES-Storage'}
VALID_PLATFORMS = {'G16', 'G17', 'G18', 'G19'}
VALID_SCENE_IDS = {'Full Disk', 'CONUS', 'Mesoscale'}

# Regular expression pattern for parsing GOES ABI L2+ Multi-Cloud MCMIP filenames
#
# Format: OR_ABI-L2-MCMIP{scene}-M{mode}_G{satellite}_s{start}_e{end}_c{created}.nc
# Where:
#   - scene: F (Full Disk), C (CONUS), M (Mesoscale)
#   - mode: ABI scan mode (3, 4, 6)
#   - satellite: GOES satellite number (16, 17, 18, 19)
#   - start/end: observation start/end timestamps (YYYYMMDDHHMMSS)
#   - created: file creation timestamp (YYYYMMDDHHMMSS)
#
# Examples:
#   OR_ABI-L2-MCMIPF-M6_G18_s20240030200212_e20240030209521_c20240030210015.nc
#   OR_ABI-L2-MCMIPC-M3_G16_s20240301150000_e20240301155959_c20240301163015.nc
GOES_FILENAME_PATTERN = re.compile(
    r'OR_ABI-L2-MCMIP(?P<scene>[FCM])-M(?P<mode>\d)_G(?P<satellite>\d{2})_s(?P<start>\d{14})_e(?P<end>\d{14})_c(?P<created>\d{14})\.nc'
)

# ABI band classification by measurement type
# Reflectance bands (1-6): Solar reflected radiation, units are dimensionless (0-1)
# Brightness temperature bands (7-16): Thermal emission, units are Kelvin
REFLECTANCE_BANDS = list(range(1, 7))
BRIGHTNESS_TEMP_BANDS = list(range(7, 17))
ALL_BANDS = REFLECTANCE_BANDS + BRIGHTNESS_TEMP_BANDS


# Default band metadata for all 16 ABI bands
# Used as fallback when configuration doesn't specify band-specific metadata
# Each band includes:
#   - wavelength: Central wavelength in micrometers (μm)
#   - long_name: Descriptive name following GOES ABI conventions
#   - standard_name: CF standard name for the variable
#   - units: Physical units (dimensionless for reflectance, K for temperature)
#   - valid_range: Expected data range for validation
#
# Band Categories:
#   1-6: Reflectance bands (solar reflected radiation)
#   7-16: Brightness temperature bands (thermal emission)

DEFAULT_BAND_METADATA = {
1: {'wavelength': 0.47, 'long_name': 'ABI Cloud and Moisture Imagery reflectance factor - Blue',
    'standard_name': 'toa_bidirectional_reflectance', 'units': '1', 'valid_range': [0.0, 1.0]},
2: {'wavelength': 0.64, 'long_name': 'ABI Cloud and Moisture Imagery reflectance factor - Red',
    'standard_name': 'toa_bidirectional_reflectance', 'units': '1', 'valid_range': [0.0, 1.0]},
3: {'wavelength': 0.86, 'long_name': 'ABI Cloud and Moisture Imagery reflectance factor - Veggie',
    'standard_name': 'toa_bidirectional_reflectance', 'units': '1', 'valid_range': [0.0, 1.0]},
4: {'wavelength': 1.37, 'long_name': 'ABI Cloud and Moisture Imagery reflectance factor - Cirrus',
    'standard_name': 'toa_bidirectional_reflectance', 'units': '1', 'valid_range': [0.0, 1.0]},
5: {'wavelength': 1.61, 'long_name': 'ABI Cloud and Moisture Imagery reflectance factor - Snow/Ice',
    'standard_name': 'toa_bidirectional_reflectance', 'units': '1', 'valid_range': [0.0, 1.0]},
6: {'wavelength': 2.24, 'long_name': 'ABI Cloud and Moisture Imagery reflectance factor - Cloud Particle Size',
    'standard_name': 'toa_bidirectional_reflectance', 'units': '1', 'valid_range': [0.0, 1.0]},
7: {'wavelength': 3.90,
    'long_name': 'ABI Cloud and Moisture Imagery brightness temperature at top of atmosphere - Shortwave Window',
    'standard_name': 'toa_brightness_temperature', 'units': 'K', 'valid_range': [197.30, 411.86]},
8: {'wavelength': 6.19,
    'long_name': 'ABI Cloud and Moisture Imagery brightness temperature at top of atmosphere - Upper-Level Water Vapor',
    'standard_name': 'toa_brightness_temperature', 'units': 'K', 'valid_range': [138.05, 311.06]},
9: {'wavelength': 6.93,
    'long_name': 'ABI Cloud and Moisture Imagery brightness temperature at top of atmosphere - Mid-Level Water Vapor',
    'standard_name': 'toa_brightness_temperature', 'units': 'K', 'valid_range': [137.7 , 311.08]},
10: {'wavelength': 7.34,
     'long_name': 'ABI Cloud and Moisture Imagery brightness temperature at top of atmosphere - Lower-Level Water Vapor',
     'standard_name': 'toa_brightness_temperature', 'units': 'K', 'valid_range': [126.91, 331.2]},
11: {'wavelength': 8.44,
     'long_name': 'ABI Cloud and Moisture Imagery brightness temperature at top of atmosphere - Cloud-Top Phase',
     'standard_name': 'toa_brightness_temperature', 'units': 'K', 'valid_range': [127.69, 341.3]},
12: {'wavelength': 9.61,
     'long_name': 'ABI Cloud and Moisture Imagery brightness temperature at top of atmosphere - Ozone',
     'standard_name': 'toa_brightness_temperature', 'units': 'K', 'valid_range': [117.49, 311.06]},
13: {'wavelength': 10.33,
     'long_name': 'ABI Cloud and Moisture Imagery brightness temperature at top of atmosphere - Clean Longwave Window',
     'standard_name': 'toa_brightness_temperature', 'units': 'K', 'valid_range': [ 89.62, 341.27]},
14: {'wavelength': 11.21,
     'long_name': 'ABI Cloud and Moisture Imagery brightness temperature at top of atmosphere - Longwave Window',
     'standard_name': 'toa_brightness_temperature', 'units': 'K', 'valid_range': [ 96.19, 341.28]},
15: {'wavelength': 12.29,
     'long_name': 'ABI Cloud and Moisture Imagery brightness temperature at top of atmosphere - Dirty Longwave Window',
     'standard_name': 'toa_brightness_temperature', 'units': 'K', 'valid_range': [ 97.38, 341.28]},
16: {'wavelength': 13.28,
     'long_name': 'ABI Cloud and Moisture Imagery brightness temperature at top of atmosphere - CO2 Longwave',
     'standard_name': 'toa_brightness_temperature', 'units': 'K', 'valid_range': [ 92.7 , 318.26]},
}


##################################################################################################################
########################### REGRID QUALITY FLAGS #########################################################
##################################################################################################################

# Extended Data Quality Flag (DQF) definitions for regridded GOES ABI data
#
# The original GOES ABI L2 product defines DQF values 0-4. This module extends
# those definitions to support quality tracking in the regridding pipeline:
#
# Original Flags (0-4):
#   0: GOOD - Good quality pixels
#   1: CONDITIONALLY_USABLE - Usable with conditions
#   2: OUT_OF_RANGE - Values outside expected range
#   3: NO_VALUE - No valid source data (outside convex hull)
#   4: FOCAL_PLANE_TEMP_EXCEEDED - Sensor temperature issue
#
# Extended Flags (5-6):
#   5: INTERPOLATED - Mixed quality from barycentric interpolation
#   6: NAN_SOURCE - NaN values in source data within convex hull
#
# Interpolation Quality Handling:
#   - Direct hit (weight ≥ 0.999): Preserve original DQF
#   - Mixed sources: Set DQF = 5 (INTERPOLATED)
#   - NaN in convex hull: Set DQF = 6 (NAN_SOURCE)
DQF_FLAGS = {
    0: {"name": "GOOD", "meaning": "good_pixels_qf"},
    1: {"name": "CONDITIONALLY_USABLE", "meaning": "conditionally_usable_pixels_qf"},
    2: {"name": "OUT_OF_RANGE", "meaning": "out_of_range_pixels_qf"},
    3: {"name": "NO_VALUE", "meaning": "no_value_pixels_qf"},
    4: {"name": "FOCAL_PLANE_TEMP_EXCEEDED", "meaning": "focal_plane_temperature_threshold_exceeded_qf"},
    # Extended flags (not in original ABI L2 DQF spec, added for regridding pipeline)
    5: {"name": "INTERPOLATED", "meaning": "interpolated_qf"},
    6: {"name": "NAN_SOURCE", "meaning": "nan_source"},
}

# Named DQF flag constants (integer values matching DQF_FLAGS keys)
DQF_GOOD = 0
DQF_CONDITIONALLY_USABLE = 1
DQF_OUT_OF_RANGE = 2
DQF_NO_VALUE = 3
DQF_FOCAL_PLANE_TEMP_EXCEEDED = 4
DQF_INTERPOLATED = 5
DQF_NAN_SOURCE = 6