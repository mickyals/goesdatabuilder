import re


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

VALID_ORBITAL_SLOTS = {'GOES-East', 'GOES-West', 'GOES-Test', 'GOES-Storage'}
VALID_PLATFORMS = {'G16', 'G17', 'G18', 'G19'}
VALID_SCENE_IDS = {'Full Disk', 'CONUS', 'Mesoscale'}

# GOES filename pattern:  OR_ABI-L2-MCMIPF-M6_G18_s20240030200212_e20240030209521_c20240030210015.nc
# CONUS - MCMIPC
# Mesoscale - MCMIPM
# FullDisk - MCMIPF
GOES_FILENAME_PATTERN = re.compile(
    r'OR_ABI-L2-MCMIP(?P<scene>[FCM])-M(?P<mode>\d)_G(?P<satellite>\d{2})_s(?P<start>\d{14})_e(?P<end>\d{14})_c(?P<created>\d{14})\.nc'
)