


class GOESSatellite:
    """
    A class to represent a GOES satellite
    """

    def __init__(self):
        """
        Initialize the GOESSatellite object

        The object will contain the attributes and variables that are
        present in the GOES MCMIPF data.
        """

        # List of Global attributes from GOES MCMIPF Data

        # Identification of file attributes
        _ID_DYNAMIC_ATTRS = ['dataset_name', 'id']
        # dataset_name follows the naming convention of ABI products, therefore each dataset has a unique dataset_name
        # id is the unique identifier of the dataset

        _ID_STATIC_ATTRS = ['naming_authority', 'project', 'iso_series_metadata_id', 'title',  'summary']

        # Identification of production attributes
        _PROD_DYNAMIC_ATTRS = ['production_environment'] # varies from OE or DE (operational or development environment)

        _PROD_STATIC_ATTRS = ['institution', 'production_site', 'platform_ID', 'processing_level', 'production_data_source']


        # Instrument attributes
        _INSTRUMENT_DYNAMIC_ATTRS = ['orbital_slot'] # varies from GOES-East, GOES-West, GOES-Test, and GOES-Storage.
        _INSTRUMENT_STATIC_ATTRS = ['spatial_resolution', 'instrument_type', 'scene_id',  'instrument_ID']

        # Time attributes
        _TIME_ATTRS = ['date_created', 'time_coverage_start', 'time_coverage_end', 'timeline_id'] # format is YYYY-MM-DD”T”HH:MM:SS.s”Z”.


        # Unidata attributes
        _UNIDATA_ATTRS = ['Conventions', 'Metadata_Conventions', 'standard_name_vocabulary', 'cdm_data_type']
        _KEYWORD_ATTRS = ['keywords_vocabulary', 'keywords' ]
        _LICENSE_ATTRS = ['license']


        # list of variables from GOES MCMIPF Data

        # 2D arrays
        _CMI_VARS =  ['CMI_C01', 'CMI_C02', 'CMI_C03', 'CMI_C04', 'CMI_C05', 'CMI_C06', 'CMI_C07', 'CMI_C08', 'CMI_C09', 'CMI_C10',
                      'CMI_C11', 'CMI_C12', 'CMI_C13', 'CMI_C14', 'CMI_C15', 'CMI_C16']

        _DQF_VARS = ['DQF_C01', 'DQF_C02', 'DQF_C03', 'DQF_C04', 'DQF_C05', 'DQF_C06', 'DQF_C07', 'DQF_C08', 'DQF_C09', 'DQF_C10',
                     'DQF_C11', 'DQF_C12','DQF_C13', 'DQF_C14', 'DQF_C15', 'DQF_C16']


        # string variables


        # int variables



        _CMI_VARS_METADATA = {
            'CMI_C01': {'long_name': 'ABI Cloud and Moisture Imagery reflectance factor', 'standard_name' : 'toa_lambertian_equivalent_albedo_multiplied_by_cosine_solar_zenith_angle', 'band_name':'ABI_BAND_01',
                        'description': 'Blue - Daytime aerosol over land, coastal water mapping.', 'central_band_wavelength': '0.47 um', 'valid_range': [0.0, 1.0], 'units': 1, 'grid_mapping': 'goes_imager_reprojection', 'ancillary_variables': 'DFQ_C01' },

            'CMI_C02': {'long_name': 'ABI Cloud and Moisture Imagery reflectance factor', 'standard_name' : 'toa_lambertian_equivalent_albedo_multiplied_by_cosine_solar_zenith_angle', 'band_name':'ABI_BAND_02',
                        'description': 'Red - Daytime clouds, fog, insolation, winds.', 'central_band_wavelength': '0.64 um', 'valid_range': [0.0, 1.0], 'units': 1, 'grid_mapping': 'goes_imager_reprojection', 'ancillary_variables': 'DFQ_C02' },

            'CMI_C03': {'long_name': 'ABI Cloud and Moisture Imagery reflectance factor', 'standard_name' : 'toa_lambertian_equivalent_albedo_multiplied_by_cosine_solar_zenith_angle', 'band_name':'ABI_BAND_03',
                        'description': 'Daytime vegetation, burn scar, aerosol over water, winds.', 'central_band_wavelength': '0.87 um', 'valid_range': [0.0, 1.0], 'units': 1, 'grid_mapping': 'goes_imager_reprojection', 'ancillary_variables': 'DFQ_C03'},

            'CMI_C04': {'long_name': 'ABI Cloud and Moisture Imagery reflectance factor', 'standard_name' : 'toa_lambertian_equivalent_albedo_multiplied_by_cosine_solar_zenith_angle', 'band_name':'ABI_BAND_04',
                        'description': 'Daytime vegetation, burn scar, aerosol over water, winds.', 'central_band_wavelength': '1.38 um', 'valid_range': [0.0, 1.0], 'units': 1, 'grid_mapping': 'goes_imager_reprojection', 'ancillary_variables': 'DFQ_C04'},

            'CMI_C05': {'long_name': 'ABI Cloud and Moisture Imagery reflectance factor', 'standard_name' : 'toa_lambertian_equivalent_albedo_multiplied_by_cosine_solar_zenith_angle', 'band_name':'ABI_BAND_05',
                        'description': 'Daytime cirrus cloud.', 'central_band_wavelength': '1.61 um', 'valid_range': [0.0, 1.0], 'units': 1, 'grid_mapping': 'goes_imager_reprojection', 'ancillary_variables': 'DFQ_C05'},

            'CMI_C06': {'long_name': 'ABI Cloud and Moisture Imagery reflectance factor', 'standard_name' : 'toa_lambertian_equivalent_albedo_multiplied_by_cosine_solar_zenith_angle', 'band_name':'ABI_BAND_06',
                        'description': 'Daytime cloud-top phase and particle size, snow.' , 'central_band_wavelength': '2.25 um', 'valid_range': [0.0, 1.0], 'units': 1, 'grid_mapping': 'goes_imager_reprojection', 'ancillary_variables': 'DFQ_C06'},

            'CMI_C07': {'long_name': 'ABI Cloud and Moisture Imagery brightness temperature at top of atmosphere', 'standard_name' : 'toa_brightness_temperature', 'band_name':'ABI_BAND_07',
                        'description': 'Daytime reflectance, nighttime radiance - Surface and cloud, fog at night, fire, winds.' , 'central_band_wavelength': '3.90 um', 'valid_range': [197.30, 411.86], 'units': 'K',
                        'grid_mapping': 'goes_imager_reprojection', 'ancillary_variables': 'DFQ_C07'},

            'CMI_C08': {'long_name': 'ABI Cloud and Moisture Imagery brightness temperature at top of atmosphere', 'standard_name' : 'toa_brightness_temperature', 'band_name':'ABI_BAND_08',
                        'description': 'High-level atmospheric water vapor, winds, rainfall.' , 'central_band_wavelength': '6.19 um', 'valid_range': [138.05, 311.06], 'units': 'K',
                        'grid_mapping': 'goes_imager_reprojection', 'ancillary_variables': 'DFQ_C08'},

            'CMI_C09': {'long_name': 'ABI Cloud and Moisture Imagery brightness temperature at top of atmosphere', 'standard_name' : 'toa_brightness_temperature', 'band_name':'ABI_BAND_09',
                        'description': 'Midlevel atmospheric water vapor, winds, rainfall.' , 'central_band_wavelength': '6.93 um', 'valid_range': [138.05, 311.06], 'units': 'K',
                        'grid_mapping': 'goes_imager_reprojection', 'ancillary_variables': 'DFQ_C09'},

            'CMI_C10': {'long_name': 'ABI Cloud and Moisture Imagery brightness temperature at top of atmosphere', 'standard_name' : 'toa_brightness_temperature', 'band_name':'ABI_BAND_10',
                        'description': 'Lower-level water vapor, winds, and silicon dioxide.' , 'central_band_wavelength': '7.34 um', 'valid_range': [126.91, 331.2], 'units': 'K',
                        'grid_mapping': 'goes_imager_reprojection', 'ancillary_variables': 'DFQ_C10'},

            'CMI_C11': {'long_name': 'ABI Cloud and Moisture Imagery brightness temperature at top of atmosphere', 'standard_name' : 'toa_brightness_temperature', 'band_name':'ABI_BAND_11',
                        'description': 'Total water for stability, cloud phase, dust, silicon dioxide, rainfall.' , 'central_band_wavelength': '8.44 um', 'valid_range': [127.69, 341.3], 'units': 'K',
                        'grid_mapping': 'goes_imager_reprojection', 'ancillary_variables': 'DFQ_C11'},

            'CMI_C12': {'long_name': 'ABI Cloud and Moisture Imagery brightness temperature at top of atmosphere', 'standard_name' : 'toa_brightness_temperature', 'band_name':'ABI_BAND_12',
                        'description': 'Total ozone, turbulence, winds.' , 'central_band_wavelength': '9.61 um', 'valid_range': [117.49, 311.06], 'units': 'K',
                        'grid_mapping': 'goes_imager_reprojection', 'ancillary_variables': 'DFQ_C12'},

            'CMI_C13': {'long_name': 'ABI Cloud and Moisture Imagery brightness temperature at top of atmosphere', 'standard_name' : 'toa_brightness_temperature', 'band_name':'ABI_BAND_13',
                        'description': 'Clean longwave - Surface and clouds.' , 'central_band_wavelength': '10.33 um', 'valid_range': [89.62, 341.27], 'units': 'K',
                        'grid_mapping': 'goes_imager_reprojection', 'ancillary_variables': 'DFQ_C13'},

            'CMI_C14': {'long_name': 'ABI Cloud and Moisture Imagery brightness temperature at top of atmosphere', 'standard_name' : 'toa_brightness_temperature', 'band_name':'ABI_BAND_14',
                        'description': 'Longwave - Imagery, sea surface temperature, clouds, rainfall.' , 'central_band_wavelength': '11.21 um', 'valid_range': [96.19, 341.28], 'units': 'K',
                        'grid_mapping': 'goes_imager_reprojection', 'ancillary_variables': 'DFQ_C14'},

            'CMI_C15': {'long_name': 'ABI Cloud and Moisture Imagery brightness temperature at top of atmosphere', 'standard_name' : 'toa_brightness_temperature', 'band_name':'ABI_BAND_15',
                        'description': 'Dirty longwave - Total water, sea surface temperature.' , 'central_band_wavelength': '12.29 um', 'valid_range': [97.38, 341.28], 'units': 'K',
                        'grid_mapping': 'goes_imager_reprojection', 'ancillary_variables': 'DFQ_C15'},

            'CMI_C16': {'long_name': 'ABI Cloud and Moisture Imagery brightness temperature at top of atmosphere', 'standard_name' : 'toa_brightness_temperature', 'band_name':'ABI_BAND_16',
                        'description': 'CO2 - Air temperature, cloud heights.' , 'central_band_wavelength': '12.29 um', 'valid_range': [92.7, 318.26], 'units': 'K',
                        'grid_mapping': 'goes_imager_reprojection', 'ancillary_variables': 'DFQ_C16'}
        }

        _GRID_MAPPING_VAR={'goes_imager_reprojection': {'long_name': 'GOES-R ABI fixed lat-lon grid projection',
                                                        'grid_mapping_name': 'latitude_longitude'}
                           }