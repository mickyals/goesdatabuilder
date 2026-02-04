import xarray as xr
import numpy as np


class GOESMultiCloud:
    def __init__(self, multi_cloud_nc_file):
        self._file_path = multi_cloud_nc_file  # Store for file size calculation

        with xr.open_dataset(multi_cloud_nc_file, engine='netcdf4') as ds:
            self.coordinates = {k: v.copy() if hasattr(v, 'copy') else v for k, v in ds.coords.items()}
            self.variables = {
                k: {'values': v.values.copy() if hasattr(v.values, 'copy') else v.values, 'attributes': dict(v.attrs)}
                for k, v in ds.data_vars.items()}
            self.attributes = {k: v.copy() if hasattr(v, 'copy') else v for k, v in ds.attrs.items()}

        self._current_band = None

    ############################################################################################
    ############### core property ##############################################################
    ############################################################################################
    @property
    def dataset_id(self):
        """UUID for this product instance"""
        return self.attributes.get('id')

    @property
    def dataset_name(self):
        """Filename of this product"""
        return self.attributes.get('dataset_name')

    @property
    def band(self):
        """Get current band number"""
        return self._current_band

    @band.setter
    def band(self, band_num: int):
        """Set current band (1-16)"""
        if not isinstance(band_num, int):
            raise TypeError(f"Band must be an integer, got {type(band_num).__name__}")
        if not 1 <= band_num <= 16:
            raise ValueError(f"Band must be 1-16, got {band_num}")
        self._current_band = band_num



    ##############################################################################################
    ############ global metadata properties ##########################################################
    ##############################################################################################
    @property
    def global_attribute_keys(self):
        return list(self.attributes.keys())

    @property
    def product_version_info(self):
        """Get algorithm and product version information"""
        container = self.variables.get('algorithm_product_version_container')
        if container is None:
            return None

        attrs = container.get('attributes', {})
        return {
            'algorithm_version': attrs.get('algorithm_version'),  # e.g., OR_ABI-L2-ALG-CMIP_v01r00.zip
            'product_version': attrs.get('product_version'),  # e.g., v01r00
            'major_release': self._parse_version(attrs.get('product_version'))[0],
            'minor_revision': self._parse_version(attrs.get('product_version'))[1],
        }

    ##############################################################################################
    ############ production metadata #############################################################
    ##############################################################################################

    @property
    def production_site(self):
        """NSOF, ESPC, WCDAS"""
        return self.attributes.get('production_site')

    @property
    def production_environment(self):
        """OE (operational) or DE (development)"""
        return self.attributes.get('production_environment')

    @property
    def production_data_source(self):
        """Realtime, Simulated, Playback, or Test"""
        return self.attributes.get('production_data_source')

    @property
    def processing_level(self):
        """NASA L2"""
        return self.attributes.get('processing_level')

    @property
    def production_info(self):
        """All production metadata"""
        return {
            'site': self.production_site,
            'environment': self.production_environment,
            'data_source': self.production_data_source,
            'processing_level': self.processing_level,
        }

    ##############################################################################################
    ############ temporal properties for nc file #############################################################
    ##############################################################################################

    @property
    def date_created(self):
        """Product creation timestamp"""
        return self.attributes.get('date_created')

    @property
    def time_coverage_start(self):
        """Observation start time"""
        return self.attributes.get('time_coverage_start')

    @property
    def time_coverage_end(self):
        """Observation end time"""
        return self.attributes.get('time_coverage_end')

    @property
    def temporal_info(self):
        """All temporal metadata"""
        return {
            'created': self.date_created,
            'start': self.time_coverage_start,
            'end': self.time_coverage_end,
        }

    ##############################################################################################
    ############ coordinates properties ##############################################################
    ##############################################################################################

    @property
    def coordinate_keys(self):
        return list(self.coordinates.keys())

    @property
    def band_wavelength(self):
        """the central wavelength of the current band"""
        if self._current_band is None:
            return None
        return self.coordinates.get(f'band_wavelength_C{self._current_band:02d}')

    @property
    def band_id(self):
        """the band identifier for the current band"""
        if self._current_band is None:
            return None
        return self.coordinates.get(f'band_id_C{self._current_band:02d}')

    @property
    def time(self):
        return self.coordinates.get('t')

    @property
    def time_bounds(self):
        return self.variables.get('time_bounds')

    @property
    def x_axis(self):
        return self.coordinates.get('x')

    @property
    def x_bounds(self):
        return self.variables.get('x_image_bounds')

    @property
    def x_center(self):
        return self.coordinates.get('x_image')

    @property
    def y_axis(self):
        return self.coordinates.get('y')

    @property
    def y_bounds(self):
        return self.variables.get('y_image_bounds')

    @property
    def y_center(self):
        return self.coordinates.get('y_image')


    ##############################################################################################
    ############ variable properties #############################################################
    ##############################################################################################
    @property
    def variable_keys(self):
        return list(self.variables.keys())

    @property
    def CMI(self):
        """Get CMI for current band"""
        if self._current_band is None:
            return None
        cmi = self.variables.get(f'CMI_C{self._current_band:02d}')
        if cmi is None:
            raise KeyError(f'CMI_C{self._current_band:02d} not found in dataset')
        return cmi

    @property
    def DQF(self):
        """Get DQF for current band"""
        if self._current_band is None:
            return None
        dqf = self.variables.get(f'DQF_C{self._current_band:02d}')
        if dqf is None:
            raise KeyError(f'DQF_C{self._current_band:02d} not found in dataset')
        return dqf

    ##############################################################################################
    ############ satellite and projection properties #############################################
    ##############################################################################################
    # SATELLITE IDENTIFICATION
    @property
    def orbital_slot(self):
        """GOES-East, GOES-West, GOES-Test, or GOES-Storage"""
        return self.attributes.get('orbital_slot')

    @property
    def platform_id(self):
        """G16, G17, etc."""
        return self.attributes.get('platform_ID')

    @property
    def instrument_type(self):
        """GOES R Series Advanced Baseline Imager"""
        return self.attributes.get('instrument_type')

    @property
    def instrument_id(self):
        """Serial number of the instrument"""
        return self.attributes.get('instrument_ID')

    @property
    def satellite_info(self):
        """All satellite identification metadata"""
        return {
            'orbital_slot': self.orbital_slot,
            'platform_id': self.platform_id,
            'instrument_type': self.instrument_type,
            'instrument_id': self.instrument_id,
        }

    @property
    def scan_mode(self):
        """ possible values are ABI Mode 3, ABI Mode 4 and ABI Mode 6. """
        return self.attributes.get('timeline_id')

    @property
    def scene_id(self):
        """ possible values are Full Disk, CONUS, and Mesoscale. """
        return self.attributes.get('scene_id')

    # PROJECTION PARAMETERS
    @property
    def goes_imager_projection(self):
        proj = self.variables.get('goes_imager_projection')
        if proj is None:
            raise KeyError("goes_imager_projection not found in dataset")
        return proj

    @property
    def satellite_height(self):
        h = self.variables.get('nominal_satellite_height')
        if h is None:
            raise KeyError("nominal_satellite_height not found in dataset")
        return h # Extract dict of info

    @property
    def subpoint_longitude(self):
        lon = self.variables.get('nominal_satellite_subpoint_lon')
        if lon is None:
            raise KeyError("nominal_satellite_subpoint_lon not found in dataset")
        return lon

    @property
    def subpoint_latitude(self):
        lat = self.variables.get('nominal_satellite_subpoint_lat')
        if lat is None:
            raise KeyError("nominal_satellite_subpoint_lat not found in dataset")
        return lat

    @property
    def spatial_resolution(self):
        """2km at nadir"""
        return self.attributes.get('spatial_resolution')

    ##############################################################################################
    ############ Stats and Local metadata properties #############################################
    ##############################################################################################

    @property
    def CMI_stats(self):
        """Get all statistics for current band"""
        if self._current_band is None:
            return None

        band_str = f'C{self._current_band:02d}'
        stats = {}

        # Get CMI data for size calculation
        cmi = self.CMI
        if cmi:
            cmi_array = cmi['values']
            stats['array_size_bytes'] = cmi_array.nbytes
            stats['array_size_mb'] = cmi_array.nbytes / (1024 ** 2)
            stats['array_shape'] = cmi_array.shape

        # Reflectance stats (bands 1-6)
        if self._current_band <= 6:
            stats['min_reflectance'] = self.variables.get(f'min_reflectance_factor_{band_str}')
            stats['max_reflectance'] = self.variables.get(f'max_reflectance_factor_{band_str}')
            stats['mean_reflectance'] = self.variables.get(f'mean_reflectance_factor_{band_str}')
            stats['std_reflectance'] = self.variables.get(f'std_dev_reflectance_factor_{band_str}')

        # Brightness temperature stats (bands 7-16)
        if self._current_band >= 7:
            stats['min_brightness_temp'] = self.variables.get(f'min_brightness_temperature_{band_str}')
            stats['max_brightness_temp'] = self.variables.get(f'max_brightness_temperature_{band_str}')
            stats['mean_brightness_temp'] = self.variables.get(f'mean_brightness_temperature_{band_str}')
            stats['std_brightness_temp'] = self.variables.get(f'std_dev_brightness_temperature_{band_str}')

        # Common stats for all bands
        stats['outlier_count'] = self.variables.get(f'outlier_pixel_count_{band_str}')

        # Remove None entries
        return {k: v for k, v in stats.items() if v is not None}

    @property
    def storage_metrics(self):
        """File-level storage metrics including compression ratio and band size breakdown"""
        import os

        # File size on disk
        file_size_bytes = os.path.getsize(self._file_path)
        file_size_mb = file_size_bytes / (1024 ** 2)

        # Calculate all CMI band sizes
        band_sizes_bytes = {}
        band_sizes_mb = {}
        total_cmi_bytes = 0

        for band in range(1, 17):
            cmi_var = f'CMI_C{band:02d}'
            if cmi_var in self.variables:
                size_bytes = self.variables[cmi_var]['values'].nbytes
                band_sizes_bytes[band] = size_bytes
                band_sizes_mb[band] = size_bytes / (1024 ** 2)
                total_cmi_bytes += size_bytes

        total_cmi_mb = total_cmi_bytes / (1024 ** 2)

        # Calculate percentages (what % of total CMI memory each band uses)
        band_percentages = {}
        if total_cmi_bytes > 0:
            for band, size_bytes in band_sizes_bytes.items():
                band_percentages[band] = 100 * size_bytes / total_cmi_bytes

        # Compression ratio
        compression_ratio = total_cmi_bytes / file_size_bytes if file_size_bytes > 0 else 0

        return {
            'file_size_bytes': file_size_bytes,
            'file_size_mb': file_size_mb,
            'total_cmi_memory_bytes': total_cmi_bytes,
            'total_cmi_memory_mb': total_cmi_mb,
            'compression_ratio': compression_ratio,
            'band_sizes_bytes': band_sizes_bytes,
            'band_sizes_mb': band_sizes_mb,
            'band_percentages': band_percentages,
        }

    @property
    def data_quality_metrics(self):
        """Get transmission error percentages"""
        return {
            'grb_errors': self.variables.get('percent_uncorrectable_GRB_errors'),
            'l0_errors': self.variables.get('percent_uncorrectable_L0_errors'),
        }

    @property
    def source_data_files(self):
        """Get input L1b radiance file patterns used to create this L2 product"""
        container = self.variables.get('dynamic_algorithm_input_data_container')
        if container:
            return {k: v for k, v in container['attributes'].items()
                    if k.startswith('input_ABI')}
        return None

    ##############################################################################################
    ############ internal methods #############################################
    ##############################################################################################

    @staticmethod
    def _parse_version(version_str):
        """Parse product version string (e.g., v01r00 -> (1, 0))"""
        if not version_str:
            return (None, None)
        try:
            # Format: vVVrRR where VV is major, RR is minor
            major = int(version_str[1:3])
            minor = int(version_str[4:6])
            return (major, minor)
        except (ValueError, IndexError):
            return (None, None)

#####################################################################################################
    def __repr__(self):
        band_str = f"band={self.band}" if self.band else "no band selected"
        return f"GOESMultiCloud(dataset='{self.dataset_name}', {band_str}, platform={self.platform_id}, scene={self.scene_id})"