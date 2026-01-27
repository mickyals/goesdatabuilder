import xarray as xr
import numpy as np


class GOESMultiCloud:
    def __init__(self, multi_cloud_nc_file):
        with xr.open_dataset(multi_cloud_nc_file, engine='netcdf4') as ds:
            self.coordinates = dict(ds.coords)
            self.variables = {k: {'values': v.values, 'attributes': v.attrs} for k, v in ds.data_vars.items()}
            self.attributes = dict(ds.attrs)

        self._current_band = None

    @property
    def band(self):
        """Get current band number"""
        return self._current_band

    @band.setter
    def band(self, band_num):
        """Set current band (1-16)"""
        if not 1 <= band_num <= 16:
            raise ValueError(f"Band must be 1-16, got {band_num}")
        self._current_band = band_num

    ##############################################################################################
    ############ coordinates access ##############################################################
    ##############################################################################################





    ##############################################################################################
    ############ variables access ################################################################
    ##############################################################################################
    @property
    def variable_keys(self):
        return list(self.variables.keys())

    @property
    def CMI(self):
        """Get CMI for current band"""
        if self._current_band is None:
            return None
        return self.variables[f'CMI_C{self._current_band:02d}']

    @property
    def DQF(self):
        """Get DQF for current band"""
        if self._current_band is None:
            return None
        return self.variables[f'DQF_C{self._current_band:02d}']

    @property
    def CMI_stats(self):
        """Get all statistics for current band"""
        if self._current_band is None:
            return None

        band_str = f'C{self._current_band:02d}'
        stats = {}

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
    def subpoint_lon(self):
        lon = self.variables.get('nominal_satellite_subpoint_lon')
        if lon is None:
            raise KeyError("nominal_satellite_subpoint_lon not found in dataset")
        return lon

    @property
    def subpoint_lat(self):
        lat = self.variables.get('nominal_satellite_subpoint_lat')
        if lat is None:
            raise KeyError("nominal_satellite_subpoint_lat not found in dataset")
        return lat

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

