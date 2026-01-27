import xarray as xr
import numpy as np


class GOES_MultiCloud:
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


