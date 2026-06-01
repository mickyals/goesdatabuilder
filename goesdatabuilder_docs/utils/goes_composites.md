# GOES Composites Reference

## Overview

The `goesdatabuilder.utils.goes_composites` module provides comprehensive metadata for GOES-R Series Advanced Baseline Imager (ABI) bands, RGB composite recipes, and band difference products used in satellite meteorology.

This module contains no computation logic. It serves as a structured lookup table for band properties, composite recipes, and their physical interpretation. All data is sourced from official CIMSS, CIRA, and NASA SPoRT quick guides.

### Key Features

- **Complete ABI band metadata** for all 16 bands (wavelength, resolution, physics, applications, limitations)
- **RGB composite recipes** with per-channel formulas, clip ranges, gamma corrections, and color interpretation guides
- **Band difference products** with physical interpretation, sign conventions, and application guidance
- **Convenience accessors** for programmatic lookup, listing, and human-readable printing
- **Source URLs** linking to the original quick guide PDFs for every entry

## Data Sources

All entries are derived from official quick guides published by:

- **CIMSS/SSEC** (Cooperative Institute for Meteorological Satellite Studies, University of Wisconsin)
- **CIRA** (Cooperative Institute for Research in the Atmosphere, Colorado State University)
- **NASA SPoRT** (Short-term Prediction Research and Transition Center)

The master index of all quick guides is maintained at:

```python
QUICK_GUIDE_INDEX = "https://rammb2.cira.colostate.edu/training/visit/quick_reference/"
```

Individual PDF links may change over time; the index page maintains current links.

## Module Structure

The module exposes three primary data dictionaries and a set of accessor functions:

| Dictionary | Contents | Keys |
|---|---|---|
| `BAND_INFO` | Metadata for ABI bands 1-16 | Integer band numbers (1-16) |
| `COMPOSITES_RGB` | RGB composite recipes | String identifiers (e.g. "ash", "dust") |
| `COMPOSITES_DIFFERENCE` | Band difference products | String identifiers (e.g. "night_fog", "split_window") |

## BAND_INFO

Each band entry contains:

| Field | Type | Description |
|---|---|---|
| `name` | str | Short name (e.g. "Red", "Clean Longwave Window") |
| `wavelength_um` | float | Central wavelength in micrometers |
| `type` | str | "reflectance" or "brightness_temp" |
| `resolution_km` | float | Pixel resolution at sub-satellite point |
| `nickname` | str | Common nickname (e.g. "Veggie", "Shortwave Infrared") |
| `source` | str | URL to the official quick guide PDF |
| `physics` | str | Physical basis for what the band measures |
| `applications` | list[str] | Primary and secondary uses |
| `limitations` | list[str] | Known constraints and caveats |

### Example

```python
from goesdatabuilder.utils.goes_composites import get_band

band_13 = get_band(13)
print(band_13["name"])           # "Clean Longwave Window"
print(band_13["wavelength_um"])  # 10.33
print(band_13["type"])           # "brightness_temp"
print(band_13["resolution_km"]) # 2.0
```

### Band Summary

| Band | Wavelength (um) | Name | Type |
|------|-----------------|------|------|
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

## COMPOSITES_RGB

Each RGB composite entry contains:

| Field | Type | Description |
|---|---|---|
| `name` | str | Human-readable product name |
| `bands` | list[int] | ABI band numbers required |
| `source` | str | URL to the official quick guide PDF |
| `channels` | dict | Per-channel (R, G, B) recipe with formula, clip, gamma, invert, contribution |
| `appearance` | dict | What common features look like in the composite |
| `applications` | list[str] | Primary use cases |
| `limitations` | list[str] | Known constraints and caveats |
| `time_of_day` | str | "day", "night", or "both" |
| `best_practices` | list[str] | (optional) Operational tips |
| `identification` | dict | (optional) How to identify specific features |
| `comparison` | dict | (optional) How this product relates to others |
| `heritage` | str | (optional) Origin of the recipe |

### Channel Schema

Each channel (R, G, B) within a composite contains:

| Field | Type | Description |
|---|---|---|
| `formula` | str | Band math expression |
| `clip` | dict or None | Min/max clipping bounds (keys vary: min_C/max_C, min_K/max_K, min_pct/max_pct) |
| `gamma` | float or None | Gamma correction value |
| `invert` | bool | Whether the channel is inverted |
| `physical_relation` | str | (optional) What physical property this channel relates to |
| `contribution` | dict | (optional) What small/medium/large values indicate |

### Available RGB Composites

| Key | Name | Bands | Time |
|-----|------|-------|------|
| `air_mass` | Air Mass | C08, C10, C12, C13 | Both |
| `ash` | Ash RGB | C11, C13, C14, C15 | Both |
| `blowing_snow` | Blowing Snow | C02, C05, C07, C13 | Day |
| `cimss_natural_true_color` | CIMSS Natural True Color | C01, C02, C03 | Day |
| `day_cloud_convection` | Day Cloud Convection | C02, C13 | Day |
| `day_cloud_phase_distinction` | Day Cloud Phase Distinction | C02, C05, C13 | Day |
| `day_cloud_type` | Day Cloud Type | C02, C04, C05 | Day |
| `day_convection` | Day Convection | C02, C05, C07, C08, C10, C13 | Day |
| `day_land_cloud` | Day Land Cloud (Natural Color) | C02, C03, C05 | Day |
| `day_land_cloud_fire` | Day Land Cloud/Fire | C02, C03, C06 | Day |
| `day_snow_fog` | Day Snow-Fog | C03, C05, C07, C13 | Day |
| `differential_water_vapor` | Differential Water Vapor | C08, C10 | Both |
| `dust` | Dust RGB | C11, C13, C14, C15 | Both |
| `dust_cvd` | Dust CVD | C11, C13, C15 | Both |
| `fire_temperature` | Fire Temperature | C05, C06, C07 | Both |
| `nighttime_microphysics` | Nighttime Microphysics | C07, C13, C15 | Night |
| `rocket_plume_day` | Rocket Plume (Daytime) | C02, C07, C08 | Day |
| `rocket_plume_night` | Rocket Plume (Nighttime) | C07, C08, C10 | Both |
| `simple_water_vapor` | Simple Water Vapor | C08, C10, C13 | Both |
| `so2` | SO2 RGB | C09, C10, C11, C13 | Both |

### Example

```python
from goesdatabuilder.utils.goes_composites import get_rgb, bands_for

recipe = get_rgb("ash")
print(recipe["name"])                          # "Ash RGB"
print(recipe["time_of_day"])                   # "both"
print(bands_for("ash"))                        # [11, 13, 14, 15]

# Access per-channel details
red = recipe["channels"]["R"]
print(red["formula"])                          # "C15 - C13 (...)"
print(red["clip"])                             # {"min_K": -6.7, "max_K": 2.6}
print(red["gamma"])                            # 1.0
print(red["contribution"]["large"])            # "Thick clouds, ash plume"
```

## COMPOSITES_DIFFERENCE

Each difference product entry contains:

| Field | Type | Description |
|---|---|---|
| `name` | str | Human-readable product name |
| `formula` | str | Band math expression (e.g. "C13 - C07") |
| `bands` | list[int] | ABI band numbers required |
| `source` | str | URL to the official quick guide PDF |
| `description` | str | Plain-language description |
| `units` | str | Physical units of the result ("K" or "reflectance") |
| `physics` | str | Physical basis for the difference |
| `appearance` | dict | What features look like in the difference field |
| `applications` | list[str] | Primary use cases |
| `limitations` | list[str] | Known constraints |
| `sign_interpretation` | dict | (optional) What positive/negative values indicate |
| `time_of_day` | str | "day", "night", or "both" |

### Available Difference Products

| Key | Name | Formula | Units | Time |
|-----|------|---------|-------|------|
| `night_fog` | Night Fog (Stratus) | C13 - C07 | K | Both |
| `split_cloud_phase` | Split Cloud Phase | C14 - C11 | K | Both |
| `split_ozone` | Split Ozone | C12 - C13 | K | Both |
| `split_snow` | Split Snow | C05 - C02 | reflectance | Day |
| `split_water_vapor` | Split Water Vapor | C08 - C10 | K | Both |
| `split_window` | Split Window Difference | C13 - C15 | K | Both |

### Example

```python
from goesdatabuilder.utils.goes_composites import get_difference

diff = get_difference("split_window")
print(diff["formula"])        # "C13 - C15"
print(diff["units"])          # "K"
print(diff["physics"])        # "Water vapor absorbs more energy at 12.3 um..."

# Sign interpretation
print(diff["sign_interpretation"]["positive"])  # "Moisture in the atmosphere..."
print(diff["sign_interpretation"]["negative"])  # "Dust in the atmosphere..."
```

## Convenience Functions

### Listing

```python
from goesdatabuilder.utils.goes_composites import list_rgb_composites, list_differences, list_bands

list_rgb_composites()  # ["blowing_snow", "dust_cvd", "day_cloud_type", ...]
list_differences()     # ["split_water_vapor", "split_ozone", ...]
list_bands()           # [1, 2, 3, ..., 16]
```

### Lookup

```python
from goesdatabuilder.utils.goes_composites import get_rgb, get_difference, get_band, bands_for

get_rgb("day_convection")       # Full RGB recipe dict
get_difference("night_fog")     # Full difference product dict
get_band(7)                     # Full band metadata dict
bands_for("ash")                # [11, 13, 14, 15]
bands_for("split_window")      # [13, 15]
```

All lookup functions raise `KeyError` with a message listing available keys if the requested name is not found.

### Human-Readable Printing

```python
from goesdatabuilder.utils.goes_composites import print_recipe

# Print an RGB composite
print_recipe("ash")

# Print a difference product
print_recipe("split_window")

# Print a band (pass int or string)
print_recipe(13)
print_recipe("13")
```

`print_recipe` auto-detects whether the argument is a band number, RGB composite, or difference product and formats the output accordingly.

## Integration with Plotting Module

The `goes_composites` module pairs with `goesdatabuilder.utils.plotting` for recipe-driven visualization. The composites module provides the "what" (which bands, what math, what ranges) while the plotting module provides the "how" (rendering, colormaps, map projections).

```python
from goesdatabuilder.utils.goes_composites import get_rgb
from goesdatabuilder.utils.plotting import plot_rgb, rescale, stack_rgb

# Look up the recipe
recipe = get_rgb("ash")
channels = recipe["channels"]

# Assume c11, c13, c14, c15 are 2-D numpy arrays already loaded
r = rescale(c15 - c13, channels["R"]["clip"]["min_K"], channels["R"]["clip"]["max_K"])
g = rescale(c14 - c11, channels["G"]["clip"]["min_K"], channels["G"]["clip"]["max_K"])
b = rescale(c13, channels["B"]["clip"]["min_K"], channels["B"]["clip"]["max_K"])

plot_rgb(lon, lat, stack_rgb(r, g, b),
         title=recipe["name"], savepath="ash_rgb.png")
```

## Adding New Composites

To add a new RGB composite, append an entry to `COMPOSITES_RGB` following the existing schema. Required fields:

1. `name`, `bands`, `source`, `channels`, `time_of_day`
2. Each channel needs at minimum: `formula`, `clip`, `gamma`, `invert`

Optional but recommended: `appearance`, `applications`, `limitations`, `best_practices`, `identification`, `comparison`, `heritage`

Only add entries where the recipe details come from an authoritative source (quick guide PDF, published documentation). Do not fabricate appearance or application information.

To add a new difference product, append to `COMPOSITES_DIFFERENCE` with: `name`, `formula`, `bands`, `source`, `description`, `units`, `physics`, `time_of_day`, and optionally `appearance`, `applications`, `limitations`, `sign_interpretation`.

## Best Practices

1. **Use `bands_for()` before loading data** to know exactly which bands a product requires
2. **Check `time_of_day`** to ensure a composite is applicable to your imagery's time
3. **Reference `clip` ranges** for proper rescaling; note that units vary (Celsius, Kelvin, percent reflectance)
4. **Check `invert` flags** on channels that require inversion (e.g. Simple Water Vapor RGB)
5. **Consult `comparison`** fields to understand how related products differ
6. **Link to `source` URLs** in notebooks or presentations for traceability
7. **Use `print_recipe()`** for quick reference during interactive analysis