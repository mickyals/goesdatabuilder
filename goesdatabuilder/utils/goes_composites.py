
"""
GOES ABI Band Information and RGB Composite Recipes
===============================================

This module contains comprehensive metadata for GOES-R Series Advanced Baseline Imager (ABI) bands
and RGB composite recipes used in satellite meteorology.

Data Sources:
- Band information and quick guide links from CIMSS/SSEC and CIRA
- RGB composite recipes from various meteorological organizations
- Physical characteristics and applications for each band

Key Components:
- BAND_INFO: Detailed metadata for all 16 ABI bands (1-16)
- COMPOSITES_RGB: Recipes for RGB composite imagery
- COMPOSITES_DIFFERENCE: Band difference products and their interpretations
- Convenience functions for accessing and displaying this information

Usage:
    from goesdatabuilder.utils.goes_composites import get_rgb, get_band, print_recipe
    
    # Get RGB composite recipe
    ash_rgb = get_rgb("ash")
    
    # Get band information
    band_13 = get_band(13)
    
    # Print human-readable recipe
    print_recipe("day_convection")
"""

# Master reference page for all GOES ABI quick guides.
# Individual PDF links may change; this page maintains current links.
QUICK_GUIDE_INDEX = "https://rammb2.cira.colostate.edu/training/visit/quick_reference/"

# Comprehensive metadata for all 16 GOES ABI bands
# Each band includes physical characteristics, applications, and limitations
BAND_INFO = {

1: {
    "name": "Blue",
    "wavelength_um": 0.47,
    "type": "reflectance",
    "resolution_km": 2.0,
    "nickname": "Blue Visible",
    "source": "https://cimss.ssec.wisc.edu/goes/OCLOFactSheetPDFs/ABIQuickGuide_Band01.pdf",
    "physics": "Samples a part of the electromagnetic spectrum where clear-sky atmospheric "
               "(Rayleigh) scattering is significant. More sensitive to aerosols, dust, and smoke "
               "than the Red band because of this enhanced scattering.",
    "applications": [
        "Monitoring aerosols, dust, haze, and smoke (nearly continuous daytime observations)",
        "Detecting faint smoke plumes not visible in the Red band",
        "Key input to GOES-R Baseline Aerosol Products and Baseline Snow Products",
        "Input to natural/true color RGB imagery (combined with simulated green from Veggie and Red band)",
    ],
    "limitations": [
        "Daytime only (detects reflected visible solar radiation)",
        "Smoke and dust signals depend on scattering angle: more apparent with low sun "
        "(forward scattering) than high sun (backward scattering)",
        "Surface features less distinct than Red band due to coarser spatial resolution (1 km vs 0.5 km) "
        "and enhanced Rayleigh scattering",
    ]},

2: {
    "name": "Red",
    "wavelength_um": 0.64,
    "type": "reflectance",
    "resolution_km": 2.0,
    "nickname": "Red Visible",
    "source": "https://cimss.ssec.wisc.edu/goes/OCLOFactSheetPDFs/ABIQuickGuide_Band02.pdf",
    "physics": "Land absorbs energy at 0.64 um; at longer near-infrared wavelengths more energy is reflected. "
               "Contrast between land and highly reflective clouds is greater over land in the Red band "
               "than in the Veggie or Snow/Ice bands. Atmospheric scattering is not as large at 0.64 um "
               "as at 0.47 um (Blue).",
    "applications": [
        "Detection and analysis of clouds and weather systems during daytime at finest ABI resolution (0.5 km)",
        "Identifying small-scale features: river fogs, fog/clear air boundaries, overshooting tops, cumulus",
        "Documenting daytime snow and ice cover",
        "Diagnosing low-level cloud-drift winds",
        "Assisting with volcanic ash detection and analysis of hurricanes and winter storms",
        "Essential input for true color RGB imagery",
        "Mesoscale sector monitoring for rapidly changing phenomena",
    ],
    "limitations": [
        "Daytime only (detects reflected visible solar radiation)",
        "Very large data volume: this single band is comparable in volume to all ABI infrared bands combined",
        "Pixel reflectance can exceed 100% over thick clouds at large solar zenith angles "
        "due to scattering contributions within the cloud",
    ]},

3: {
    "name": "Veggie",
    "wavelength_um": 0.86,
    "type": "reflectance",
    "resolution_km": 2.0,
    "nickname": "Vegetation",
    "source": "https://cimss.ssec.wisc.edu/goes/OCLOFactSheetPDFs/ABIQuickGuide_Band03.pdf",
    "physics": "Vegetated land is more reflective at 0.86 um than in visible bands. "
               "Land-water contrast is large because water absorbs strongly at 0.86 um. "
               "Reflectance over vegetation at 0.86 um is much greater than for true green light (~0.51 um).",
    "applications": [
        "Detecting daytime clouds, fog, and aerosols",
        "Computing Normalized Difference Vegetation Index (NDVI)",
        "Detecting burn scars for early identification of potential runoff issues",
        "Identifying islands, lakes, flooded regions, and land/sea boundaries (high land-water contrast)",
        "Simulating the green band for true color imagery (combined with Red and Blue bands)",
    ],
    "limitations": [
        "Daytime only (detects reflected solar energy)",
        "Can be used as a stand-in for green band (~0.51 um) in RGB composites but reflectance over "
        "vegetation at 0.86 um is much greater than for green light and must be accounted for",
        "Clouds over land are less distinct than over water because both land and clouds are "
        "reflective at 0.86 um",
    ]},


4: {
    "name": "Cirrus",
    "wavelength_um": 1.37,
    "type": "reflectance",
    "resolution_km": 2.0,
    "nickname": "Cirrus",
    "source": "https://cimss.ssec.wisc.edu/goes/OCLOFactSheetPDFs/ABIQuickGuide_Band04.pdf",
    "physics": "Occupies a region of very strong water vapor absorption in the electromagnetic spectrum. "
               "Energy at 1.37 um is absorbed as it moves through moist atmosphere, so low-level features "
               "are only faintly visible. High clouds above most of the atmospheric moisture are detected clearly.",
    "applications": [
        "Detecting very thin cirrus clouds during daytime",
        "Detecting thick cirrus and high clouds",
        "In dry atmospheres, detecting highly reflective features such as dust or low clouds "
        "when limited water vapor exists above them",
        "Important input to the Daytime Cloud Mask computation",
        "Red component of the Day Cloud Type RGB",
    ],
    "limitations": [
        "Daytime only (detects reflected solar radiation)",
        "Approximately 12 mm of Total Precipitable Water is sufficient to absorb most solar radiation at 1.37 um; "
        "variable moisture amounts and vertical distribution influence how far down the satellite can see",
        "Low clouds not detected in moist atmospheres",
        "One of two near-infrared ABI channels with 2 km resolution (coarser than Red or Veggie bands)",
    ]},

5: {
    "name": "Snow/Ice",
    "wavelength_um": 1.61,
    "type": "reflectance",
    "resolution_km": 2.0,
    "nickname": "Snow/Ice",
    "source": "https://cimss.ssec.wisc.edu/goes/OCLOFactSheetPDFs/ABIQuickGuide_Band05.pdf",
    "physics": "Exploits the difference in refraction components of water and ice at 1.61 um. "
               "Liquid water clouds are bright (reflective) while ice clouds are dark (ice absorbs "
               "rather than reflects at 1.61 um). Snow is also dark at this wavelength despite being "
               "bright in visible bands. Land-water contrast is large; shadows are particularly striking.",
    "applications": [
        "Cloud phase discrimination: water clouds appear bright, ice clouds appear dark",
        "Identifying snow/ice cover (snow is bright at 0.86 um but dark at 1.61 um)",
        "Discriminating water-based clouds from snow-covered surfaces",
        "Nighttime fire detection (very hot fires emit 1.61 um radiation, visible against dark background)",
        "Component of nighttime fire detection RGBs",
        "Component of Day Cloud Type RGB (blue channel) and Day Snow-Fog RGB (green channel)",
    ],
    "limitations": [
        "Daytime application for cloud/surface analysis (detects reflected solar radiation)",
        "Nighttime fire detection requires changing default AWIPS enhancement",
        "Cloud motion or development can block nighttime view of fires",
    ]},

6: {
    "name": "Cloud Particle Size",
    "wavelength_um": 2.24,
    "type": "reflectance",
    "resolution_km": 2.0,
    "nickname": "Cloud Particle Size",
    "source": "https://cimss.ssec.wisc.edu/goes/OCLOFactSheetPDFs/ABIQuickGuide_Band06.pdf",
    "physics": "Small cloud particles are more reflective at 2.24 um than large particles. "
               "In conjunction with other bands, enables cloud particle size estimation. "
               "Exhibits less liquid water-ice contrast than the 1.61 um channel.",
    "applications": [
        "Cloud particle size estimation (small particles bright, large particles dark)",
        "Tracking cloud particle size changes as an indicator of cloud development",
        "Estimating aerosol particle size by characterizing aerosol-free background over land",
        "Input to derived products: Cloud Mask, Aerosol Optical Depth, Cloud Phase",
        "Cloud phase determination for icing threat prediction",
        "Nighttime hot fire detection (very hot fires emit 2.24 um radiation)",
        "Component of fire detection RGBs (red channel in Day Land Cloud/Fire RGB)",
    ],
    "limitations": [
        "Daytime application for cloud/surface analysis (detects reflected solar radiation)",
        "Less liquid water-ice contrast and poorer spatial resolution (2 km) than the 1.61 um channel; "
        "for many daytime uses the 1.61 um channel is a better choice",
        "Nighttime fire detection requires changing default AWIPS enhancement",
        "Cloud motion or development can block nighttime view of fires",
    ]},

7: {
    "name": "Shortwave Window",
    "wavelength_um": 3.90,
    "type": "brightness_temp",
    "resolution_km": 2.0,
    "nickname": "Shortwave Infrared",
    "source": "https://cimss.ssec.wisc.edu/goes/OCLOFactSheetPDFs/ABIQuickGuide_Band07.pdf",
    "physics": "Unique among ABI bands in sensing both emitted terrestrial radiation and significant "
               "reflected solar radiation during the day. Short wavelength makes it more sensitive "
               "than longer-wavelength IR channels to the hottest part of a pixel. Has the most bit "
               "depth of any ABI band (14 bits), with brightness temperature range of -75 C to 140 C.",
    "applications": [
        "Fire detection and hot spot identification (primary application)",
        "Nighttime fog and low cloud identification (stratus clouds do not emit 3.9 um as a blackbody, "
        "producing positive BTD with 10.3 um)",
        "Daytime ice crystal size discrimination (small ice crystals reflect more solar 3.9 um than large crystals)",
        "Volcanic ash detection",
        "Sea surface temperature estimation",
        "Low-level atmospheric vector wind estimation",
        "Urban heat island studies",
        "Component of Nighttime Microphysics RGB (green channel via BTD with 10.3 um)",
        "Component of Fire Temperature RGB (red channel)",
    ],
    "limitations": [
        "2 km resolution means very small fires can be overlooked",
        "Daytime solar reflectance adds to detected 3.9 um radiation, making brightness temperatures "
        "much warmer than 10.3 um; interpretation differs between day and night",
    ]},


8: {
    "name": "Upper-Level Water Vapor",
    "wavelength_um": 6.19,
    "type": "brightness_temp",
    "resolution_km": 2.0,
    "nickname": "Upper-level tropospheric water vapor",
    "source": "https://cimss.ssec.wisc.edu/goes/OCLOFactSheetPDFs/ABIQuickGuide_Band08.pdf",
    "physics": "Senses the mean temperature of a moisture layer in the upper troposphere. "
               "The altitude and depth of the sensed layer varies with the temperature/moisture "
               "profile and satellite viewing angle. Has the highest weighting function peak of "
               "the three ABI water vapor bands.",
    "applications": [
        "Tracking upper-tropospheric winds",
        "Identifying jet streams, troughs, and ridges",
        "Forecasting hurricane track and mid-latitude storm motion",
        "Monitoring severe weather potential",
        "Estimating upper/mid-level moisture for legacy vertical moisture profiles",
        "Identifying regions with potential for turbulence",
        "Validating numerical model initialization",
        "Revealing vertical motions at mid- and upper-levels through warming/cooling with time",
        "Identifying cloudless features that will soon produce clouds/precipitation",
        "Input to Derived Motion Winds and Total Precipitable Water baseline products",
        "Key component of Air Mass RGB (blue channel, inverted) and Differential Water Vapor RGB",
    ],
    "limitations": [
        "WV bands sense the mean temperature of a moisture layer whose altitude and depth vary "
        "with atmospheric profile and viewing angle; weighting function examination helps correct interpretation",
        "Optically dense clouds obstruct view of lower altitude moisture features",
        "Limb cooling: brightness temperature can be ~8 C cooler at the limb vs nadir for identical conditions "
        "due to longer path through colder upper atmosphere",
    ]},

9: {
    "name": "Mid-Level Water Vapor",
    "wavelength_um": 6.93,
    "type": "brightness_temp",
    "resolution_km": 2.0,
    "nickname": "Mid-level tropospheric water vapor",
    "source": "https://cimss.ssec.wisc.edu/goes/OCLOFactSheetPDFs/ABIQuickGuide_Band09.pdf",
    "physics": "Senses the mean temperature of a moisture layer in the mid-troposphere. "
               "Weighting function peaks between the upper-level (6.2 um) and lower-level (7.3 um) "
               "water vapor bands. Surface features are usually not apparent. Legacy GOES 6.5 um "
               "channel covered much of the 6.2, 6.9, and 7.3 um ABI bands.",
    "applications": [
        "Tracking middle-tropospheric winds",
        "Identifying jet streams and vorticity centers",
        "Forecasting hurricane track and mid-latitude storm motion",
        "Monitoring severe weather potential",
        "Estimating mid-level moisture for legacy vertical moisture profiles",
        "Identifying regions where turbulence might exist",
        "Detecting contrails and mountain waves (visibility depends on topography and atmospheric profile)",
        "Input to Derived Motion Winds, Cloud Mask, Stability Indices, and Total Precipitable Water products",
        "Radiances can be assimilated into numerical models",
    ],
    "limitations": [
        "WV bands sense the mean temperature of a moisture layer whose altitude and depth vary "
        "with atmospheric profile and viewing angle; weighting function examination may be necessary",
        "Optically dense clouds obstruct view of lower altitude moisture features",
        "Limb cooling: brightness temperature can be ~8 C cooler at the limb vs nadir for identical conditions",
        "Mountain waves may show up better on different WV bands depending on topography and profile; "
        "different enhancements may need to be applied",
    ]},

10: {
    "name": "Lower-Level Water Vapor",
    "wavelength_um": 7.34,
    "type": "brightness_temp",
    "resolution_km": 2.0,
    "nickname": "Lower-level tropospheric water vapor",
    "source": "https://cimss.ssec.wisc.edu/goes/OCLOFactSheetPDFs/ABIQuickGuide_Band10.pdf",
    "physics": "Typically senses farthest down into the mid-troposphere in cloud-free regions, "
               "to around 500-750 hPa. Has the lowest weighting function peak of the three ABI "
               "water vapor bands. Can detect lower-level clouds when mid/upper atmosphere is "
               "relatively dry.",
    "applications": [
        "Tracking lower-tropospheric winds",
        "Identifying jet streaks and dry slots",
        "Monitoring severe weather potential",
        "Estimating lower-level moisture for legacy vertical moisture profiles",
        "Identifying regions where potential for turbulence exists",
        "Highlighting volcanic plumes rich in SO2 (strong absorption at 7.3 um)",
        "Tracking lake-effect snow bands",
        "Detecting contrails, downslope winds, and horizontal convective rolls",
        "Input to Derived Motion Winds, Cloud Mask, Stability Indices, Total Precipitable Water, "
        "Rain Rate, and Volcanic Ash baseline products",
        "Component of Simple Water Vapor RGB (blue channel, inverted)",
        "Component of Differential Water Vapor RGB (red and green channels)",
    ],
    "limitations": [
        "WV bands sense the mean temperature of a moisture layer whose altitude and depth vary "
        "with atmospheric profile and viewing angle; weighting function plots may help interpretation",
        "Optically dense clouds obstruct view of lower altitude moisture features",
    ]},

11: {
    "name": "Cloud-Top Phase",
    "wavelength_um": 8.44,
    "type": "brightness_temp",
    "resolution_km": 2.0,
    "nickname": "IR Cloud Phase",
    "source": "https://cimss.ssec.wisc.edu/goes/OCLOFactSheetPDFs/ABIQuickGuide_Band11.pdf",
    "physics": "A window channel with little atmospheric absorption in clear skies unless SO2 is present. "
               "Emissivity differences at 8.5 um occur over different soil types and between water droplets "
               "and ice crystals. Water droplets have different emissivity properties at 8.5 um compared "
               "to other wavelengths, enabling cloud phase determination via BTD with 11.2 um. "
               "Not available on legacy GOES Imager or Sounder.",
    "applications": [
        "Monitoring volcanic activity (SO2 strongly absorbs at 8.5 um)",
        "Cloud phase determination via BTD with 11.2 um (driven by emissivity differences between water and ice)",
        "Component of Split Cloud Phase difference product (C14 - C11)",
        "Component of Ash RGB (green channel via BTD with 11.2 um)",
        "Component of Dust RGB (green channel via BTD with 11.2 um)",
        "Component of SO2 RGB (green channel via BTD with 10.35 um)",
    ],
    "limitations": [
        "This is a 'dirty' window: more water vapor absorption than the Clean Window at 10.3 um, "
        "so brightness temperatures are modulated by water vapor",
        "For tracking most meteorological features, the cleaner 10.3 um window channel makes more sense",
        "Surface emissivity varies over different soil types, affecting perceived brightness temperature",
        "Cold overshooting tops and thin cirrus show different brightness temperatures across window channels; "
        "important to consider if using threshold temperatures",
    ]},

12: {
    "name": "Ozone",
    "wavelength_um": 9.61,
    "type": "brightness_temp",
    "resolution_km": 2.0,
    "nickname": "Ozone",
    "source": "https://cimss.ssec.wisc.edu/goes/OCLOFactSheetPDFs/ABIQuickGuide_Band12.pdf",
    "physics": "Both ozone and water vapor absorb 9.6 um energy, producing cooler brightness temperatures "
               "than the clean window band. The weighting function has peaks at the surface and in the "
               "stratosphere (where ozone is most concentrated). Surface ozone cannot be detected because "
               "water vapor also absorbs at 9.6 um. Shows the most limb cooling of any ABI IR band. "
               "Deep convection appears warmer than in window channels due to absorption by ozone "
               "in the warmer stratosphere.",
    "applications": [
        "Providing day and night information about dynamics near the tropopause",
        "Component of the Air Mass RGB (green channel via BTD with 10.3 um, i.e. Split Ozone)",
        "Input to derived products such as Legacy Atmospheric Profiles",
        "Surface features can be discerned (this is also a window channel)",
    ],
    "limitations": [
        "Cannot diagnose total column ozone alone; product generation using other bands is necessary",
        "Water vapor absorption complicates interpretation because horizontal distribution of ozone "
        "and water vapor varies across the globe",
        "Brightness temperature generally increases with less water vapor, less ozone, or increased "
        "air temperature in the layer where water vapor or ozone occurs",
        "Shows the most cooling at large zenith angles (limb effects) of any ABI IR band",
    ]},


13: {
    "name": "Clean Longwave Window",
    "wavelength_um": 10.33,
    "type": "brightness_temp",
    "resolution_km": 2.0,
    "nickname": "Clean Longwave Infrared Window",
    "source": "http://cimss.ssec.wisc.edu/goes/OCLOFactSheetPDFs/ABIQuickGuide_Band13.pdf",
    "physics": "Less sensitive to water vapor absorption than other infrared window bands, making it "
               "the 'cleanest' of the three longwave window channels. Improves atmospheric moisture "
               "corrections in derived products. Legacy GOES 10.7 um channel covered parts of both "
               "the 10.3 um and 11.2 um ABI bands.",
    "applications": [
        "Continuous day/night cloud feature identification and classification",
        "Convective severe weather signature detection (Enhanced-V, thermal couplets associated with "
        "damaging winds, large hail, or tornadoes, usually within 20-30 minutes)",
        "Hurricane intensity estimation and storm contraction monitoring",
        "Cloud-top brightness temperature and cloud particle size estimation",
        "Surface property characterization in derived products",
        "Input to legacy vertical temperature/moisture profiles, stability indices, TPW, SST, "
        "Hurricane Intensity Estimate (HIE), and snow cover baseline products",
        "Used in many RGB composites and band differences (Ash, Dust, Nighttime Microphysics, "
        "Air Mass, Simple Water Vapor, Day Cloud Phase Distinction, etc.)",
    ],
    "limitations": [
        "Brightness temperatures are not necessarily representative of 2-m shelter air temperatures, "
        "especially during daytime when land warms substantially compared to near-surface air",
        "Some absorption of upwelling energy by atmospheric water vapor means satellite-measured "
        "brightness temperatures do not provide truly accurate skin temperature "
        "(Land Surface Temperature baseline product addresses this)",
    ]},


14: {
    "name": "Longwave Window",
    "wavelength_um": 11.21,
    "type": "brightness_temp",
    "resolution_km": 2.0,
    "nickname": "Longwave Infrared Window",
    "source": "https://cimss.ssec.wisc.edu/goes/OCLOFactSheetPDFs/ABIQuickGuide_Band14.pdf",
    "physics": "A window channel with more water vapor absorption than the Clean Window (10.3 um) "
               "but less than the Dirty Window (12.3 um). Clear-sky brightness temperatures are cooler "
               "than 10.3 um by an amount that is a function of atmospheric moisture. Similar spectral "
               "width to legacy GOES 10.7 um but shifted to longer wavelengths.",
    "applications": [
        "Used similarly to the Clean Window (10.3 um) for cloud and surface analysis",
        "Input to Fire Detection, Volcanic Ash Detection, Derived Motion Wind Vectors, "
        "Legacy Atmospheric Profiles, Precipitable Water, Cloud Top Properties, "
        "Aerosol Detection, and Land Surface Temperature baseline products",
        "Component of Ash RGB (green channel via BTD with 8.5 um)",
        "Component of Dust RGB (green channel via BTD with 8.5 um)",
        "Component of Split Cloud Phase difference (C14 - C11)",
    ],
    "limitations": [
        "Not a clean window: water vapor absorbs 11.2 um energy which is re-emitted from higher, "
        "cooler temperatures, so surface/near-surface clear-sky brightness temperatures are cooler "
        "than actual by an amount proportional to atmospheric moisture",
        "Over cold cloud tops (e.g. overshooting tops) brightness temperatures are very similar to "
        "10.3 um because little water vapor exists above the overshoot to absorb energy",
    ]},

15: {
    "name": "Dirty Longwave Window",
    "wavelength_um": 12.29,
    "type": "brightness_temp",
    "resolution_km": 2.0,
    "nickname": "Dirty Longwave Infrared Window",
    "source": "https://cimss.ssec.wisc.edu/goes/OCLOFactSheetPDFs/ABIQuickGuide_Band15.pdf",
    "physics": "Has the most water vapor absorption of the three longwave window channels. "
               "Absorption and re-emission by water vapor, particularly in the lower troposphere, "
               "cools most non-cloud brightness temperatures compared to 10.3 um and 11.2 um. "
               "The more water vapor present, the greater the BT difference with the Clean Window.",
    "applications": [
        "Computing the Split Window Difference (10.3 - 12.3 um) to detect moisture and dust",
        "Distinguishing volcanic ash and dust silicates from cloud water and ice "
        "(silicate emissivity is lower at 10.3 um than at 12.3 um, so 10.3 um BTs are cooler for dust/ash)",
        "Detecting airborne dust including Saharan Air Layers that suppress tropical cyclogenesis",
        "Input to Clear Sky Mask, Cloud Top Properties, Legacy Atmospheric Profiles, "
        "Volcanic Ash, and Fire Hot Spot Characterization baseline products",
        "Component of Ash RGB (red channel via BTD with 10.3 um)",
        "Component of Dust RGB (red channel via BTD with 10.3 um)",
        "Component of Nighttime Microphysics RGB (red channel via BTD with 10.3 um)",
    ],
    "limitations": [
        "This is a 'dirty' window: water vapor absorbs 12.3 um energy which is re-emitted from higher, "
        "cooler temperatures, so surface/near-surface BTs are cooler than shelter thermometers "
        "by an amount proportional to atmospheric moisture",
        "The 10.3 um Clean Window is preferred over the 12.3 um for monitoring simple atmospheric phenomena",
    ]},

16: {
    "name": "CO2 Longwave",
    "wavelength_um": 13.28,
    "type": "brightness_temp",
    "resolution_km": 2.0,
    "nickname": "Carbon Dioxide",
    "source": "https://cimss.ssec.wisc.edu/goes/OCLOFactSheetPDFs/ABIQuickGuide_Band16.pdf",
    "physics": "Occupies the shoulder of a CO2 absorption band between 13 and 14 um. "
               "Cooling is associated with the ubiquitous nature of CO2 in the atmosphere. "
               "Clear-sky brightness temperatures are uniformly cooler than the 10.3 um window channel. "
               "Limb cooling is among the strongest of any ABI channel, just slightly less than the "
               "Ozone band (9.6 um). Surface features are apparent in clear skies but muted compared "
               "to cleaner window channels.",
    "applications": [
        "Delineating the tropopause",
        "Estimating cloud-top heights, pressures, and temperatures",
        "Discerning the level of Derived Motion Winds",
        "Supplementing ASOS sky observations",
        "Identifying volcanic ash (input to quantitative ash detection and height algorithm)",
        "Input to cloud mask, legacy moisture and temperature profiles, TPW, and stability indices",
        "Creating RGBs that highlight upper-level features (muted surface view emphasizes upper atmosphere)",
        "Present on heritage GOES Imagers and Sounders, vital for baseline products",
    ],
    "limitations": [
        "This is a 'dirty' window: strong CO2 absorption means brightness temperatures are much cooler "
        "than other window channels, especially at the limb",
        "Despite importance in derived products, typically not used for visual interpretation of weather events",
    ]}
}

# RGB composite recipes for various meteorological applications
# Each composite defines how to combine ABI bands into color imagery
# Includes channel formulas, clipping ranges, gamma corrections, and interpretation guides
COMPOSITES_RGB = {
    "blowing_snow": {
    "name": "Blowing Snow",
    "bands": [2, 5, 7, 13],
    "source": "https://rammb2.cira.colostate.edu/wp-content/uploads/2025/12/QuickGuide_GOES_Blowing_Snow.pdf",
    "channels": {
        "R": {
            "formula": "C02 (Red)",
            "clip": {"min_pct": 0, "max_pct": 50},
            "gamma": 0.7,
            "invert": False,
            "contribution": {
                "small": "Water body, land surface",
                "medium": "Liquid clouds",
                "large": "Glaciated clouds, snow/ice cover, blowing snow",
            },
        },
        "G": {
            "formula": "C05 (Snow/Ice)",
            "clip": {"min_pct": 0, "max_pct": 20},
            "gamma": 1.0,
            "invert": False,
            "contribution": {
                "small": "Water body, snow/ice cover",
                "medium": "Blowing snow, glaciated clouds",
                "large": "Land surface, liquid clouds",
            },
        },
        "B": {
            "formula": "C07 - C13 (Shortwave Window - Clean Longwave Window)",
            "clip": {"min_C": 0, "max_C": 30},
            "gamma": 0.7,
            "invert": False,
            "contribution": {
                "small": "Water body, snow/ice cover, land surface",
                "medium": "Blowing snow",
                "large": "Glaciated clouds, liquid clouds",
            },
        },
    },
    "appearance": {
        "blowing_snow": "Peach / orange / brown against darker red background (snow cover)",
        "snow_cover": "Dark red",
        "bare_ground": "Light green",
        "water_bodies": "Black",
        "liquid_cloud": "Light green/cyan",
        "thick_ice_cloud": "Blue/violet/purple",
        "thin_ice_cloud_over_snow": "Pink/violet",
        "mixed_phase_cloud": "Light blue",
        "clouds_high_illumination": "Saturated cyan (ice and liquid appear similar)",
        "clouds_low_illumination": "Dark blue (ice and liquid appear similar)",
    },
    "applications": [
        "Detecting blowing snow plumes over snow-covered surfaces",
        "Identifying horizontal convective rolls (HCRs) associated with blowing snow",
        "Visibility hazard monitoring for aviation and road transport",
    ],
    "limitations": [
        "Daytime only (visible and near-infrared channels)",
        "Reduced availability in winter due to shorter daylight",
        "Cloud cover can obscure blowing snow beneath",
        "Insufficient spatial resolution for small-scale or shallow plumes "
        "(VIIRS Blowing Snow RGB offers better resolution)",
        "Under high solar illumination, ice and liquid clouds may both "
        "appear as saturated cyan, making differentiation difficult",
    ],
    "best_practices": [
        "Animate to confirm linear plume movement across snow-covered surfaces",
        "Look for texture in imagery near sunrise/sunset when deepening HCRs cast shadows on snow cover",
        "Use METARs, webcams, and surface reports for confirmation",
    ],
    "identification": {
        "color": "Shades of brown, orange, and peach against darker red background (snow cover)",
        "texture": "Deepening HCRs cast small shadows, creating revealing texture in imagery",
        "trends": "Apparent linear movement across snow-covered surfaces visible in animation",
    },
    "time_of_day": "day"},

"dust_cvd": {
    "name": "Dust - CVD (Color Vision Deficiency Accessible)",
    "bands": [11, 13, 15],
    "source": "https://rammb2.cira.colostate.edu/wp-content/uploads/2025/12/QuickGuide_GOES_Dust_CVD.pdf",
    "channels": {
        "R": {
            "formula": "C13 - C11 (Clean Longwave Window - Cloud-Top Phase)",
            "clip": {"min_C": -0.5, "max_C": 6},
            "gamma": 1.5,
            "invert": False,
            "contribution": {
                "small": "Clear sky",
                "medium": "Low cloud",
                "large": "Blowing dust, thick high cloud, thin high cloud",
            },
        },
        "G": {
            "formula": "C15 - C13 (Dirty Longwave Window - Clean Longwave Window)",
            "clip": {"min_C": -6, "max_C": 2.5},
            "gamma": 1.5,
            "invert": False,
            "contribution": {
                "small": "Clear sky (moist), thin high cloud",
                "medium": "Clear sky (dry), thick high cloud, low cloud",
                "large": "Blowing dust",
            },
        },
        "B": {
            "formula": "C13 (Clean Longwave Window)",
            "clip": {"min_C": -40, "max_C": 40},
            "gamma": 1.0,
            "invert": False,
            "contribution": {
                "small": "High cloud",
                "medium": "Blowing dust, low cloud",
                "large": "Clear sky surface",
            },
        },
    },
    "appearance": {
        "blowing_dust_thick": "Yellow / bright green",
        "blowing_dust_diffuse": "Lighter blue relative to background",
        "blowing_dust_night": "Light pink/orange",
        "clear_sky_moist": "Dark blue",
        "clear_sky_dry": "Light blue",
        "thick_ice_cloud": "Orange",
        "thin_ice_cloud": "Red",
        "low_cloud": "Medium green/cyan to grey",
    },
    "applications": [
        "Detecting blowing dust plumes during both day and night",
        "Visibility hazard monitoring for aviation and road transport",
        "Accessible alternative to traditional Dust RGB for users with color vision deficiencies",
    ],
    "limitations": [
        "Cloud cover can obscure dust beneath",
        "Ash-laden smoke from intense wildfires can resemble blowing dust",
        "Low concentration or shallow dust may be subtle; signal strongest for thick dense plumes",
        "Very hot/dry desert surfaces, salt flats, or bright playas can mimic dust signatures, "
        "particularly near sunrise/sunset or at night when thermal contrast is small",
    ],
    "best_practices": [
        "Animate to confirm plume motion and source region",
        "Compare with GeoColor or visible imagery when available",
        "Use METARs, webcams, and surface reports for confirmation",
    ],
    "time_of_day": "both"},

"day_cloud_type": {
    "name": "Day Cloud Type",
    "bands": [2, 4, 5],
    "source": "http://cimss.ssec.wisc.edu/goes/OCLOFactSheetPDFs/ABIQuickGuide_Day_Cloud_Type_RGB.pdf",
    "channels": {
        "R": {
            "formula": "C04 (Cirrus, 1.38 um)",
            "clip": None,
            "gamma": 0.66,
            "invert": False,
            "physical_relation": "Cloud height",
            "contribution": {
                "small": "Low clouds",
                "large": "High clouds",
            },
        },
        "G": {
            "formula": "C02 (Red, 0.64 um)",
            "clip": None,
            "gamma": 1.0,
            "invert": False,
            "physical_relation": "Cloud optical thickness",
            "contribution": {
                "small": "Thin (or no) clouds",
                "large": "Thick clouds / snow / ice",
            },
        },
        "B": {
            "formula": "C05 (Snow/Ice, 1.61 um)",
            "clip": None,
            "gamma": 1.0,
            "invert": False,
            "physical_relation": "Cloud phase",
            "contribution": {
                "small": "Ice crystals",
                "large": "Water droplets",
            },
        },
    },
    "appearance": {
        "thin_cirrus": "Red/orange (strong C04, weak C02 and C05)",
        "thick_cirrus": "Orange/yellow (strong C04 and C02)",
        "water_clouds": "Blue (strong C05, weak C04)",
        "thick_water_clouds": "Cyan/white (strong C05 and C02)",
        "snow_cover_clear": "Bright green (strong C02, similar to Day Cloud Phase Distinction)",
        "clear_sky_low_clouds": "Identical appearance to Day Cloud Phase Distinction RGB",
    },
    "applications": [
        "Differentiating thin cirrus from thick cirrus (improved over Day Cloud Phase Distinction)",
        "Detecting cloud phase changes by observing color transitions",
        "Tracking glaciation in growing convection (color shifts from blue to green to yellow to orange)",
    ],
    "limitations": [
        "Daytime only (reflective bands only)",
        "No temperature information (unlike Day Cloud Phase Distinction RGB)",
        "Cloud growth tracked indirectly via cirrus channel signal rather than brightness temperature",
    ],
    "identification": {
        "glaciation_sequence": "As cloud grows vertically and glaciates: blue -> green -> yellow -> orange",
        "vs_day_cloud_phase": "Thin cirrus is far more apparent in Day Cloud Type than Day Cloud Phase Distinction; "
                              "in regions of low clouds or clear air the two RGBs are identical",
    },
    "time_of_day": "day"},

"rocket_plume_night": {
    "name": "Rocket Plume (Nighttime / IR-only)",
    "bands": [7, 8, 10],
    "source": "https://cimss.ssec.wisc.edu/goes/OCLOFactSheetPDFs/QuickGuide_Template_GOESRBanner_Plume_night.pdf",
    "channels": {
        "R": {
            "formula": "C07 (Shortwave Window, 3.9 um)",
            "clip": {"min_K": 273, "max_K": 338},
            "gamma": 1.0,
            "invert": False,
            "physical_relation": "Plume temperature",
            "contribution": {
                "large": "Warm plume / hotspot",
            },
        },
        "G": {
            "formula": "C08 (Upper-Level Water Vapor, 6.2 um)",
            "clip": {"min_K": 220, "max_K": 280},
            "gamma": 1.0,
            "invert": False,
            "physical_relation": "Plume warming in upper-level WV channel",
            "contribution": {
                "large": "Plume cloud",
            },
        },
        "B": {
            "formula": "C10 (Lower-Level Water Vapor, 7.3 um)",
            "clip": {"min_K": 230, "max_K": 290},
            "gamma": 1.0,
            "invert": False,
            "physical_relation": "Plume warming in lower-level WV channel",
            "contribution": {
                "large": "Plume cloud",
            },
        },
    },
    "appearance": {
        "hotspot": "Reddish (strong 3.9 um signal)",
        "plume_cloud": "Warmer signal in water vapor bands",
        "background": "Variable; colors shift diurnally, seasonally, and latitudinally",
    },
    "applications": [
        "Quick-look detection of rocket plume spectral signatures",
        "Monitoring hotspots associated with rocket launches",
        "IR-only version usable both day and night",
    ],
    "limitations": [
        "Care needed as region of interest approaches edge of full disk",
        "Water vapor plume may be harder to detect depending on background moisture",
        "Blue component can be overwhelmed by the 3.9 um band",
        "Clip limits may need adjustment for improved presentation",
        "Thick clouds or low-level moisture can obscure the plume hotspot when lower in the atmosphere",
    ],
    "time_of_day": "both"},

"rocket_plume_day": {
    "name": "Rocket Plume (Daytime)",
    "bands": [2, 7, 8],
    "source": "https://cimss.ssec.wisc.edu/goes/OCLOFactSheetPDFs/QuickGuide_Template_GOESRBanner_Plume_day.pdf",
    "channels": {
        "R": {
            "formula": "C07 (Shortwave Window, 3.9 um)",
            "clip": {"min_K": 273, "max_K": 338},
            "gamma": 1.0,
            "invert": False,
            "physical_relation": "Plume temperature",
            "contribution": {
                "large": "Warm plume / hotspot",
            },
        },
        "G": {
            "formula": "C08 (Upper-Level Water Vapor, 6.2 um)",
            "clip": {"min_K": 233, "max_K": 253},
            "gamma": 1.0,
            "invert": False,
            "physical_relation": "Plume warming in upper-level WV channel",
            "contribution": {
                "large": "Plume cloud",
            },
        },
        "B": {
            "formula": "C02 (Red, 0.64 um)",
            "clip": {"min_pct": 0, "max_pct": 80},
            "gamma": 1.0,
            "invert": False,
            "physical_relation": "Reflective clouds",
            "contribution": {
                "large": "Plume location",
            },
        },
    },
    "appearance": {
        "hotspot": "Reddish (strong 3.9 um signal)",
        "warming_trail": "Yellow-green (rocket warming trail)",
        "background": "Variable; colors shift diurnally, seasonally, and latitudinally",
    },
    "applications": [
        "Quick-look detection of rocket plume spectral signatures during daytime",
        "Monitoring hotspots associated with rocket launches",
        "Daytime complement to the IR-only nighttime version",
    ],
    "limitations": [
        "Care needed as region of interest approaches edge of full disk",
        "Water vapor plume may be harder to detect depending on background moisture",
        "Blue component can be overwhelmed by the 3.9 um band",
        "Clip limits may need adjustment for better presentation, especially for cold scenes",
        "Thick clouds or low-level moisture can obscure the plume hotspot when lower in the atmosphere",
    ],
    "comparison": {
        "fire_rgb": "Similar hotspot appearance (red)",
        "airmass_rgb": "Similar in leveraging a mid-level water vapor band",
        "rocket_plume_night": "Nighttime version replaces C02 (blue channel) with C10 (7.3 um WV) "
                              "and uses different green channel clip range (220-280 K vs 233-253 K)",
    },
    "time_of_day": "day"},

"nighttime_microphysics": {
    "name": "Nighttime Microphysics",
    "bands": [7, 13, 15],
    "source": "https://rammb2.cira.colostate.edu/wp-content/uploads/2025/07/QuickGuide_GOESR_NtMicroRGB_Final_20191206_acc.pdf",
    "channels": {
        "R": {
            "formula": "C15 - C13 (Dirty Longwave Window - Clean Longwave Window, 12.4 - 10.4 um)",
            "clip": {"min_C": -6.7, "max_C": 2.6},
            "gamma": 1.0,
            "invert": False,
            "physical_relation": "Optical depth",
            "contribution": {
                "small": "Thin clouds",
                "large": "Thick clouds",
            },
        },
        "G": {
            "formula": "C13 - C07 (Clean Longwave Window - Shortwave Window, 10.4 - 3.9 um)",
            "clip": {"min_C": -3.1, "max_C": 5.2},
            "gamma": 1.0,
            "invert": False,
            "physical_relation": "Particle phase and size",
            "contribution": {
                "small": "Ice particles; surface (cloud free)",
                "large": "Water clouds with small particles",
            },
        },
        "B": {
            "formula": "C13 (Clean Longwave Window, 10.4 um)",
            "clip": {"min_C": -29.6, "max_C": 19.5},
            "gamma": 1.0,
            "invert": False,
            "physical_relation": "Temperature of surface",
            "contribution": {
                "small": "Cold surface",
                "large": "Warm surface",
            },
        },
    },
    "appearance": {
        "fog_warm_regime": "Dull aqua to grey (washed out compared to low clouds)",
        "fog_cold_regime": "Dull yellow-green to grey",
        "very_low_warm_cloud": "Aqua",
        "low_cool_cloud": "Bright green",
        "mid_water_cloud": "Light green",
        "mid_thick_water_ice_cloud": "Tan",
        "high_thin_ice_cloud": "Dark blue",
        "high_very_thin_ice_cloud": "Purple",
        "high_thick_cloud": "Dark red",
        "high_opaque_cirrus": "Near black",
        "high_thick_very_cold_cloud": "Red/yellow, noisy",
    },
    "applications": [
        "Distinguishing fog from low clouds (fog appears washed out / less bright compared to low clouds)",
        "Efficient multi-cloud-type discrimination across full imagery",
        "Cloud height and phase analysis",
        "Fire hotspot detection",
        "Moisture boundary identification",
    ],
    "limitations": [
        "Nighttime only (shortwave IR band contaminated by solar reflectance during daytime)",
        "Thin radiation fog is semi-transparent, allowing surface emissions to impact pixel color",
        "Cloud-free regions vary in color depending on temperature, surface type, and column moisture",
        "Speckled yellow pixels appear in very cold clouds (roughly below -30 C) due to shortwave IR noise",
        "Best applies to opaque clouds; semi-transparent clouds are influenced by underlying surface",
    ],
    "best_practices": [
        "Fog tends to appear washed out compared to low clouds; look for less bright or near-grey coloring",
        "Low clouds are aqua in warm regimes but shift to yellow/light green in cold regimes (decreased blue component)",
        "Compare with legacy 10.3-3.9 um fog product to reduce false alarms",
    ],
    "comparison": {
        "legacy_fog_product": "NtMicro RGB helps distinguish fog from clouds and false alarm features "
                              "seen in the legacy 10.3-3.9 um channel difference (which is also embedded in the green channel)",
    },
    "time_of_day": "night"},

"so2": {
    "name": "SO2 RGB",
    "bands": [9, 10, 11, 13],
    "source": "https://rammb2.cira.colostate.edu/wp-content/uploads/2020/01/Quick_Guide_SO2_RGB-1-1.pdf",
    "channels": {
        "R": {
            "formula": "C09 - C10 (Mid-Level Water Vapor - Lower-Level Water Vapor, 6.95 - 7.34 um)",
            "clip": {"min_C": -4.0, "max_C": 2.0},
            "gamma": 1.0,
            "invert": False,
            "physical_relation": "Vertical water vapor difference, presence of SO2",
            "contribution": {
                "small": "Low-levels, relatively drier atmosphere",
                "large": "SO2 present in mid- and high-levels of the atmosphere",
            },
        },
        "G": {
            "formula": "C13 - C11 (Clean Longwave Window - Cloud-Top Phase, 10.35 - 8.50 um)",
            "clip": {"min_C": -4.0, "max_C": 5.0},
            "gamma": 1.0,
            "invert": False,
            "physical_relation": "Moisture, stability, particle size and phase, presence of ash and SO2",
            "contribution": {
                "small": "Small crystal ice cloud",
                "large": "Low- and mid-level cloud, volcanic ash and/or SO2",
            },
        },
        "B": {
            "formula": "C13 (Clean Longwave Window, 10.35 um)",
            "clip": {"min_C": -30.1, "max_C": 29.8},
            "gamma": 1.0,
            "invert": False,
            "physical_relation": "Cloud top or surface temperature",
            "contribution": {
                "small": "Mid- and high-levels in the atmosphere",
                "large": "Surface or low-levels in the atmosphere",
            },
        },
    },
    "appearance": {
        "upper_level_so2_cold_background": "Orange",
        "upper_level_so2_warm_background": "Light yellow",
        "low_level_so2": "Light green",
        "low_mid_level_cloud": "Green",
        "convective_clouds": "Tan",
        "thin_high_level_cloud": "Dark blue",
        "ocean_land_surface": "Light blue",
    },
    "applications": [
        "Detecting and monitoring large SO2 emissions from volcanic eruptions",
        "Monitoring industrial SO2 sources such as power plants",
        "Distinguishing SO2 from volcanic ash and water vapor",
    ],
    "limitations": [
        "Low-level SO2 appears light green, similar to low-level cloud, making discrimination difficult",
        "Thick opaque upper-level clouds can mask SO2 signal below",
        "Water vapor can mask ash and aerosol signals in volcanic eruptions",
        "Interpretation still under investigation",
    ],
    "comparison": {
        "ash_rgb": "SO2 RGB is a modified Ash RGB: red channel uses C09-C10 (SO2 absorption at 7.34 um) "
                   "instead of longwave difference; green channel uses similar bands but different ranges "
                   "to exploit lesser SO2 absorption at 8.50 um; blue channel is identical",
        "ir_10_35": "SO2 is not discernible in single-channel 10.35 um infrared imagery",
    },
    "heritage": "Originally developed by the Japan Meteorological Agency (JMA) for Himawari-8",
    "time_of_day": "both"},

"day_snow_fog": {
    "name": "Day Snow-Fog",
    "bands": [3, 5, 7, 13],
    "source": "https://rammb2.cira.colostate.edu/wp-content/uploads/2020/01/QuickGuide_DaySnowFogRGB-1.pdf",
    "channels": {
        "R": {
            "formula": "C03 (Veggie, 0.86 um)",
            "clip": {"min_pct": 0, "max_pct": 100},
            "gamma": 1.7,
            "invert": False,
            "physical_relation": "Reflectance of clouds and surfaces",
            "contribution": {
                "small": "Water, thin cirrus",
                "large": "Thick clouds, snow, sea ice",
            },
        },
        "G": {
            "formula": "C05 (Snow/Ice, 1.6 um)",
            "clip": {"min_pct": 0, "max_pct": 70},
            "gamma": 1.7,
            "invert": False,
            "physical_relation": "Reflectance of clouds and surfaces",
            "contribution": {
                "small": "Water, snow",
                "large": "Vegetated land, thick water clouds",
            },
        },
        "B": {
            "formula": "C07 - C13 (Shortwave Window - Clean Longwave Window, 3.9 - 10.3 um)",
            "clip": {"min_C": 0, "max_C": 30},
            "gamma": 1.7,
            "invert": False,
            "physical_relation": "Proxy for 3.9 um reflected solar radiance",
            "contribution": {
                "small": "Water, snow",
                "large": "Thick clouds",
            },
        },
    },
    "appearance": {
        "snow": "Red-orange",
        "water_clouds_fog": "Shades of yellow",
        "ice_clouds_cirrus": "Shades of pink",
        "ocean": "Black",
        "vegetation": "Green",
    },
    "applications": [
        "Distinguishing snow and clear ground from clouds",
        "Discriminating low-level water cloud (bright) from non-reflective snow (dark at 1.6 and 3.9 um)",
        "Cloud phase identification (water vs ice)",
        "Identifying thin middle/upper-level clouds over low-level cloud layers (especially in animation)",
    ],
    "limitations": [
        "Daytime only (0.86, 1.6, and 3.9 um bands detect reflected solar radiation)",
        "Low solar angles at sunrise/sunset change color interpretation; limited for high latitudes in winter",
        "Limited ability to detect thin cirrus due to low contrast with background (mitigated by animation)",
        "Coniferous forest canopy masks snow signature beneath",
        "Blue component uses channel difference as proxy for 3.9 um reflected solar, "
        "not the reflected solar component directly as in JMA or EUMETSAT implementations",
    ],
    "comparison": {
        "visible_064": "Day Snow-Fog RGB provides better distinction between low clouds and snow/ice "
                       "and better identification of low-level cloud thickness compared to single-channel visible",
        "jma_himawari": "JMA Himawari Day Snow-Fog uses 3.9 um reflected solar directly for blue; "
                        "GOES version uses channel difference as proxy, so color interpretation differs slightly",
        "eumetsat_snow_rgb": "EUMETSAT Snow RGB (formerly Day Solar RGB) also uses 3.9 um reflected solar directly",
    },
    "heritage": "Based on JMA Himawari Day Snow-Fog RGB and EUMETSAT Snow RGB (Day Solar RGB)",
    "time_of_day": "day"},


"day_cloud_phase_distinction": {
    "name": "Day Cloud Phase Distinction",
    "bands": [2, 5, 13],
    "source": "https://rammb2.cira.colostate.edu/wp-content/uploads/2025/06/QuickGuide_DayCloudPhaseDistinction_final_v2.pdf",
    "channels": {
        "R": {
            "formula": "C13 (Clean Longwave Window, 10.3 um)",
            "clip": {"min_C": -53.5, "max_C": 7.5},
            "gamma": 1.0,
            "invert": False,
            "physical_relation": "Surface or cloud top temperature",
            "contribution": {
                "small": "Warm: land (seasonal), ocean",
                "large": "Cold: land (winter), snow, high clouds",
            },
        },
        "G": {
            "formula": "C02 (Red, 0.64 um)",
            "clip": {"min_pct": 0, "max_pct": 78},
            "gamma": 1.0,
            "invert": False,
            "physical_relation": "Reflectance of clouds and surfaces",
            "contribution": {
                "small": "Water, vegetation, land",
                "large": "Cloud, snow, white sand",
            },
        },
        "B": {
            "formula": "C05 (Snow/Ice, 1.6 um)",
            "clip": {"min_pct": 1, "max_pct": 59},
            "gamma": 1.0,
            "invert": False,
            "physical_relation": "Reflectance, particle phase",
            "contribution": {
                "small": "Ice particles",
                "large": "Water particles, land surface",
            },
        },
    },
    "appearance": {
        "low_level_water_cloud": "Cyan, lavender",
        "glaciating_cloud": "Green",
        "snow": "Shades of green",
        "thick_high_ice_cloud": "Yellow",
        "thin_mid_level_water_cloud": "Magenta",
        "thin_high_ice_cloud": "Red-orange",
        "land_surface": "Shades of blue",
        "water_surface": "Black",
    },
    "applications": [
        "Monitoring convective initiation by observing cloud phase transitions",
        "Tracking storm growth and decay (cumulus transitioning from light shades to bolder green/yellow indicates glaciation)",
        "Identifying updrafts and overshooting tops to evaluate storm evolution",
        "Snow squall detection (glaciated cloud bands associated with heavy precipitation snow events)",
        "Snow cover identification on the ground",
    ],
    "limitations": [
        "Daytime only (0.64 um VIS and 1.6 um NIR rely on reflected solar radiation)",
        "Low solar angles (sunrise/sunset, winter) decrease VIS and NIR reflectance values, producing reddish scenes",
        "Limb cooling effect at high latitudes skews 10.35 um IR toward cold temperatures, also producing reddish scenes",
        "Colors vary diurnally, seasonally, and by latitude due to 10.35 um IR component sensitivity to surface temperature",
        "Thin high-level cloud color interpretation may differ from JMA developer guidance "
        "(JMA identifies thin high-level cloud as magenta; actual presentation depends on context)",
    ],
    "identification": {
        "convective_initiation": "Cumulus transitioning from light shades to bolder green and yellow indicates "
                                 "vertical development and increasing cloud ice associated with strong storms",
        "glaciation_sequence": "Water cloud (cyan/lavender) -> glaciating (green) -> thick ice (yellow)",
    },
    "comparison": {
        "visible_064": "Day Cloud Phase Distinction RGB provides greater contrast for distinguishing "
                       "ice and water clouds and background features compared to traditional visible imagery",
        "day_cloud_type": "Day Cloud Type RGB uses C04 (Cirrus) instead of C13 (IR), providing better "
                          "thin/thick cirrus differentiation but no temperature information",
    },
    "heritage": "Developed by the Japan Meteorological Agency (JMA) for Himawari-8",
    "time_of_day": "day"},


"ash": {
    "name": "Ash RGB",
    "bands": [11, 13, 14, 15],
    "source": "https://rammb2.cira.colostate.edu/wp-content/uploads/2020/01/GOES_Ash_RGB-1.pdf",
    "channels": {
        "R": {
            "formula": "C15 - C13 (Dirty Longwave Window - Clean Longwave Window, 12.3 - 10.3 um)",
            "clip": {"min_K": -6.7, "max_K": 2.6},
            "gamma": 1.0,
            "invert": False,
            "physical_relation": "Optical depth / cloud thickness",
            "contribution": {
                "small": "Thin clouds",
                "large": "Thick clouds, ash plume",
            },
        },
        "G": {
            "formula": "C14 - C11 (Longwave Window - Cloud-Top Phase, 11.2 - 8.4 um)",
            "clip": {"min_K": -6.0, "max_K": 6.3},
            "gamma": 1.0,
            "invert": False,
            "physical_relation": "Particle phase/size",
            "contribution": {
                "small": "Large water or ice particles",
                "large": "Small water or ice particles, sulfur dioxide gas",
            },
        },
        "B": {
            "formula": "C13 (Clean Longwave Window, 10.3 um)",
            "clip": {"min_K": 243.6, "max_K": 302.4},
            "gamma": 1.0,
            "invert": False,
            "physical_relation": "Temperature of surface",
            "contribution": {
                "small": "Cold surface",
                "large": "Warm surface",
            },
        },
    },
    "appearance": {
        "ash_pure": "Reds to magentas",
        "so2_gas": "Bright greens",
        "ash_mixed_with_so2": "Yellows",
        "low_thick_water_cloud": "Light green to grey",
        "mid_thick_cloud": "Light tan",
        "mid_thin_cloud": "Dark green",
        "high_thick_ice_cloud": "Browns",
        "high_thin_cloud": "Dark blue to black",
    },
    "applications": [
        "Detection and monitoring of volcanic ash plumes (day and night, IR-only channels)",
        "SO2 identification (8.4 um absorbs SO2, producing large difference with 11.2 um, appearing bright green)",
        "Secondary: water vs ice and thick vs thin cloud analysis (though other RGBs may be more suited)",
    ],
    "limitations": [
        "At high viewing angles near the limb, SO2 and low clouds appear in similar green coloring "
        "(Dust RGB recommended for greater contrast in these cases)",
        "As rocky/desert surfaces cool diurnally, their color shifts from blue toward magenta/pink, "
        "making ash plumes less apparent against the cooling surface",
        "Black cirrus can appear in both volcanic and non-volcanic cloud systems",
        "Less effective for ash and SO2 analysis when ice clouds are in the same area (mixed scenes)",
    ],
    "identification": {
        "ash_detection": "Positive 12.3-10.3 um difference (opposite absorption characteristics of ash vs ice) "
                         "provides more red than any other cloud object",
        "ash_altitude": "Ash appears red to magenta to pink depending on altitude",
        "so2_detection": "Strong SO2 absorption at 8.4 um creates large difference with 11.2 um, "
                         "appearing as bright green",
    },
    "comparison": {
        "10_3_minus_12_3_difference": "Ash RGB already contains the 10.3-12.3 um difference information "
                                      "but further separates ash from cloud objects, provides SO2 detection, "
                                      "and allows water vs ice cloud analysis",
        "so2_rgb": "SO2 RGB is a modified version of the Ash RGB, tuned for better SO2 detection",
    },
    "time_of_day": "both"},


"day_cloud_convection": {
    "name": "Day Cloud Convection",
    "bands": [2, 13],
    "source": "https://rammb2.cira.colostate.edu/wp-content/uploads/2025/06/QuickGuide_DayCloudConvectionRGB_final-1acc.pdf",
    "channels": {
        "R": {
            "formula": "C02 (Red, 0.64 um)",
            "clip": {"min_pct": 0, "max_pct": 100},
            "gamma": 1.7,
            "invert": False,
            "physical_relation": "Reflectance of clouds and surfaces",
            "contribution": {
                "small": "Water, vegetation, land",
                "large": "Cloud, snow, white sand",
            },
        },
        "G": {
            "formula": "C02 (Red, 0.64 um)",
            "clip": {"min_pct": 0, "max_pct": 100},
            "gamma": 1.7,
            "invert": False,
            "physical_relation": "Reflectance of clouds and surfaces (duplicate of red channel)",
            "contribution": {
                "small": "Water, vegetation, land",
                "large": "Cloud, snow, white sand",
            },
        },
        "B": {
            "formula": "C13 (Clean Longwave Window, 10.3 um)",
            "clip": {"min_C": -70.15, "max_C": 49.85},
            "gamma": 1.0,
            "invert": False,
            "physical_relation": "Surface or cloud top temperature",
            "contribution": {
                "small": "Warm: land (seasonal), ocean",
                "large": "Cold: land (winter), snow, high clouds",
            },
        },
    },
    "appearance": {
        "low_mid_water_clouds": "Shades of yellow",
        "snow": "Shades of yellow",
        "land_sparse_vegetation": "Olive green",
        "upper_level_clouds": "Shades of white and grey",
        "thin_cirrus": "Shades of blue-grey",
        "water_flooded_forested": "Dark blue",
    },
    "applications": [
        "Distinguishing high-level convective clouds from low/mid-level water clouds",
        "Providing a three-dimensional view of the atmosphere (IR as height proxy, VIS as structure detail)",
        "Revealing wind shear when animated",
        "Analyzing cloud height, vertical wind shear, and cloud vs snow in animation",
    ],
    "limitations": [
        "Daytime only (0.64 um band detects reflected solar radiation)",
        "Morning/evening sun angle and length of daylight affect color interpretation",
        "Snow and warm water clouds both appear yellow; geographic context or animation needed to differentiate",
        "Two-component pseudo RGB (red and green both use 0.64 um, providing duplicate rather than contrasting information)",
    ],
    "comparison": {
        "visible_064": "Thin cirrus, shallow cumulus, and snow are not always easy to differentiate "
                       "in single-channel 0.64 um visible; Day Cloud Convection RGB helps analyze "
                       "cloud height and vertical wind shear, especially in animation",
        "eumetsat_hrv_cloud": "Similar concept (HRV Cloud RGB) but uses gamma=1 for all components",
    },
    "heritage": "Can be produced for any meteorological satellite with one visible and one longwave IR channel",
    "time_of_day": "day"},

"cimss_natural_true_color": {
    "name": "CIMSS Natural True Color",
    "bands": [1, 2, 3],
    "source": "http://cimss.ssec.wisc.edu/goes/OCLOFactSheetPDFs/ABIQuickGuide_CIMSSRGB_v2.pdf",
    "channels": {
        "R": {
            "formula": "C02 (Red, 0.64 um)",
            "clip": None,
            "gamma": None,
            "invert": False,
            "physical_relation": "Visible reflectance",
        },
        "G": {
            "formula": "0.45 * C02 + 0.10 * C03 + 0.45 * C01",
            "clip": None,
            "gamma": None,
            "invert": False,
            "physical_relation": "Synthetic green approximation using Veggie band (0.86 um) "
                                 "which mimics enhanced reflectivity present in the true green band",
        },
        "B": {
            "formula": "C01 (Blue, 0.47 um)",
            "clip": None,
            "gamma": None,
            "invert": False,
            "physical_relation": "Visible reflectance",
        },
    },
    "appearance": {
        "vegetation_growing_season": "Green",
        "clouds": "White",
        "smoke": "Distinct brown color",
        "ocean_water": "Blue, or aquamarine",
        "snow": "White (similar to clouds; animation helps differentiate)",
        "suspended_sediment": "Visible in coastal waters (e.g. post-hurricane runoff)",
        "blowing_dust": "Distinct coloring visible against surface",
    },
    "applications": [
        "True color imagery approximating what would be seen from space",
        "Identifying phenomena with distinctive colors: snow cover, blowing dust, smoke, vegetation",
        "Monitoring surface changes (e.g. suspended sediment after hurricanes, vegetation changes)",
        "Generated from individual ABI bands only; no upstream preprocessing or extra files needed",
    ],
    "limitations": [
        "Daytime only (uses reflected solar light)",
        "ABI lacks a true green band; green is synthesized using Veggie (0.86 um) as a proxy "
        "for the enhanced reflectance over vegetation seen near 0.55 um",
        "No direct correction for atmospheric scattering effects",
        "Snow and clouds both appear white; animation helpful for differentiation",
    ],
    "comparison": {
        "cira_geocolor": "CIRA GeoColor is an alternative true color product available online",
        "himawari_true_color": "Himawari-8/9 AHI has a true green band at 0.51 um, "
                               "so no synthetic green approximation is needed",
    },
    "heritage": "Developed by CIMSS (Scott Lindstrom, Kaba Bah, Tim Schmit, Rick Kohrs)",
    "time_of_day": "day"},


"air_mass": {
    "name": "Air Mass",
    "bands": [8, 10, 12, 13],
    "source": "https://rammb2.cira.colostate.edu/wp-content/uploads/2020/01/QuickGuide_GOESR_AirMassRGB_final-1.pdf",
    "channels": {
        "R": {
            "formula": "C08 - C10 (Upper-Level Water Vapor - Lower-Level Water Vapor, 6.2 - 7.3 um)",
            "clip": {"min_C": -26.2, "max_C": 0.6},
            "gamma": 1.0,
            "invert": False,
            "physical_relation": "Vertical water vapor difference",
            "contribution": {
                "small": "Moist upper levels (cloud free)",
                "large": "Dry upper levels (cloud free)",
            },
        },
        "G": {
            "formula": "C12 - C13 (Ozone - Clean Longwave Window, 9.6 - 10.3 um)",
            "clip": {"min_C": -43.2, "max_C": 6.7},
            "gamma": 1.0,
            "invert": False,
            "physical_relation": "Tropopause height based on ozone",
            "contribution": {
                "small": "Low tropopause and high ozone (cloud free)",
                "large": "High tropopause and low ozone (cloud free)",
            },
        },
        "B": {
            "formula": "C08 (Upper-Level Water Vapor, 6.2 um)",
            "clip": {"min_C": -64.65, "max_C": -29.25},
            "gamma": 1.0,
            "invert": True,
            "physical_relation": "Water vapor approximately 200-500 mb",
            "contribution": {
                "small": "Dry upper levels (cloud free)",
                "large": "Moist upper levels (cloud free)",
            },
        },
    },
    "appearance": {
        "jet_stream_pv_dry_upper": "Dark red/orange",
        "cold_air_mass": "Dark blue/purple",
        "warm_air_mass": "Green",
        "warm_air_mass_less_moisture": "Olive/dark orange",
        "high_thick_cloud": "White",
        "mid_level_cloud": "Tan/salmon",
        "low_level_cloud": "Green, dark blue",
        "limb_effects": "Purple/blue",
    },
    "applications": [
        "Inferring cyclogenesis by identifying warm, dry, ozone-rich descending stratospheric air "
        "associated with jet streams and potential vorticity (PV) anomalies",
        "Validating location of PV anomalies in model data",
        "Distinguishing polar from tropical air masses, especially along upper-level frontal boundaries",
        "Identifying high-, mid-, and low-level clouds",
        "Tracking cyclogenesis as shortwaves approach and clouds form, evolve, and rotate",
        "Detecting upper-level moisture boundaries",
        "Inferring turbulence by identifying stratospheric intrusions",
    ],
    "limitations": [
        "Limb cooling: longer wavelength channels experience more atmospheric absorption at large viewing angles, "
        "producing cooler brightness temperatures and false blue/violet colors along the entire limb; "
        "tropical air can appear blue rather than green at the limb",
        "Upper troposphere only: mid- to upper-tropospheric conditions detectable but surface conditions "
        "cannot be directly observed",
        "Intense daytime heating over dry desert regions produces red/orange coloring in summer "
        "that does not indicate anomalous PV",
    ],
    "comparison": {
        "wv_6_2": "6.2 um water vapor channel can show air mass interactions, jet streaks, and deformation zones, "
                  "but air mass temperature and ozone content are not distinguishable in single-channel imagery",
    },
    "time_of_day": "both"},



"day_convection": {
    "name": "Day Convection",
    "bands": [2, 5, 7, 8, 10, 13],
    "source": "https://rammb2.cira.colostate.edu/wp-content/uploads/2020/01/QuickGuide_GOESR_DayConvectionRGB_final-1.pdf",
    "channels": {
        "R": {
            "formula": "C08 - C10 (Upper-Level Water Vapor - Lower-Level Water Vapor, 6.2 - 7.3 um)",
            "clip": {"min_C": -35.0, "max_C": 5.0},
            "gamma": 1.0,
            "invert": False,
            "physical_relation": "Cloud height",
            "contribution": {
                "small": "Low clouds",
                "large": "High clouds",
            },
        },
        "G": {
            "formula": "C07 - C13 (Shortwave Window - Clean Longwave Window, 3.9 - 10.3 um)",
            "clip": {"min_C": -5.0, "max_C": 60.0},
            "gamma": 1.0,
            "invert": False,
            "physical_relation": "Particle size",
            "contribution": {
                "small": "Large ice/water particles, weak updrafts",
                "large": "Small ice/water particles, strong updrafts",
            },
        },
        "B": {
            "formula": "C05 - C02 (Snow/Ice - Red, 1.6 - 0.64 um)",
            "clip": {"min_pct": -0.75, "max_pct": 0.25},
            "gamma": 1.0,
            "invert": False,
            "physical_relation": "Cloud phase",
            "contribution": {
                "small": "Ice clouds",
                "large": "Water clouds",
            },
        },
    },
    "appearance": {
        "strong_convection_small_ice": "Bright yellow",
        "moderate_convection_large_ice": "Orange",
        "weak_convection_large_ice": "Red",
        "low_mid_water_clouds": "Light blue",
        "mid_thick_small_particles": "Light green",
        "thin_cirrus_large_ice": "Pink",
        "thin_cirrus_small_ice": "Purple",
        "high_thick_large_ice": "Dark red",
    },
    "applications": [
        "Identifying intense updrafts indicating strong convection",
        "Nowcasting severe storms by detecting early-stage strong convection (bright yellow)",
        "Differentiating new convection (bright yellow) from mature/dissipating convection (orange/red)",
        "Determining microphysical characteristics of convective clouds to assess storm strength and stage",
    ],
    "limitations": [
        "Daytime only (relies on solar reflectance from visible, near-IR, and shortwave IR channels)",
        "Sun glint in the 3.9 um channel can falsely increase yellow coloring",
        "Pixel color fades during dawn/dusk when sun angle is low",
        "Very cold cloud tops with only moderate 3.9 um reflectivity can produce yellow without strong updrafts",
        "Mountain wave clouds or polluted air can also produce yellow",
        "Dust carried aloft can lead to long-lived small ice particles mimicking strong convection signals",
    ],
    "identification": {
        "strong_updrafts": "Smaller particles are more reflective at 3.9 um; within strong updrafts "
                           "particles do not have enough time to grow, producing large 3.9 um values. "
                           "Strong convection quickly saturates red and green, resulting in bright yellow.",
        "storm_lifecycle": "New/intense convection appears bright yellow; mature or dissipating convection "
                           "shifts to orange or red depending on particle size growth and cloud top warming.",
    },
    "comparison": {
        "visible_064": "Traditional 0.64 um visible can identify overshooting tops and convective clouds, "
                       "but Day Convection RGB distinguishes between newer (yellow) and dissipating (orange/red) convection",
    },
    "time_of_day": "day"},


"day_land_cloud": {
    "name": "Day Land Cloud (Natural Color)",
    "bands": [2, 3, 5],
    "source": "https://rammb2.cira.colostate.edu/wp-content/uploads/2020/01/QuickGuide_GOESR_daylandcloudRGB_final-1.pdf",
    "channels": {
        "R": {
            "formula": "C05 (Snow/Ice, 1.6 um)",
            "clip": {"min_pct": 0, "max_pct": 97.5},
            "gamma": 1.0,
            "invert": False,
            "physical_relation": "Reflectance of clouds and surfaces",
            "contribution": {
                "small": "Ice or large particle clouds, water, snow/ice, sea ice",
                "large": "Water clouds with small drops, and desert",
            },
        },
        "G": {
            "formula": "C03 (Veggie, 0.86 um)",
            "clip": {"min_pct": 0, "max_pct": 108.6},
            "gamma": 1.0,
            "invert": False,
            "physical_relation": "Reflectance of clouds and surfaces",
            "contribution": {
                "small": "Water, inactive vegetation, bare soil",
                "large": "Clouds, vegetation, and snow/ice",
            },
        },
        "B": {
            "formula": "C02 (Red, 0.64 um)",
            "clip": {"min_pct": 0, "max_pct": 100},
            "gamma": 1.0,
            "invert": False,
            "physical_relation": "Reflectance of clouds and surfaces",
            "contribution": {
                "small": "Thin cloud, water, vegetation, bare soil",
                "large": "Thick clouds and snow/ice",
            },
        },
    },
    "appearance": {
        "bare_land_inactive_vegetation": "Shades of brown",
        "vegetation": "Shades of green",
        "water_bodies_flooded": "Dark blue to black",
        "low_water_clouds": "Shades of grey and white",
        "high_ice_clouds": "Bright cyan",
        "snow": "Dark to bright cyan",
        "mid_mixed_phase_clouds": "Grey shades of cyan",
    },
    "applications": [
        "Discriminating water/ice cloud phase to identify low vs high clouds",
        "High ice clouds, snow, and sea ice appear cyan (ice absorbs strongly at 1.6 um, reducing red contribution)",
        "Low water clouds with small droplets (fog) appear grey/white (high reflectance in all three bands)",
        "Assessing vegetation health and detecting land surface changes (green vegetation, brown deserts/burn scars)",
    ],
    "limitations": [
        "Daytime only (relies on solar reflectance from visible and near-IR channels)",
        "Sun glint complicates water scenes (water appears grey to white when sun reflects toward satellite)",
        "Snow and ice clouds both appear bright cyan; geographic features or animation needed to differentiate",
        "Thin cirrus/cirrostratus are semi-transparent and difficult to detect with visible channels",
        "Dust appears similar color to bare land",
    ],
    "comparison": {
        "visible_064": "Cloud particle phase is not easy to discern in single-channel 0.64 um visible; "
                       "Day Land Cloud RGB distinguishes ice crystal clouds (bright cyan) from "
                       "liquid water clouds (grey/dull white)",
        "eumetsat_natural_color": "Day Land Cloud RGB is the same as the EUMETSAT Natural Color RGB",
    },
    "heritage": "Same as EUMETSAT Natural Color RGB",
    "time_of_day": "day"},



"day_land_cloud_fire": {
    "name": "Day Land Cloud/Fire",
    "bands": [2, 3, 6],
    "source": "https://rammb2.cira.colostate.edu/wp-content/uploads/2025/06/QuickGuide_GOESR_DayLandCloudFireRGB_final-1.pdf",
    "channels": {
        "R": {
            "formula": "C06 (Cloud Particle Size, 2.2 um)",
            "clip": {"min_pct": 0, "max_pct": 100},
            "gamma": 1.0,
            "invert": False,
            "physical_relation": "Particle size / land type",
            "contribution": {
                "small": "Large water/ice particles, water or snow",
                "large": "Small water/ice particles, hotspot",
            },
        },
        "G": {
            "formula": "C03 (Veggie, 0.86 um)",
            "clip": {"min_pct": 0, "max_pct": 100},
            "gamma": 1.0,
            "invert": False,
            "physical_relation": "Reflectance",
            "contribution": {
                "small": "Thin cloud, water, less green vegetation, bare soil",
                "large": "Thick cloud, highly vegetated, snow, desert",
            },
        },
        "B": {
            "formula": "C02 (Red, 0.64 um)",
            "clip": {"min_pct": 0, "max_pct": 100},
            "gamma": 1.0,
            "invert": False,
            "physical_relation": "Reflectance",
            "contribution": {
                "small": "Thin cloud, water, forest, bare soil",
                "large": "Thick cloud, snow, desert",
            },
        },
    },
    "appearance": {
        "inactive_vegetation_bare_land": "Olive green to browns",
        "vegetation": "Shades of green",
        "water_bodies_flooded": "Dark blue to black",
        "low_mid_clouds": "Grey shades of cyan",
        "high_thick_ice_clouds": "Bright cyan",
        "snow": "Cyan",
        "smoke": "Dark cyan",
        "hotspot_fire": "Red",
        "burn_scar": "Visible as land surface change",
    },
    "applications": [
        "Fire hotspot detection (2.2 um highlights fire intensity beyond what 3.9 um shows alone)",
        "Smoke plume identification",
        "Burn scar detection (vegetation/land change visible immediately and in future imagery)",
        "Snow/ice cover identification",
        "Land surface feature assessment (vegetation, desert, water bodies)",
    ],
    "limitations": [
        "Daytime only (relies on solar reflectance from visible and near-IR channels)",
        "Less ice/water cloud contrast than Day Land Cloud RGB: 2.2 um reflectance of medium to large "
        "cloud particles is very similar for water and ice, resulting in more overall cyan coloring; "
        "use a separate RGB when primarily interested in cloud phase",
        "Snow and high ice clouds both appear bright cyan; geographic features needed to differentiate",
        "Dust appears similar color to bare land",
        "Noise possible within convective clouds where reflectance exceeds 1",
    ],
    "comparison": {
        "day_land_cloud": "Same as Day Land Cloud RGB but replaces C05 (1.6 um) with C06 (2.2 um) in red channel; "
                          "this highlights fire hotspots but reduces water vs ice cloud discrimination",
        "ir_3_9": "Traditional 3.9 um detects fire hotspots; Day Land Cloud/Fire RGB additionally shows "
                  "fire intensity via 2.2 um and reveals burn scars through vegetation/land change",
        "eumetsat_natural_color": "Similar to EUMETSAT Natural Color RGB but with 2.2 um replacing 1.6 um",
    },
    "time_of_day": "day"},


"differential_water_vapor": {
    "name": "Differential Water Vapor",
    "bands": [8, 10],
    "source": "https://rammb2.cira.colostate.edu/wp-content/uploads/2020/01/QuickGuide_GOESR_DifferentialWaterVaporRGB_final-1.pdf",
    "channels": {
        "R": {
            "formula": "C10 - C08 (Lower-Level Water Vapor - Upper-Level Water Vapor, 7.3 - 6.2 um)",
            "clip": {"min_C": -3, "max_C": 30},
            "gamma": 0.2587,
            "invert": True,
            "physical_relation": "Vertical water vapor difference",
            "contribution": {
                "small": "Moist upper levels (cloud free)",
                "large": "Dry upper levels (cloud free)",
            },
        },
        "G": {
            "formula": "C10 (Lower-Level Water Vapor, 7.3 um)",
            "clip": {"min_C": -60, "max_C": 5},
            "gamma": 0.4,
            "invert": True,
            "physical_relation": "Low-level water vapor",
            "contribution": {
                "small": "Dry low levels (cloud free)",
                "large": "Moist lower levels (cloud free)",
            },
        },
        "B": {
            "formula": "C08 (Upper-Level Water Vapor, 6.2 um)",
            "clip": {"min_C": -64.65, "max_C": -29.25},
            "gamma": 0.4,
            "invert": True,
            "physical_relation": "Upper-level water vapor",
            "contribution": {
                "small": "Dry upper levels (cloud free)",
                "large": "Moist upper levels (cloud free)",
            },
        },
    },
    "appearance": {
        "very_dry_mid_upper": "Bright orange",
        "dry_mid_upper": "Orange",
        "dry_upper_moist_mid_or_mid_cloud": "Gold",
        "moderate_moisture_mid_upper": "Grey",
        "moist_upper": "Light teal",
        "thick_high_clouds": "White",
    },
    "applications": [
        "Identifying upper-level moisture boundaries",
        "Analyzing trough/ridge patterns with added dimension beyond single-channel WV",
        "Detecting potential vorticity (PV) anomalies and stratospheric air influence on rapid cyclogenesis",
        "Assessing tropopause fold-driven high-impact wind events",
        "Predicting hurricane intensity changes and extratropical transition by analyzing moist/dry layers",
        "Assessing depth of moist/dry layers using both lower-level and upper-level WV bands",
    ],
    "limitations": [
        "Range of orange tones may make it difficult to distinguish moisture layers at first glance; "
        "extra care needed when interpreting",
        "Cloud features are not distinct; only mid and high clouds identifiable, low contrast limits detail",
        "Limb cooling: longer wavelength channels experience more atmospheric absorption at large viewing angles, "
        "producing cooler brightness temperatures and false teal/white coloring along the entire limb",
    ],
    "comparison": {
        "wv_6_2": "6.2 um water vapor band identifies upper-level moisture but the RGB provides "
                  "additional information about dry air depth through mid and upper levels in a single image",
    },
    "time_of_day": "both"},


"dust": {
    "name": "Dust RGB",
    "bands": [11, 13, 14, 15],
    "source": "https://rammb2.cira.colostate.edu/wp-content/uploads/2020/01/Dust_RGB_Quick_Guide-1-1.pdf",
    "channels": {
        "R": {
            "formula": "C15 - C13 (Dirty Longwave Window - Clean Longwave Window, 12.3 - 10.3 um)",
            "clip": {"min_C": -6.7, "max_C": 2.6},
            "gamma": 1.0,
            "invert": False,
            "physical_relation": "Optical depth / cloud thickness",
            "contribution": {
                "small": "Thin clouds",
                "large": "Thick clouds or dust",
            },
        },
        "G": {
            "formula": "C14 - C11 (Longwave Window - Cloud-Top Phase, 11.2 - 8.4 um)",
            "clip": {"min_C": -0.5, "max_C": 20.0},
            "gamma": 2.5,
            "invert": False,
            "physical_relation": "Particle phase",
            "contribution": {
                "small": "Ice and particles of uniform shape (dust)",
                "large": "Water particles or thin cirrus over deserts",
            },
        },
        "B": {
            "formula": "C13 (Clean Longwave Window, 10.3 um)",
            "clip": {"min_C": -11.95, "max_C": 15.55},
            "gamma": 1.0,
            "invert": False,
            "physical_relation": "Surface temperature",
            "contribution": {
                "small": "Cold surface",
                "large": "Warm surface",
            },
        },
    },
    "appearance": {
        "dust_plume_day": "Bright magenta, pink",
        "dust_low_level_night": "Purple/plum (below ~3 km)",
        "dust_high_level_night": "Pink/magenta (changes with height)",
        "dust_very_thick": "Purple in both day and night",
        "thin_dust": "Barely visible",
        "desert_surface_day": "Light blue",
        "low_water_cloud": "Light purple",
        "mid_thick_clouds": "Tan shades",
        "mid_thin_cloud": "Green",
        "cold_thick_clouds": "Red",
        "high_thin_ice_clouds": "Black",
        "very_thin_cloud_warm_surface": "Blue",
        "cloud_free_dry_air": "Distinct from moist air coloring",
        "cloud_free_moist_air": "Distinct from dry air coloring",
    },
    "applications": [
        "Identifying dust plumes, distinguishable from surrounding clouds both day and night",
        "Detecting dust that is optically thin or appears similar to cirrus in single-channel imagery",
        "Night: inferring dust plume height from color changes (purple/plum at low levels, pink/magenta higher)",
        "Cloud height/type analysis",
        "Inferring air mass/moisture boundaries (low vs high humidity)",
        "Volcanic ash detection (appears orange/peach)",
    ],
    "limitations": [
        "Magenta/pink variations in daytime indicate density, not thickness; "
        "very thick plumes appear purple in both day and night",
        "Marine stratus over tropical oceans appears light purple, similar to dust, particularly at night",
        "High cloud cover can obscure dust plumes beneath and complicate spatial analysis",
    ],
    "comparison": {
        "ir_10_3": "Single-channel IR can identify dust if contrast with background is sufficient "
                   "(e.g. dust over hot desert) but detection becomes more difficult at night or over oceans",
        "dust_cvd": "Dust CVD RGB uses a color scheme more accessible to users with color vision deficiencies",
    },
    "time_of_day": "both"},


"fire_temperature": {
    "name": "Fire Temperature",
    "bands": [5, 6, 7],
    "source": "https://rammb2.cira.colostate.edu/wp-content/uploads/2020/01/Fire_Temperature_RGB-1.pdf",
    "channels": {
        "R": {
            "formula": "C07 (Shortwave Window, 3.9 um)",
            "clip": {"min_C": 0, "max_C": 60},
            "gamma": 0.4,
            "invert": False,
            "physical_relation": "Cloud top phase and temperature",
            "contribution": {
                "small": "Cold land surfaces, water, snow, clouds",
                "large": "Hot land surface (low fire temperature, saturated ~500 K)",
            },
        },
        "G": {
            "formula": "C06 (Cloud Particle Size, 2.2 um)",
            "clip": {"min_pct": 0, "max_pct": 100},
            "gamma": 1.0,
            "invert": False,
            "physical_relation": "Particle size / land type",
            "contribution": {
                "small": "Large ice/water particles, snow, oceans",
                "large": "Small ice/water particles (medium fire temperature)",
            },
        },
        "B": {
            "formula": "C05 (Snow/Ice, 1.6 um)",
            "clip": {"min_pct": 0, "max_pct": 75},
            "gamma": 1.0,
            "invert": False,
            "physical_relation": "Particle size / land type",
            "contribution": {
                "small": "Ice clouds with large particles, snow, oceans",
                "large": "Water clouds (high fire temperature, saturated ~1400 K)",
            },
        },
    },
    "appearance": {
        "warm_fire": "Red (only 3.9 um detects, ~500 K)",
        "very_warm_fire": "Orange",
        "hot_fire": "Yellow (2.2 um begins to saturate)",
        "very_hot_fire": "Near white (all channels saturated, ~1400 K)",
        "burn_scars": "Shades of maroon",
        "clear_sky_land": "Purples to pinks",
        "clear_sky_water_snow_night": "Near black",
        "water_clouds": "Shades of blue",
        "ice_clouds": "Shades of green",
    },
    "applications": [
        "Detecting fire hotspot locations (3.9 um saturation brightness temperature)",
        "Analyzing fire intensity (color transitions from red to yellow to white as intensity increases "
        "and near-IR channels saturate)",
        "Differentiating intense fires from cooler fires in a single product",
        "Burn scar identification (visible as maroon-colored surface change)",
    ],
    "limitations": [
        "Cloud cover blocks view of fires (only visible in clear-sky areas)",
        "Cloud features/type have less detail than dedicated cloud-analysis RGBs",
        "Daytime only for cloud features (near-IR reflectance not available at night), "
        "though fire hotspots are detectable day and night via 3.9 um",
        "False red fire signatures possible over arid/dry surfaces that are highly emissive at 3.9 um",
    ],
    "identification": {
        "intensity_scale": "Fires transition red -> orange -> yellow -> white as intensity increases; "
                           "background solar radiation increases from 3.9 to 1.6 um, so fires must be "
                           "more intense to be detected at shorter wavelengths",
        "saturation_physics": "Small/cool fires only detected at 3.9 um (red); "
                              "1.6 um channel saturates near 1400 K peak emission (white)",
    },
    "comparison": {
        "true_color": "True Color RGB shows smoke but does not distinguish fire intensity; "
                      "Fire Temperature RGB shows active fire location and behavior but misses smoke",
    },
    "time_of_day": "both"},


"simple_water_vapor": {
    "name": "Simple Water Vapor",
    "bands": [8, 10, 13],
    "source": "https://rammb2.cira.colostate.edu/wp-content/uploads/2020/01/Simple_Water_Vapor_RGB-1.pdf",
    "channels": {
        "R": {
            "formula": "C13 (Clean Longwave Window, 10.3 um)",
            "clip": {"min_C": -70.86, "max_C": 5.81, "min_K": 202.29, "max_K": 278.96},
            "gamma": None,
            "invert": True,
            "physical_relation": "Cloud top or surface temperature",
            "contribution": {
                "small": "Shallow low-mid clouds",
                "large": "High and/or deep clouds",
            },
        },
        "G": {
            "formula": "C08 (Upper-Level Water Vapor, 6.2 um)",
            "clip": {"min_C": -58.49, "max_C": -30.48, "min_K": 214.66, "max_K": 242.67},
            "gamma": None,
            "invert": True,
            "physical_relation": "Upper-level water vapor",
            "contribution": {
                "small": "Relatively dry upper-level atmosphere",
                "large": "Relatively moist upper-level atmosphere",
            },
        },
        "B": {
            "formula": "C10 (Lower-Level Water Vapor, 7.3 um)",
            "clip": {"min_C": -28.03, "max_C": -12.12, "min_K": 245.12, "max_K": 261.03},
            "gamma": None,
            "invert": True,
            "physical_relation": "Lower-level water vapor",
            "contribution": {
                "small": "Relatively dry low-mid level atmosphere",
                "large": "Relatively moist low-mid level atmosphere",
            },
        },
    },
    "appearance": {
        "low_mid_level_moisture": "Blue",
        "upper_level_moisture_dry_below": "Green",
        "moisture_all_levels_no_clouds": "Aqua",
        "dry_all_levels": "Black",
        "mid_level_cloud_moist_all_levels": "Milky pink",
        "mid_level_cloud_low_mid_moisture_dry_above": "Bright pink",
        "convective_initiation": "White",
        "high_thick_clouds": "White",
    },
    "applications": [
        "Detecting low-level moisture return in the absence of clouds (7.3 um channel contribution)",
        "Distinguishing depth of moisture without analyzing all water vapor bands individually",
        "Analyzing cyclone structure (warm, cold, and dry conveyor belts stand out)",
        "Identifying jet features (some easier to see in RGB than individual bands)",
        "Convective initiation detection",
        "Gravity waves, mountain waves, standing waves impacting turbulence",
    ],
    "limitations": [
        "Cannot detect fog/stratus or low-level features (very little contribution from lowest atmospheric layers)",
        "Upper cloud layer detail washes out due to gamma values and stretching applied to RGB components",
        "Limb effects: longer wavelength channels experience more atmospheric absorption at large viewing angles, "
        "producing cooler brightness temperatures and more white/aqua coloring at high viewing angles",
    ],
    "comparison": {
        "wv_7_3": "7.3 um water vapor band shows low-level moisture return, but the RGB allows "
                  "distinguishing the depth of moisture in a single image without analyzing all WV bands separately",
    },
    "time_of_day": "both"}
}



# Band difference products for specialized analysis
# These are mathematical differences between two bands that highlight specific atmospheric features
# Includes physical interpretation and application guidance
COMPOSITES_DIFFERENCE = {
"split_water_vapor": {
    "name": "Split Water Vapor",
    "formula": "C08 - C10",
    "bands": [8, 10],
    "source": "https://cimss.ssec.wisc.edu/goes/OCLOFactSheetPDFs/ABIQuickGuide_SplitWV_BTDiffv2.pdf",
    "description": "Upper-Level Water Vapor minus Lower-Level Water Vapor (6.2 - 7.3 um)",
    "units": "K",
    "physics": "Energy at 6.2 um is absorbed more readily by water vapor than at 7.3 um. "
               "The brightness temperature difference approximates the concentration and "
               "distribution of water vapor between mid and upper levels.",
    "appearance": {
        "thick_high_clouds": "Near zero (BTD is small, especially if atmosphere above is dry)",
        "high_level_moisture_small_amounts": "Very large negative values",
        "very_dry_atmosphere": "Negative values",
    },
    "applications": [
        "Approximating concentration and distribution of mid- to upper-level water vapor",
        "Red component of the Air Mass RGB (values between -26.2 K and 0.9 K)",
        "Detecting thin cirrus (detected in 6.2 um but transparent in 7.3 um, producing large BTD)",
        "Identifying tropopause folds (typically large BTD values in clear regions)",
        "Deducing cloud height information when one channel sees a cloud but the other does not",
    ],
    "limitations": [
        "WV bands sense the mean temperature of a moisture layer whose altitude and depth vary "
        "with temperature/moisture profile and satellite viewing angle (weighting functions describe "
        "where in the atmosphere the energy originates)",
        "BTD is small over thick clouds, especially with dry atmosphere above, limiting information content",
    ],
    "time_of_day": "both"},

"split_ozone": {
    "name": "Split Ozone",
    "formula": "C12 - C13",
    "bands": [12, 13],
    "source": "https://cimss.ssec.wisc.edu/goes/OCLOFactSheetPDFs/ABIQuickGuide_SplitOzoneDiff.pdf",
    "description": "Ozone minus Clean Longwave Window (9.6 - 10.3 um)",
    "units": "K",
    "physics": "Reveals the influence of ozone absorption compared to the clean window. "
               "The sign of the BTD is controlled by the temperature of the emitting surface "
               "relative to the temperature of ozone in the stratosphere. The 9.6 um weighting "
               "function has a peak near the surface and a peak in the stratosphere; the 10.3 um "
               "has a peak at the surface only.",
    "appearance": {
        "high_cold_clouds": "Positive or near-zero values (cloud tops colder than stratosphere)",
        "clear_sky_warm_water": "Large negative values",
        "clear_sky_land": "Negative values; magnitude depends on surface temperature",
        "low_clouds": "Negative values; magnitude depends on cloud temperature",
    },
    "applications": [
        "Green component of the Air Mass RGB composite",
        "Identifying very high clouds (positive BTD where cold cloud tops contrast with warmer stratosphere)",
        "Distinguishing high clouds from near-surface features (sign of BTD changes)",
    ],
    "limitations": [
        "Cannot by itself determine presence or concentration of stratospheric ozone "
        "(multi-band ozone retrievals are needed for that)",
        "ABI broad spectral bands are not sensitive to low-level (near-surface) ozone",
    ],
    "time_of_day": "both"},


"split_cloud_phase": {
    "name": "Split Cloud Phase",
    "formula": "C14 - C11",
    "bands": [11, 14],
    "source": "http://cimss.ssec.wisc.edu/goes/OCLOFactSheetPDFs/ABIQuickGuide_G16_CloudPhaseBTD.pdf",
    "description": "Longwave Window minus Cloud-Top Phase (11.2 - 8.4 um)",
    "units": "K",
    "physics": "The emissivity of ice varies with particle size at 8.4 um, allowing the BTD "
               "to differentiate between thick and thin cirrus and between ice and water clouds. "
               "The 8.4 um band also has sensitivity to SO2, so the BTD can show a signal in "
               "volcanic plumes.",
    "appearance": {
        "thin_cirrus_small_ice": "Large negative values",
        "thick_cirrus_large_ice": "Small negative values",
        "water_droplet_clouds": "Positive values",
        "blowing_dust": "Small positive values",
        "volcanic_plume_so2": "Positive values (but negative BTD plumes can also arise from non-volcanic events)",
        "land_surface_clear": "Variable; depends on surface moisture, temperature, and vegetation/drought conditions",
    },
    "applications": [
        "Differentiating thin cirrus (large negative) from thick cirrus (small negative)",
        "Distinguishing ice clouds (negative) from water droplet clouds (positive)",
        "Detecting blowing dust (small positive values)",
        "Detecting volcanic plumes containing SO2 (8.4 um sensitivity to SO2)",
        "Used in both the Ash RGB and Dust RGB products",
        "Related to baseline Cloud Particle Size and Cloud Top Phase derived products",
    ],
    "limitations": [
        "Changes in difference field over land can be affected by surface moisture or temperature changes",
        "Optically thin cirrus are hard to detect",
        "Supercooled water clouds are difficult to interpret",
        "Over hot land, more transmissivity of upwelling 8.4 um radiation through cirrus increases BTD; "
        "BTD will be somewhat less over cooler land",
        "Negative BTD plumes can arise from non-volcanic events; do not use BTD alone to identify eruptions",
    ],
    "time_of_day": "both" },

"split_snow": {
    "name": "Split Snow",
    "formula": "C05 - C02",
    "bands": [2, 5],
    "source": "http://cimss.ssec.wisc.edu/goes/OCLOFactSheetPDFs/ABIQuickGuide_SplitSnowv2.pdf",
    "description": "Snow/Ice minus Red (1.6 - 0.64 um)",
    "units": "reflectance",
    "physics": "Highlights regions where significant differences exist between visible (0.64 um) "
               "and near-infrared (1.61 um) reflectances. Ice (whether as glaciated cloud, "
               "snow/ice on the ground, or lofted blowing snow) is more reflective at 0.64 um "
               "than at 1.61 um, producing negative values. Land is more reflective at 1.61 um, "
               "producing positive values.",
    "appearance": {
        "cirrus_clouds": "Large negative values",
        "water_clouds": "Small negative values",
        "snow_on_ground": "Negative values",
        "clear_land": "Large positive values (land more reflective at 1.61 um)",
        "clear_water": "Near zero",
        "cloud_shadows": "Negative (shadows darker at 1.61 um)",
    },
    "sign_interpretation": {
        "positive": "Cloud-free land (land more reflective at 1.61 um)",
        "negative": "Cloud-free water, glaciated clouds, snow cover, cloud shadows "
                    "(all more reflective at 0.64 um)",
    },
    "applications": [
        "Differentiating glaciated cloud (or snow/blowing snow) from water droplet clouds",
        "Identifying snow-covered vs open lakes (very different reflectance signatures)",
        "Detecting land/water boundaries",
    ],
    "limitations": [
        "Daytime only (reflectance difference product)",
        "Very hot fires can emit significant 1.61 um radiation, affecting the difference field",
        "Component bands have different spatial resolutions; important to consider when comparing "
        "the difference field to individual component fields",
        "Differences decrease near sunset as reflected radiation decreases",
    ],
    "time_of_day": "day"},

"night_fog": {
    "name": "Night Fog (Stratus) Difference",
    "formula": "C13 - C07",
    "bands": [7, 13],
    "source": "http://cimss.ssec.wisc.edu/goes/OCLOFactSheetPDFs/ABIQuickGuide_NightFogBTD.pdf",
    "description": "Clean Longwave Window minus Shortwave Window (10.3 - 3.9 um)",
    "units": "K",
    "physics": "Stratus/water droplet clouds do not emit 3.9 um radiation as a blackbody but "
               "emit 10.3 um radiation nearly as a blackbody. This emissivity difference produces "
               "a positive BTD at night over water droplet clouds. During daytime, small ice/water "
               "crystals are more highly reflective at 3.9 um, so the BTD gives particle size information "
               "and the sign flips.",
    "sign_interpretation": {
        "night_positive": "Clouds made up of water droplets (emissivity difference)",
        "night_negative": "Clouds made up of ice crystals",
        "day_negative": "Small ice or water particles, strong updrafts (reflectivity difference)",
    },
    "appearance": {
        "water_droplet_clouds_night": "Positive values (low stratus decks stand out clearly)",
        "ice_clouds_night": "Negative values",
        "strong_updrafts_day": "Minimum (large negative) over convective towers where small ice crystals "
                               "strongly reflect solar 3.9 um radiation",
    },
    "applications": [
        "Nighttime identification of clouds made of small water droplets (low stratus, fog)",
        "Daytime identification of strong convective updrafts via particle size information",
        "Component of the Nighttime Microphysics RGB (green channel)",
    ],
    "limitations": [
        "Commonly called the 'Fog Product' but is really the 'Stratus Product'; satellite sees only "
        "cloud top and gives no specific information about the cloud base",
        "Low clouds cannot be identified if high clouds are present (satellite views top-most cloud deck)",
        "Default AWIPS enhancement used at night must be changed for useful daytime information",
    ],
    "time_of_day": "both" },


"split_window": {
    "name": "Split Window Difference",
    "formula": "C13 - C15",
    "bands": [13, 15],
    "source": "http://cimss.ssec.wisc.edu/goes/OCLOFactSheetPDFs/ABIQuickGuide_SplitWindowDifference.pdf",
    "description": "Clean Longwave Window minus Dirty Longwave Window (10.3 - 12.3 um)",
    "units": "K",
    "physics": "Water vapor absorbs more energy at 12.3 um (Dirty Window) than at 10.3 um (Clean Window), "
               "so moisture in the atmosphere produces positive BTD values. Airborne silicates (dust) "
               "absorb more 10.3 um energy than 12.3 um energy, producing negative BTD values.",
    "sign_interpretation": {
        "positive": "Moisture in the atmosphere (water vapor absorbing 12.3 um energy)",
        "negative": "Dust in the atmosphere (silicate particles absorbing 10.3 um energy)",
    },
    "appearance": {
        "moist_atmosphere": "Positive values; gradients in SWD highlight moisture gradients",
        "dust": "Negative values (10.3 um BT colder than 12.3 um BT due to silicate absorption)",
    },
    "applications": [
        "Identifying gradients in low-level moisture or detecting atmospheric moistening",
        "Detecting regions of low-level dust",
        "Moisture convergence axis identification (convection may develop along SWD axes)",
        "Features in SWD correspond to other moisture measures (TPW, CAPE)",
    ],
    "limitations": [
        "If dust occurs in a moist environment, the cooling effects of water vapor and silicates "
        "can balance each other, masking both signals",
        "Changes in the difference field can be affected by changes in moisture or temperature or both; "
        "especially true as heating erodes inversions after sunrise",
    ],
    "time_of_day": "both" },


}

# ==========================================================================
# Convenience accessors
# ==========================================================================

def list_rgb_composites() -> list:
    """Return a list of all available RGB composite names."""
    return list(COMPOSITES_RGB.keys())  # Returns list of all RGB composite names


def list_differences() -> list:
    """Return a list of all available difference product names."""
    return list(COMPOSITES_DIFFERENCE.keys())  # Returns list of all difference product names


def list_bands() -> list:
    """Return a sorted list of all ABI band numbers."""
    return sorted(BAND_INFO.keys())  # Returns sorted list of ABI band numbers (1-16)


def get_rgb(name: str) -> dict:
    """Look up an RGB composite recipe by name.

    Parameters
    ----------
    name : str
        Composite key (e.g. "ash", "blowing_snow", "day_convection").

    Returns
    -------
    dict

    Raises
    ------
    KeyError
        If *name* is not a recognized composite.
    """
    if name not in COMPOSITES_RGB:
        available = ", ".join(COMPOSITES_RGB.keys())
        raise KeyError(f"Unknown RGB composite '{name}'. Available: {available}")
    return COMPOSITES_RGB[name]  # Returns complete RGB composite recipe


def get_difference(name: str) -> dict:
    """Look up a difference product recipe by name.

    Parameters
    ----------
    name : str
        Difference product key (e.g. "night_fog", "split_cloud_phase").

    Returns
    -------
    dict

    Raises
    ------
    KeyError
        If *name* is not a recognized difference product.
    """
    if name not in COMPOSITES_DIFFERENCE:
        available = ", ".join(COMPOSITES_DIFFERENCE.keys())
        raise KeyError(f"Unknown difference product '{name}'. Available: {available}")
    return COMPOSITES_DIFFERENCE[name]  # Returns complete difference product recipe


def get_band(band: int) -> dict:
    """Look up band metadata by number.

    Parameters
    ----------
    band : int
        ABI band number (1 through 16).

    Returns
    -------
    dict

    Raises
    ------
    KeyError
        If *band* is not a valid ABI band number.
    """
    if band not in BAND_INFO:
        raise KeyError(f"Unknown band {band}. Valid bands: 1-16")
    return BAND_INFO[band]  # Returns complete band metadata


def bands_for(name: str) -> list:
    """Return the sorted list of band numbers needed for a product.

    Searches both ``COMPOSITES_RGB`` and ``COMPOSITES_DIFFERENCE``.

    Parameters
    ----------
    name : str
        Key from either dict.

    Returns
    -------
    list of int

    Raises
    ------
    KeyError
        If *name* is not found in either dictionary.
    """
    if name in COMPOSITES_RGB:
        return sorted(COMPOSITES_RGB[name]["bands"])  # Return sorted bands for RGB composite
    if name in COMPOSITES_DIFFERENCE:
        return sorted(COMPOSITES_DIFFERENCE[name]["bands"])  # Return sorted bands for difference product
    all_keys = list(COMPOSITES_RGB.keys()) + list(COMPOSITES_DIFFERENCE.keys())
    raise KeyError(f"Unknown product '{name}'. Available: {', '.join(all_keys)}")


def print_recipe(name: str) -> None:
    """Print a human-readable summary of a product.

    Works for RGB composites, difference products, and individual bands
    (pass band number as string, e.g. "1" or as int).

    Parameters
    ----------
    name : str or int
        Key from ``COMPOSITES_RGB``, ``COMPOSITES_DIFFERENCE``, or
        a band number (int or string of int).
    """
    # Check if it's a band number
    try:
        band_num = int(name)
        if band_num in BAND_INFO:
            _print_band(band_num)
            return
    except (ValueError, TypeError):
        pass

    if name in COMPOSITES_RGB:
        _print_rgb(name)
    elif name in COMPOSITES_DIFFERENCE:
        _print_difference(name)
    else:
        all_keys = (
            list(COMPOSITES_RGB.keys())
            + list(COMPOSITES_DIFFERENCE.keys())
            + [str(b) for b in BAND_INFO.keys()]
        )
        raise KeyError(f"Unknown product '{name}'. Available: {', '.join(all_keys)}")


def _print_band(band: int) -> None:
    """Print a human-readable summary of a single ABI band."""
    entry = BAND_INFO[band]  # Get band metadata from dictionary
    print(f"=== Band {band}: {entry['name']} ({entry['wavelength_um']} um) ===")
    print(f"Type: {entry['type']}")
    print(f"Resolution: {entry['resolution_km']} km")
    if "nickname" in entry:
        print(f"Nickname: {entry['nickname']}")
    if "source" in entry:
        print(f"Source: {entry['source']}")
    print()
    if "physics" in entry:
        print(f"Physics: {entry['physics']}")
        print()
    if "applications" in entry:
        print("Applications:")
        for app in entry["applications"]:
            print(f"  - {app}")
        print()
    if "limitations" in entry:
        print("Limitations:")
        for lim in entry["limitations"]:
            print(f"  - {lim}")


def _print_rgb(name: str) -> None:
    """Print a human-readable summary of an RGB composite."""
    entry = COMPOSITES_RGB[name]  # Get RGB composite recipe from dictionary
    print(f"=== {entry['name']} RGB ===")
    print(f"Bands: {', '.join(f'C{b:02d}' for b in entry['bands'])}")
    print(f"Time of day: {entry.get('time_of_day', 'unknown')}")
    if "source" in entry:
        print(f"Source: {entry['source']}")
    if "heritage" in entry:
        print(f"Heritage: {entry['heritage']}")
    print()

    for ch_name, ch in entry["channels"].items():
        parts = [f"  {ch_name}: {ch['formula']}"]
        if ch.get("gamma"):
            parts.append(f"gamma={ch['gamma']}")
        if ch.get("invert"):
            parts.append("inverted")
        print(", ".join(parts))

        clip = ch.get("clip")
        if clip:
            clip_str = ", ".join(f"{k}={v}" for k, v in clip.items())
            print(f"       clip: [{clip_str}]")

        if "physical_relation" in ch:
            print(f"       relates to: {ch['physical_relation']}")

        contrib = ch.get("contribution", {})
        for level, desc in contrib.items():
            print(f"       {level}: {desc}")
    print()

    for section in ["appearance", "applications", "limitations", "best_practices"]:
        if section not in entry:
            continue
        print(f"{section.replace('_', ' ').title()}:")
        val = entry[section]
        if isinstance(val, dict):
            for feature, desc in val.items():
                print(f"  {feature}: {desc}")
        elif isinstance(val, list):
            for item in val:
                print(f"  - {item}")
        print()

    for section in ["identification", "comparison"]:
        if section not in entry:
            continue
        print(f"{section.title()}:")
        for key, desc in entry[section].items():
            print(f"  {key}: {desc}")
        print()


def _print_difference(name: str) -> None:
    """Print a human-readable summary of a difference product."""
    entry = COMPOSITES_DIFFERENCE[name]
    print(f"=== {entry['name']} ===")
    print(f"Formula: {entry['formula']}")
    print(f"Bands: {', '.join(f'C{b:02d}' for b in entry['bands'])}")
    print(f"Units: {entry['units']}")
    if "source" in entry:
        print(f"Source: {entry['source']}")
    print(f"Time of day: {entry.get('time_of_day', 'unknown')}")
    print()
    print(f"Description: {entry['description']}")
    if "physics" in entry:
        print(f"Physics: {entry['physics']}")
    print()

    for section in ["sign_interpretation", "appearance", "applications", "limitations"]:
        if section not in entry:
            continue
        print(f"{section.replace('_', ' ').title()}:")
        val = entry[section]
        if isinstance(val, dict):
            for feature, desc in val.items():
                print(f"  {feature}: {desc}")
        elif isinstance(val, list):
            for item in val:
                print(f"  - {item}")
        print()