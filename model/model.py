import urllib.error
from typing import List

import geopandas as gpd
import numpy as np
import pandas as pd
from calendar import isleap
import sqlalchemy
from pyproj import CRS, Transformer

from tqdm import tqdm

from model.utils import read_data_excel, save_data, Var

var = Var()


class PVGIS:

    def __init__(self, year: int = 2019): 
        # start_year must be equal to end_year
        # if not we get error from 'self.get_pv_gis_data()' because the result dict is not updated properly (not right length of different columns)
        self.total_hours = self.get_total_hours(year)
        self.id_hour = np.arange(1, self.total_hours+1)
        self.start_year = year
        self.end_year = year
        self.pv_calculation: int = 1
        self.peak_power: float = 1
        self.pv_loss: int = 14
        self.pv_tech: str = "crystSi"
        self.tracking_type: int = 0
        self.angle: int = 90
        self.optimal_inclination: int = 1
        self.optimal_angle: int = 1
        self.data_types = {
            var.region: sqlalchemy.types.Unicode,
            var.year: sqlalchemy.types.BigInteger,
            var.id_hour: sqlalchemy.types.BigInteger,
            var.pv_generation: sqlalchemy.types.Float,
            var.pv_generation_unit: sqlalchemy.types.Unicode,
            var.temperature: sqlalchemy.types.Float,
            var.temperature_unit: sqlalchemy.types.Unicode,
            var.radiation_south: sqlalchemy.types.Float,
            var.radiation_east: sqlalchemy.types.Float,
            var.radiation_west: sqlalchemy.types.Float,
            var.radiation_north: sqlalchemy.types.Float,
            var.radiation_unit: sqlalchemy.types.Unicode
        }

    """
    pv_calculation: No = 0; Yes = 1
    peak_power: size of PV (in kW_peak)
    pv_loss: system losses in %
    pv_tech: "crystSi", "CIS", "CdTe" and "Unknown".
    tracking_type: type of sun-tracking used,
                    - fixed = 0
                    - single horizontal axis aligned north-south = 1
                    - two-axis tracking = 2
                    - vertical axis tracking = 3
                    - single horizontal axis aligned east-west = 4
                    - single inclined axis aligned north-south = 5
    angle: inclination angle from horizontal plane, which is set to 90° because we are looking at a vertical plane.
    optimal_inclination: Yes = 1, meaning to calculate the optimum inclination angle.
                         All other values (or no value) mean "no". Not relevant for 2-axis tracking.
    optimal_angle: Yes = 1, meaning to calculate the optimum inclination AND orientation angles.
                   All other values (or no value) mean "no". Not relevant for tracking planes.
    """

    def scalar2array(self, value):
        return [value for _ in range(0, self.total_hours)]
    
    @staticmethod
    def get_total_hours(year):
        if isleap(year):
            return 8784
        else:
            return 8760

    @staticmethod
    def get_url_region_geo_center(nuts_level: int):
        if nuts_level in [0, 1, 2, 3]:
            return f'https://gisco-services.ec.europa.eu/distribution/v2/nuts/geojson/' \
                   f'NUTS_RG_60M_2021_3035_LEVL_{nuts_level}.geojson'
        else:
            raise Exception(f'Wrong input for NUTS level.')

    def get_geo_center(self, region: str):
        nuts_level = self.get_nuts_level(region)
        nuts = gpd.read_file(self.get_url_region_geo_center(nuts_level))
        transformer = Transformer.from_crs(CRS("EPSG:3035"), CRS("EPSG:4326"))
        point = nuts[nuts.NUTS_ID == region].centroid.values[0]
        lat, lon = transformer.transform(point.y, point.x)
        return lat, lon

    @staticmethod
    def get_nuts_level(region: str):
        return int(len(region) - 2)

    def get_pv_generation(self, region: str) -> np.array:
        pv_generation_dict = {}
        self.pv_calculation = 1
        self.optimal_inclination = 1
        self.optimal_angle = 1
        lat, lon = self.get_geo_center(region)
        req = f"https://re.jrc.ec.europa.eu/api/v5_2/seriescalc?lat={lat}&lon={lon}&" \
              f"startyear={self.start_year}&" \
              f"endyear={self.end_year}&" \
              f"pvcalculation={self.pv_calculation}&" \
              f"peakpower={self.peak_power}&" \
              f"loss={self.pv_loss}&" \
              f"pvtechchoice={self.pv_tech}&" \
              f"components={1}&" \
              f"trackingtype={self.tracking_type}&" \
              f"optimalinclination={self.optimal_inclination}&" \
              f"optimalangles={self.optimal_angle}"
        try:
            # Read the csv from api and use 20 columns to receive the source, because depending on the parameters,
            # the number of columns could vary. Empty columns are dropped afterwards:
            df = pd.read_csv(req, sep=",", header=None, names=range(20), low_memory=False).dropna(how="all", axis=1)
            df = df.dropna().reset_index(drop=True)
            # set header to first row
            header = df.iloc[0]
            df = df.iloc[1:, :]
            df.columns = header
            df = df.reset_index(drop=True)
            pv_generation_dict[var.pv_generation] = pd.to_numeric(df["P"]).to_numpy()  # unit: W
            pv_generation_dict[var.pv_generation_unit] = self.scalar2array(var.pv_generation_unit_string)
            return pv_generation_dict
        except urllib.error.HTTPError:
            print(f"pv_generation source is not available for region {region}.")

    def get_temperature_and_solar_radiation(self, region, aspect) -> pd.DataFrame:

        self.pv_calculation = 0
        self.optimal_inclination = 0
        self.optimal_angle = 0
        lat, lon = self.get_geo_center(region)
        req = f"https://re.jrc.ec.europa.eu/api/v5_2/seriescalc?lat={lat}&lon={lon}&" \
              f"startyear={self.start_year}&" \
              f"endyear={self.end_year}&" \
              f"pvcalculation={self.pv_calculation}&" \
              f"peakpower={self.peak_power}&" \
              f"loss={self.pv_loss}&" \
              f"pvtechchoice={self.pv_tech}&" \
              f"components={1}&" \
              f"trackingtype={self.tracking_type}&" \
              f"optimalinclination={self.optimal_inclination}&" \
              f"optimalangles={self.optimal_angle}&" \
              f"angle={self.angle}&" \
              f"aspect={aspect}"

        # Read the csv from api and use 20 columns to receive the source, because depending on the parameters,
        # the number of columns could vary. Empty columns are dropped afterwards:
        try:
            df = pd.read_csv(req, sep=",", header=None, names=range(20), low_memory=False).dropna(how="all", axis=1)
            df = df.dropna().reset_index(drop=True)
            # set header to first row
            header = df.iloc[0]
            df = df.iloc[1:, :]
            df.columns = header
            return df
        except urllib.error.HTTPError:
            pass

    def get_temperature(self, region: str) -> dict:
        temperature_dict = {}
        try:
            df = self.get_temperature_and_solar_radiation(region, 0)
            temperature_dict[var.temperature] = pd.to_numeric(df["T2m"].reset_index(drop=True)).values
            temperature_dict[var.temperature_unit] = self.scalar2array(var.temperature_unit_string)
            return temperature_dict
        except Exception as e:
            print(f"Temperature source is not available for region {region}.")

    def get_radiation(self, region) -> dict:
        radiation_dict = {}
        celestial_direction_aspect = {
            var.south: 0,
            var.east: -90,
            var.west: 90,
            var.north: -180
        }
        try:
            for direction, aspect in celestial_direction_aspect.items():
                df = self.get_temperature_and_solar_radiation(region, aspect)
                radiation = pd.to_numeric(df["Gb(i)"]) + pd.to_numeric(df["Gd(i)"])
                radiation_dict[var.radiation_prefix + direction] = radiation.reset_index(drop=True).to_numpy()
            radiation_dict[var.radiation_unit] = self.scalar2array(var.radiation_unit_string)
            return radiation_dict
        except Exception as e:
            print(f"Radiation source is not available for region {region}.")

    def get_pv_gis_data(self, region) -> pd.DataFrame:
        result_dict = {
            var.region: self.scalar2array(region),
            var.year: self.start_year,
            var.id_hour: self.id_hour,
        }

        pv_generation_dict = self.get_pv_generation(region)
        temperature_dict = self.get_temperature(region)
        radiation_dict = self.get_radiation(region)

        try:
            assert pv_generation_dict[var.pv_generation].sum() != 0
            assert temperature_dict[var.temperature].sum() != 0
            assert radiation_dict[var.radiation_south].sum() != 0
            assert radiation_dict[var.radiation_east].sum() != 0
            assert radiation_dict[var.radiation_west].sum() != 0
            assert radiation_dict[var.radiation_north].sum() != 0
            result_dict.update(pv_generation_dict)
            result_dict.update(temperature_dict)
            result_dict.update(radiation_dict)
            result_df = pd.DataFrame.from_dict(result_dict)
            return result_df
        except Exception as e:
            print(f"At least one pv_gis source of Region {region} includes all zeros.")

    def download_pv_gis(self, countries: List[str]):
        nuts = read_data_excel("NUTS2021")
        country_no_pv_gis_data = []
        year_no_pv_gis_data = []
        
        for country in countries:
            nuts1 = nuts.loc[nuts["nuts0"] == country]["nuts1"].unique()

            for region in nuts1:
                print(f'Downloading: {self.start_year} - {country} - {region}.')

                country_pv_gis_list = []
                nuts3 = nuts.loc[nuts["nuts1"] == region]["nuts3"].to_list()

                for subregion in tqdm(nuts3):
                    subregion_df = self.get_pv_gis_data(subregion)
                    country_pv_gis_list.append(subregion_df)
                try:
                    country_pv_gis_df = pd.concat(country_pv_gis_list)
                    save_data(country_pv_gis_df, f"pv_gis_{country}_{region}_{self.start_year}_nuts3")
                except Exception as e:
                    year_no_pv_gis_data.append(self.start_year)
                    country_no_pv_gis_data.append(country)
        print(f'country_no_pv_gis_data: {country_no_pv_gis_data}')
        return year_no_pv_gis_data
