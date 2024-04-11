from pathlib import Path
from dataclasses import dataclass
import pandas as pd

@dataclass
class Var:
    region = "region"
    year = "year"
    id_hour = "id_hour"
    pv_generation = "pv_generation"
    pv_generation_unit = "pv_generation_unit"
    pv_generation_unit_string = 'W/kW_peak'
    temperature = "temperature"
    temperature_unit = "temperature_unit"
    temperature_unit_string = "°C"
    south = "south"
    east = "east"
    west = "west"
    north = "north"
    radiation_prefix = "radiation_"
    radiation_south = "radiation_south"
    radiation_east = "radiation_east"
    radiation_west = "radiation_west"
    radiation_north = "radiation_north"
    radiation_unit = "radiation_unit"
    radiation_unit_string = "W"


def read_data_excel(file_name: str, sheet_name: str = None):
    folder = "data"
    if sheet_name is None:
        df = pd.read_excel(folder / Path(file_name + ".xlsx"))
    else:
        df = pd.read_excel(folder / Path(file_name + ".xlsx"), sheet_name=sheet_name)
    return df


def save_data(df: pd.DataFrame, file_name: str):
    df.to_csv(os.path.join("data", f"{file_name}.csv"), index=False)


