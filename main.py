from model.model import PVGIS

if __name__ == "__main__":
    # country_list = read_data_excel("NUTS2021")["nuts0"].unique()
    country_list = ['DE']
    pv_gis = PVGIS()
    pv_gis.download_pv_gis(country_list)