from model.model import PVGIS

if __name__ == "__main__":
    # country_list = read_data_excel("NUTS2021")["nuts0"].unique()
    country_list = ['DE']
    year_no_pv_gis_data = []
    for year in range(2016, 2017):
        pv_gis = PVGIS(year=year)
        l = pv_gis.download_pv_gis(country_list)
        year_no_pv_gis_data.extend(l)
    print(f'years with no pv gis data: {year_no_pv_gis_data}')