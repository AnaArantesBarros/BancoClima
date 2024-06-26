import os
import pandas as pd
from sqlalchemy import exists
from datetime import datetime
from ppt.config.database.tables import INMET
from ppt.earth_engine_helper import authenticate
from ppt.config.parameters import (start_date, end_date)
from ppt.transformations import (compile_year,inmet_download_unzip, extract_ppt,
                                 date_range, ppt_nasa)
from ppt.config.database.connector import (generate_database_session)

authenticate(project='ee-anaarantes')
session = generate_database_session()

#Importando arquivo parquet com as estações
path = os.path.dirname(os.path.realpath(__file__))
parquet_file = os.path.join(path, 'Files/stations_inmet_sp.parquet')
stations_sp = pd.read_parquet(parquet_file)

run_INMET = False
run_satelite = False

#Populando o banco INMET (dados brutos)
if run_INMET == True:
    for year in range(2000, 2025):
        inmet_download_unzip(year, path)
        df_year = compile_year(year, path)
        data_to_insert = []
        
        # Loop pelas linhas do DataFrame
        for index, row in df_year.iterrows():
            key = row['KEY']
            if not session.query(exists().where(INMET.KEY == key)).scalar():
                inmet_data = INMET(
                    KEY=key,
                    DATE=row['DATE'],
                    CODE=row['CODE'],
                    LATITUDE=float(row['LATITUDE'].replace(',', '.')),
                    LONGITUDE=float(row['LONGITUDE'].replace(',', '.')),
                    STATION=row['STATION'],
                    UF=row['UF'],
                    PPT_mm=float(row['PPT_mm'])
                )
                data_to_insert.append(inmet_data)
        
        # Inserir os dados em lotes (ano)
        if data_to_insert:
            session.bulk_save_objects(data_to_insert)
            session.commit()
else:
    pass

#Baxaindo precipitação estimada para as estações (dados brutos)
if run_satelite == True:
    for index, row in stations_sp.iterrows():
        point = [row['LATITUDE'], row['LONGITUDE']]
        for date in date_range(start_date, end_date):
            date_ = date.strftime('%Y-%m-%d')
            ppt_chirps = extract_ppt(start_date, point,'CHIRPS')
            ppt_ecmwf = extract_ppt(start_date, point,'ECMWF')
            print(date_,point,ppt_chirps,ppt_ecmwf)

    precipitation_data = ppt_nasa(start_date, end_date, point, args="PRECTOT")
    print(str(datetime(2024,10,1)).replace(' 00:00:00',''))
    print(precipitation_data)

session.close()