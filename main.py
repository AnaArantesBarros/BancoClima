import os
from dotenv import load_dotenv
from ppt.earth_engine_helper import authenticate
from sqlalchemy import create_engine, Column, Integer, String, MetaData, Table, DateTime
from ppt.config.database.tables import INMET
from ppt.config.database.connector import (generate_database_session)
from ppt.transformations import (compile_year,inmet_download_unzip)

authenticate(project='ee-anaarantes')
# engine = generate_database_engine()
session = generate_database_session()

path = os.path.dirname(os.path.realpath(__file__))
#parquet_file = os.path.join(path, 'Files\\sp.parquet')
#points = pd.read_parquet(parquet_file)
for year in range(2000,2024,1):
  inmet_download_unzip(year,path)
  df_year = compile_year(year,path)


#all_ppt = pd.DataFrame() 


#for index, row in df.iterrows():
#    point = [row['left'], row['top']]
#    for date in date_range(start_date, end_date):
#        date_ = date.strftime('%Y-%m-%d')
#        ppt_chirps = extract_ppt(start_date, point,'CHIRPS')
#        ppt_ecmwf = extract_ppt(start_date, point,'ECMWF')
#        print(date_,point,ppt_chirps,ppt_ecmwf)

#precipitation_data = ppt_nasa(start_date, end_date, point, args="PRECTOT")
#print(str(datetime(2024,10,1)).replace(' 00:00:00',''))
#ppt_inmet_update('2030',path)
#print(precipitation_data)WeatherStation