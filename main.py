import os
import pyproj
import pandas as pd
from datetime import datetime
from datetime import timedelta
from ppt.config.parameters import start_date, end_date, point
from ppt.earth_engine_helper import authenticate
from ppt.transformations import extract_ppt, date_range, ppt_nasa, datetime_to_str, ppt_inmet_update

authenticate(project='ee-anaarantes')


path = os.path.dirname(os.path.realpath(__file__))
parquet_file = os.path.join(path, 'Files\\sp.parquet')
df = pd.read_parquet(parquet_file)

all_ppt = pd.DataFrame() 

for index, row in df.iterrows():
    point = [row['left'], row['top']]
    for date in date_range(start_date, end_date):
        date_ = date.strftime('%Y-%m-%d')
        ppt_chirps = extract_ppt(start_date, point,'CHIRPS')
        ppt_ecmwf = extract_ppt(start_date, point,'ECMWF')
        print(date_,point,ppt_chirps,ppt_ecmwf)

#precipitation_data = ppt_nasa(start_date, end_date, point, args="PRECTOT")
#print(str(datetime(2024,10,1)).replace(' 00:00:00',''))
#ppt_inmet_update('2030',path)
#print(precipitation_data)WeatherStation