import os
import pyproj
import pandas as pd
from datetime import timedelta
from ppt.config.parameters import start_date, end_date, point
from ppt.earth_engine_helper import authenticate
from ppt.transformations import extract_ppt_chirps

authenticate(project='ee-anaarantes')

path = os.path.dirname(os.path.realpath(__file__))
parquet_file = os.path.join(path, 'Files\\pontos_sp_teste.parquet')
df = pd.read_parquet(parquet_file)

# Exibir o DataFrame com as coordenadas WGS84
print(df)

#Defining functions
def date_range(start_date, end_date, step=timedelta(days=1)):
    current_date = start_date
    while current_date <= end_date:
        yield current_date
        current_date += step

#for point in points:
#point = []
#  for date in date_range(start_date, end_date):
    #print(date.strftime('%Y-%m-%d'))
    #append value

ppt = extract_ppt_chirps(start_date, point)
print(ppt)
