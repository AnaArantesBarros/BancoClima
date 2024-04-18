import os
import pyproj
import pandas as pd
from datetime import timedelta
from ppt.config.parameters import start_date, end_date, point
from ppt.earth_engine_helper import authenticate
from ppt.transformations import extract_ppt, date_range

authenticate(project='ee-anaarantes')

path = os.path.dirname(os.path.realpath(__file__))
parquet_file = os.path.join(path, 'Files\\sp.parquet')
df = pd.read_parquet(parquet_file)

for index, row in df.iterrows():
    point = [row['left'], row['top']]
    for date in date_range(start_date, end_date):
        date_ = date.strftime('%Y-%m-%d')
        ppt = extract_ppt(start_date, point,'CHIRPS')
        print(date_,point,ppt)