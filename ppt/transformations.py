import ee
import re
import os
import requests
import zipfile
import pandas as pd
from datetime import timedelta, datetime


def extract_ppt(date,geometry,collection:str = ''): #Tem como usar um multipolygon?? Seria mais rápido???
  '''Extract precipitation in a sigle coordinate. Collections current avaliable include: 
    CHIRPS Daily- Climate Hazards Group InfraRed Precipitation With Station Data and ECMWF ERA5-Land Daily Aggregated.

    Keyword arguments:
    date --  '%Y-%m-%d' 
    geometry -- [latitude,longitude]  (Decimal degres)
    collection -- string Upper case ('CHIRPS' or 'ECMWF'.)
    '''
  collections = ['CHIRPS', 'ECMWF']
  if collection == 'CHIRPS':
    snippet = 'UCSB-CHG/CHIRPS/DAILY'
    column_filter = 'precipitation'
    multiplier = 1

  if collection == 'ECMWF':
    snippet = 'ECMWF/ERA5_LAND/DAILY_AGGR'
    column_filter = 'total_precipitation_sum'
    multiplier = 1000

  elif collection not in collections:
    raise ValueError('Invalid collection. Expected one of: %s' % collections)
  
  image = ee.ImageCollection(snippet).filter(
      ee.Filter.date(date, date + timedelta(days=1))).first()

  region = ee.Geometry.Point(geometry) 

  image = image.select(column_filter)

  mean = image.reduceRegion(**{
    'reducer': ee.Reducer.mean(),
    'geometry': region,
    'scale': 30
  })
  response = mean.getInfo()
  try:
    return(response[column_filter]*multiplier)
  except:
    return(0)
  

def date_range(start_date, end_date): 
  '''Create a list of dates from a specified start date to end date with the step of one day.

  Keyword arguments:
  start_date --  datetime object 
  end_date -- datetime object
  '''
  current_date = start_date
  while current_date <= end_date:
      yield current_date
      current_date += timedelta(days=1)


def datetime_to_str(date_input):
   new_data = str(date_input).replace(' 00:00:00','').replace('-','')
   return new_data


def ppt_nasa(start_date:datetime, end_date:datetime, point:list , args='PRECTOT'):
    """Baixa dados da Api do NasaPower.

    start_date -- datetime object
    end_date -- datetime object
    point -- list [latitude, longitude]
    args -- parameter column names
    """
    start_date = datetime_to_str(start_date)
    end_date = datetime_to_str(end_date)
    lat = str(point[1])
    lon =str(point[0])
    response = requests.get(
        'https://power.larc.nasa.gov/api/temporal/daily/point?start=' + start_date + '&end=' + end_date + '&latitude=' + 
            str(lat) + '&longitude=' + str(lon) + '&community=sb&parameters=' + args + '&format=csv&header=true')
    response = response.status_code
    print('Status Code: {}'.format(response))

    data = response.text

    # Limpamos as informações desnecessárias
    before, sep, after = data.partition('-END HEADER-')
    if len(after) > 0:
        data = after

        df = pd.DataFrame([x.split(',') for x in data.split('\n')[2:]],
                          columns=[x for x in data.split('\n')[1].split(',')])
        df = df.dropna(subset=['YEAR','MO','DY'])
        df['DATE'] = df['YEAR'].astype(str) + '-' + df['MO'].astype(str) + '-' + df['DY'].astype(str)
        df['DATE'] = pd.to_datetime(df['DATE'])
        df.rename(columns={'PRECTOTCORR': 'PRECTOT'}, inplace=True)
        df = df[['DATE', 'PRECTOT']]
        return(df)


def ppt_inmet_update(year, output_path):
    year = str(year)
    url = f'https://portal.inmet.gov.br/uploads/dadoshistoricos/{year}.zip'
    output = output_path + '\\WeatherStation'
    zip_filename = os.path.join(output, f'\\{year}.zip')
    response = requests.get(url)

    if response.status_code == 200:
        with open(zip_filename, 'wb') as f:
            f.write(response.content)
        with zipfile.ZipFile(zip_filename, 'r') as zip_ref:
            zip_ref.extractall(output)
        os.remove(zip_filename)

        print(f"Arquivos extraídos com sucesso para: {output}")
    else:
        print("Erro ao baixar o arquivo.")