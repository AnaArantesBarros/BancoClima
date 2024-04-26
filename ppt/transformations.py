import ee
import re
import os
import requests
import zipfile
import pandas as pd
import dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from sqlalchemy.orm import sessionmaker
from os import listdir
from os.path import isfile, join
from datetime import timedelta, datetime
from ppt.config.database.tables import INMET
from ppt.config.database.connector import generate_database_session
import sys
sys.path.insert(0, os.path.abspath(".."))


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


def inmet_download_unzip(year, output_path:str):
    """Download and extract .zip files from inmet site.
    
    Keyword arguments:
    year -- string, float or interger
    output_path -- string
    """
    year = str(year)
    url = f'https://portal.inmet.gov.br/uploads/dadoshistoricos/{year}.zip'
    if int(year) < 2020:
        output = output_path + '/WeatherStation'
        output_folder = output + f'/{year}'
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)
        zip_filename = os.path.join(output, f'{year}.zip')
    else:
        output = output_path + '/WeatherStation'+ f'/{year}'
        if not os.path.exists(output):
            os.makedirs(output)
        zip_filename = os.path.join(output, f'{year}.zip')

    try:
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

    except Exception as e:
        print(f"Erro: {e}")


def extract_info_from_filename(filename:str):
  """Extract information from the name of the file using regex.

  filename -- string
  """
  matches_date = re.findall(r'(\d{2}-\d{2}-\d{4})', filename)
  start_date = matches_date[0]
  end_date = matches_date[1]

  matches_state = re.findall(r'(_[A-Za-z]{2}+_A)', filename)
  state = matches_state[0].replace('_A','').replace("_",'')

  matches_station = re.findall(r'([0-9]_[A-Za-z ]{4,}_[0-9])', filename)
  station = matches_station[0][2:-2]

  return(filename, start_date, end_date,state,station)


def ppt_from_header(path: str, file: str):
  """ Extracting information in the header of station file. 

  path -- string
  file -- string
  """
  with open(str(path + '/' + file), 'r', encoding='ISO-8859-1') as arquivo:
    lines = arquivo.readlines()[:8] 
    dict = {}
    for line in lines:
      key, value = line.strip().split(':')
      dict[key] = value.strip(';')  # Remover ';' dos valores
    return dict


def ppt_from_station(path:str,file:str):
  """ Converting information from station file to a pandas Data Frame.
  
  path -- string
  file -- string
  """
  output_path = path+'/'+file
  with open(output_path, encoding='ISO-8859-1') as my_file:
    df = pd.read_csv(output_path, skiprows=8, delimiter=';', decimal=',',encoding='ISO-8859-1')
    try:
      df['DATE'] = pd.to_datetime(df['DATA (YYYY-MM-DD)'])
    except:
       df['DATE'] = pd.to_datetime(df['Data'])
    df['PPT_mm'] = df['PRECIPITAÇÃO TOTAL, HORÁRIO (mm)'].replace(-9999.0,0)
    ppt_by_date = df.groupby('DATE')['PPT_mm'].sum()
    new_df = ppt_by_date.reset_index()
    linhas = ppt_from_header(path,file)
    try:
      new_df['STATION'] = linhas['ESTAÇÃO']
    except KeyError:
      try:
        new_df['STATION'] = linhas['ESTACAO']
      except KeyError:
        try:
            new_df['STATION'] = linhas['ESTAC?O']
        except KeyError:
            print("Nenhuma das chaves encontradas")
    new_df['LATITUDE'] = linhas['LATITUDE']
    new_df['LONGITUDE'] = linhas['LONGITUDE']
    new_df['CODE'] = linhas['CODIGO (WMO)']
    new_df['KEY'] = new_df['DATE'].astype(str) + new_df['CODE'].astype(str)
    new_df['UF'] = linhas['UF']
    return(new_df)
   
def compile_year(year,path:str):
  """Uses a given year to extract all dates from station files in their respective folders. All data is grouped and summed by day.
  
  year -- string/integer
  path -- string
  """
  output_path = path + '/WeatherStation/' + str(year) + '/' + ''
  files = [f for f in listdir(output_path) if isfile(join(output_path, f))]

  all_dfs = []

  for file in files:
    df = ppt_from_station(output_path, file)
    all_dfs.append(df)

  combined_df = pd.concat(all_dfs, ignore_index=True)
  return(combined_df)
