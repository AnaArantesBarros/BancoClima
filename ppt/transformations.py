import ee
from datetime import timedelta

def extract_ppt(date,geometry,collection:str = ''):
  """Extract precipitation in a sigle coordinate. Collections current avaliable include: 
    CHIRPS Daily- Climate Hazards Group InfraRed Precipitation With Station Data and ECMWF ERA5-Land Daily Aggregated.

    Keyword arguments:
    date --  '%Y-%m-%d' 
    geometry -- [latitude,longitude]  (Decimal degres)
    collection -- string Upper case ('CHIRPS' or 'ECMWF'.)
    """
  collections = ['CHIRPS', 'ECMWF']
  if collection == 'CHIRPS':
    snippet = 'UCSB-CHG/CHIRPS/DAILY'
  if collection == 'ECMWF':
    snippet = 'ECMWF/ERA5_LAND/DAILY_AGGR'
  elif collection not in collections:
    raise ValueError("Invalid collection. Expected one of: %s" % collections)
  
  image = ee.ImageCollection(snippet).filter(
      ee.Filter.date(date, date + timedelta(days=1))).first()

  region = ee.Geometry.Point(geometry) 

  mean = image.reduceRegion(**{
    'reducer': ee.Reducer.mean(),
    'geometry': region,
    'scale': 30
  })

  return(mean.getInfo())

def date_range(start_date, end_date):
  """Create a list of dates from a specified start date to end date with the step of one day.

  Keyword arguments:
  start_date --  datetime object 
  end_date -- datetime object
  """
  current_date = start_date
  while current_date <= end_date:
      yield current_date
      current_date += timedelta(days=1)

#ee.ImageCollection("ECMWF/ERA5_LAND/DAILY_AGGR")