import ee
from datetime import timedelta

def extract_ppt_chirps(date,geometry):
#image = ee.Image('LANDSAT/LC08/C01/T1_TOA/LC08_044034_20140318')
  image = ee.ImageCollection('UCSB-CHG/CHIRPS/DAILY').filter(
      ee.Filter.date(date, date + timedelta(days=1))).first()

  region = ee.Geometry.Point(geometry) 

  mean = image.reduceRegion(**{
    'reducer': ee.Reducer.mean(),
    'geometry': region,
    'scale': 30
  })

  return(mean.getInfo())
