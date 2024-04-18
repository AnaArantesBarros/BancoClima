import ee
from datetime import datetime, timedelta
ee.Authenticate()
ee.Initialize(project='ee-anaarantes')


def date_range(start_date, end_date, step=timedelta(days=1)):
    current_date = start_date
    while current_date <= end_date:
        yield current_date
        current_date += step


start_date = datetime(2000, 1, 1)
end_date = datetime(2000, 1, 10)
point = [-46.625290,-23.533773]

def extract_ppt(s_date,e_date,geometry):
#image = ee.Image('LANDSAT/LC08/C01/T1_TOA/LC08_044034_20140318')
  image = ee.ImageCollection('UCSB-CHG/CHIRPS/DAILY').filter(
      ee.Filter.date(s_date, e_date)).first()

# Create an arbitrary rectangle as a region and display it.
  region = ee.Geometry.Point(geometry) 

# Get a dictionary of means in the region.  Keys are bandnames.
  mean = image.reduceRegion(**{
    'reducer': ee.Reducer.mean(),
    'geometry': region,
    'scale': 30
  })

  return(mean.getInfo())

#for date in date_range(start_date, end_date):
    #print(date.strftime('%Y-%m-%d'))

ppt = extract_ppt(start_date,end_date,point)
print(ppt)