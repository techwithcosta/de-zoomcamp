#%%

# download FHV CSV files automatically
import os
from urllib.parse import urlparse

year = '2019'
folder = f'fhv_data_{year}'
os.system(f'mkdir {folder}')
months = [str(month).zfill(2) for month in range(1, 13)]

for month in months:
    url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_{year}-{month}.csv.gz'
    os.system(f'wget {url} -P {folder}')

    parsed_url = urlparse(url)
    csv_name = os.path.basename(parsed_url.path)

    if csv_name.split('.')[-1] != 'csv':
        os.system(f'gzip -df {folder + '/' + csv_name}')
