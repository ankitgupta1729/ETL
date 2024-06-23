import pandas as pd

# reading csv
df_csv=pd.read_csv('h9gi-nx95.csv')
print(df_csv.head(5))

# reading parquet file using pandas
df_parquet=pd.read_parquet('yellow_tripdata_2024-04.parquet')
print(df_parquet)

import certifi
import json
import urllib3
from urllib3 import request

# get api data from url
url = 'https://data.cityofnewyork.us/resource/h9gi-nx95.json?$limit=500'

# Check if API is available to retrieve the data
http = urllib3.PoolManager(cert_reqs='CERT_REQUIRED',ca_certs=certifi.where())
apt_status = http.request('GET', url).status
print(apt_status)
if apt_status == 200:
    # Sometimes we get certificate error . We should never silence this error as this may cause a securirty threat.
    # Create a Pool manager that can be used to read the API response
    http = urllib3.PoolManager(cert_reqs='CERT_REQUIRED',ca_certs=certifi.where())
    data = json.loads(http.request('GET', url).data.decode('utf-8'))
    df_api = pd.json_normalize(data)
else:
    df_api = pd.Dataframe()
print(df_api.head(10))

