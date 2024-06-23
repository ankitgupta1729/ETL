# Below is an example on how we can create an enterprise level pipeline.
# Make sure below packages have been installed
# pip install pyarrow
# pip install certifi

import urllib3
from urllib3 import request
import certifi
import json
import sqlite3
import pandas as pd

def source_data_from_csv(csv_file_name):
    try:
        df_csv=pd.read_csv(csv_file_name)
    except Exception as e:
        df_csv=pd.DataFrame()
    return df_csv

def source_data_from_parquet(parquet_file_name):
    try:
        df_parquet=pd.read_parquet(parquet_file_name)
    except as Exception as e:
        df_parquet=pd.DataFrame()
    return df_parquet

def source_data_from_api(api_endpoint):
    try:
        http=urllib3.PoolManager(cert_reqs='CERT_REQUIRED',ca_certs=certifi.where())
        api_response=http.request('GET',api_endpoint)
        api_status=api_response.status
        print("API RESPONSE CODE: ",api_status)
        if api_status==200:
            data=json.loads(api_response.data.decode('utf-8'))
            df_api=data.json_normalize(data)
        else:
            df_api=pd.DataFrame()
    except Exception as e:
        df_api=pd.DataFrame()
    return  df_api

def source_data_from_table(db_name,table_name):
    try:
        with sqlite3.connect(db_name) as conn:
            df_table=pd.read_sql(f"select * from {table_name}",conn)
    except Exception as e:
        df_table=pd.DataFrame()
    return  df_table

def source_data_from_webpage(web_page_url,matching_keyword):
    try:
        df_html=pd.read_html(web_page_url,match=matching_keyword)
        df_html=df_html[0]
    except Exception as e:
        df_html=pd.DataFrame()
    return  df_html

def Extract_Data():
    parquet_file_name = "yellow_tripdata_2024-04.parquet"
    csv_file_name = "h9gi-nx95.csv"
    api_endpoint = "https://data.cityofnewyork.us/resource/h9gi-nx95.json?$limit=500"
    db_name = "movies.sqlite"
    table_name = "movies"
    web_page_url = "https://en.wikipedia.org/wiki/List_of_countries_by_GDP_(nominal)"
    matching_keyword = "by country"

    df_parquet, df_csv, df_api, df_table, df_html = (source_data_from_parquet(parquet_file_name),
                                                     source_data_from_csv(csv_file_name),
                                                     source_data_from_api(api_endpoint),
                                                     source_data_from_table(db_name, table_name),
                                                     source_data_from_webpage(web_page_url, matching_keyword))
    return df_parquet, df_csv, df_api, df_table, df_html