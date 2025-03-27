#!/usr/bin/env python
# coding: utf-8

# In[7]:

import argparse
import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine
from time import time
import os

def main(params):

    user = params.user
    password = params.password
    host= params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    parquet_name = 'output.parquet'

    os.system(f"wget {url} -O {parquet_name}")
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # trips = pq.read_table('yellow_tripdata_2021-01.parquet')
    # df = trips.to_pandas()

    trips = pq.ParquetFile(parquet_name)
    for batch in trips.iter_batches(batch_size = 100000):
        t_start= time()
        df = batch.to_pandas()
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        df.to_sql(name = table_name, con = engine, if_exists = 'append')
        t_end = time()
        print('insert a new chunk ... took %.3f seconds' %(t_end - t_start))


if __name__=="__main__":
    parser = argparse.ArgumentParser(description='Ingest CSV to Postgres')

    parser.add_argument('--user', help = 'username for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='table name where we will write the results to')
    parser.add_argument('--url', help='url to download file')
    
    args = parser.parse_args()
    main(args)



