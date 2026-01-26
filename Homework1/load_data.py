#!/usr/bin/env python
# coding: utf-8

# In[2]:

import pandas as pd
from sqlalchemy import create_engine



url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet"
url_zones = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv"

df = pd.read_parquet(url)

df = df.astype({
    "VendorID": "Int64",
    "store_and_fwd_flag": "string",
    "RatecodeID": "Int64",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "ehail_fee": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "payment_type": "Int64",
    "trip_type": "Int64",
    "congestion_surcharge": "float64",
    "cbd_congestion_fee": "float64"
})


df["lpep_pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])
df["lpep_dropoff_datetime"] = pd.to_datetime(df["lpep_dropoff_datetime"])



dtypes_zones = {
    "LocationID": "Int64",
    "Borough": "string",
    "Zone": "string",
    "service_zone": "string",
}

df_zones = pd.read_csv(
    url_zones,
    dtype=dtypes_zones
)

engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')


#creates table
df.head(0).to_sql(name='green_tripdata_2025-11', con=engine, if_exists='replace')
df_zones.head(0).to_sql(name='taxi_zone', con=engine, if_exists='replace')

#insert data
df.to_sql(name='green_tripdata_2025-11', con=engine, if_exists='replace') 
df_zones.to_sql(name='taxi_zone', con=engine, if_exists='replace')

