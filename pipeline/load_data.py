import pandas as pd
from sqlalchemy import create_engine
import click

url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet"
url_zones = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv"

dtypes_zones = {
    "LocationID": "Int64",
    "Borough": "string",
    "Zone": "string",
    "service_zone": "string",
}

@click.command()
@click.option('--year', default=2025, help='Year for the data ingestion')
@click.option('--month', default=11, help='Month for the data ingestion')
@click.option('--pg_user', default='root', help='PostgreSQL user')
@click.option('--pg_pass', default='root', help='PostgreSQL password')
@click.option('--pg_host', default='localhost', help='PostgreSQL host')
@click.option('--pg_port', default=5432, help='PostgreSQL port')
@click.option('--pg_db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--target_table', default='green_tripdata_2025-11', help='Target table for data ingestion')
@click.option('--target_table2', default='taxi_zones', help='Target table for data ingestion')
def ingest_data(year, month, pg_user, pg_pass, pg_host, pg_port, pg_db, target_table, target_table2):
    url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{year}-{month:02d}.parquet'
    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

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

    url_zones = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv"
    df_zones = pd.read_csv(url_zones, dtype=dtypes_zones)

    first = True    
    if first:
        df.head(0).to_sql(name=target_table, con=engine, if_exists='replace') #Creates table
        df_zones.head(0).to_sql(name=target_table2, con=engine, if_exists='replace')
        first = False

    df.to_sql(name=target_table, con=engine, if_exists='append') #inserts data
    df_zones.to_sql(name=target_table2, con=engine, if_exists='append') #inserts data

if __name__ == '__main__':
    ingest_data()