q3:
select count(*) 
from public."green_tripdata_2025-11"
where date(lpep_pickup_datetime) >= '2025-11-01' and date(lpep_pickup_datetime )< '2025-12-01'
And trip_distance <= 1;

q4:
select date(lpep_pickup_datetime), trip_distance
from public."green_tripdata_2025-11"
where trip_distance < 100
group by date(lpep_pickup_datetime), trip_distance
order by trip_distance desc;

q5:
Select a."Zone", date(b.lpep_pickup_datetime) as "Date", sum(b.total_amount) as "Sum_Total_amount"
from public.taxi_zones a
left join public."green_tripdata_2025-11" b on a."LocationID" = b."PULocationID"
Where date(b.lpep_pickup_datetime) = '2025-11-18'
group by a."Zone", "Date"
order by "Sum_Total_amount" desc

q6: 
Select a."Zone", b.tip_amount
from public.taxi_zones a
left join public."green_tripdata_2025-11" b on a."LocationID" = b."DOLocationID"
where date(b.lpep_pickup_datetime) >= '2025-11-01' and date(b.lpep_pickup_datetime )< '2025-12-01' and b."PULocationID" = 74
Group by a."Zone", b.tip_amount
order by b.tip_amount desc

Or to just input pick up location name: 

Select a."Zone", b.tip_amount
from public.taxi_zones a
left join public."green_tripdata_2025-11" b on a."LocationID" = b."DOLocationID"
inner join (select distinct(c."Zone"), d."PULocationID"
from public."green_tripdata_2025-11" d
left join public.taxi_zones c on d."PULocationID" = c."LocationID"
where c."Zone" = 'East Harlem North'
Group by c."Zone", d."PULocationID") e on e."PULocationID" = b."PULocationID"
where date(b.lpep_pickup_datetime) >= '2025-11-01' and date(b.lpep_pickup_datetime )< '2025-12-01'
Group by a."Zone", b.tip_amount
order by b.tip_amount desc;

Homework 2:
Q3:
SELECT count(*)
FROM de-zoomcamp-485820.zoomcamp.yellow_tripdata
Where filename like ('%2020%')

Q4:
SELECT count(*)
FROM de-zoomcamp-485820.zoomcamp.green_tripdata
Where filename like ('%2020%')

Q5:
SELECT count(*)
FROM de-zoomcamp-485820.zoomcamp.yellow_tripdata_2021_03

Homework 3: 
Create or replace external table de-zoomcamp-485820.zoomcamp.external_yellow_tripdata_2024
options (
  format = 'PARQUET',
  uris = ['gs://kestra_zoomcamp_test_david/yellow_tripdata_2024-*.parquet']
);

--

Create or replace table de-zoomcamp-485820.zoomcamp.yellow_tripdata_2024_non_partiotioned AS
Select * FROM de-zoomcamp-485820.zoomcamp.external_yellow_tripdata_2024

--

Select count(1)
from de-zoomcamp-485820.zoomcamp.yellow_tripdata_2024_non_partiotioned

Select Distinct(Count(PULocationID))
From de-zoomcamp-485820.zoomcamp.yellow_tripdata_2024_non_partiotioned

Select Distinct(Count(PULocationID))
From de-zoomcamp-485820.zoomcamp.external_yellow_tripdata_2024

Select PULocationID
From de-zoomcamp-485820.zoomcamp.yellow_tripdata_2024_non_partiotioned

Select PULocationID, DOLocationID
From de-zoomcamp-485820.zoomcamp.yellow_tripdata_2024_non_partiotioned

Select count(1)
from de-zoomcamp-485820.zoomcamp.yellow_tripdata_2024_non_partiotioned
where fare_amount = 0

Create or replace table de-zoomcamp-485820.zoomcamp.yellow_tripdata_2024_partiotioned 
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
Select * FROM de-zoomcamp-485820.zoomcamp.yellow_tripdata_2024_non_partiotioned


Select DISTINCT(VendorID)
From de-zoomcamp-485820.zoomcamp.yellow_tripdata_2024_non_partiotioned
Where tpep_dropoff_datetime >= '2024-03-01'AND tpep_dropoff_datetime <  '2024-03-16'

Select DISTINCT(VendorID)
From de-zoomcamp-485820.zoomcamp.yellow_tripdata_2024_partiotioned
Where tpep_dropoff_datetime >= '2024-03-01'AND tpep_dropoff_datetime <  '2024-03-16'

Select Count(*)
From de-zoomcamp-485820.zoomcamp.yellow_tripdata_2024_partiotioned