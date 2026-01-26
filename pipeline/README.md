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