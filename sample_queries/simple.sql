
-- query 1
select * from read_files('simple/*.parquet')
  where id > 25;

-- query 2
select id, value2 from read_files('simple/*.parquet')
  where id > 25;

-- query 3
select 
  id, 
  id + 10.0 as id_plus_10, 
  (value2 + 10) / 100 as value2 
from read_files('simple/*.parquet')
  where id > 25 + 0.0;

