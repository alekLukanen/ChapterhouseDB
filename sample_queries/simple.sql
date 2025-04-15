
-- query 1
select * from read_files('simple_wide_string/*.parquet')
  where id > 25;

-- query 2
select * from read_files('simple/*.parquet')
  where id > 25;

-- query 3
select id, value2 from read_files('simple/*.parquet')
  where id > 25;

-- query 4
select 
  id, 
  value1, 
  id + 10.0 as id_plus_10,
  (value2 + 10) / 100 as value2,
  1.0 / id as value3,
  1.0 / (id * id) as value4,
  id * id as value5
from read_files('simple/*.parquet')
  where id > 25 + 0.0;

