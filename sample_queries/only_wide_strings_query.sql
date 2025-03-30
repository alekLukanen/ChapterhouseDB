
-- query 1
select * from read_files('simple_wide_string/*.parquet')
  where id > 25;

