
-- query 1
select * from read_files('sample_data/simple_wide_string/*.parquet')
  where id > 25;

