
-- query 1
select * from read_files('sample_data/large_simple/*.parquet')
  where id % 2 = 0; 

