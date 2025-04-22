-- query 2
select * from read_files('sample_data/simple/*.parquet')
  where id < 25;
