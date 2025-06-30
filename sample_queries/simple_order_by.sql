
-- query 1
select * from read_files('sample_data/simple/*.parquet')
  where id < 25
  order by id desc;

