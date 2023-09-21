import awswrangler as wr
from settings import s3_path

def delete_from_s3():
    wr.s3.delete_objects(s3_path)

def write_to_s3(df, partition_cols = ["obligation_end_date"], mode = 'append'):
    wr.s3.to_parquet(df=df,
                    path=s3_path,
                    mode=mode, dataset = True,
                    partition_cols = partition_cols)