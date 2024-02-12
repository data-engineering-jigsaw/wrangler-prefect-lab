import awswrangler as wr

from settings import s3_path


def delete_from_s3():
    wr.s3.delete_objects(s3_path)

def write_to_s3(df, mode = 'overwrite'):
    wr.s3.to_parquet(df=df,
                    path=s3_path,
                    mode=mode, dataset = True
                    )

# def read_from_s3():
#     df = wr.s3.read_parquet(s3_path, dataset=True)
#     return df