import awswrangler as wr
from settings import s3_path

def delete_from_s3():
    wr.s3.delete_objects(s3_path)