import awswrangler as wr
from settings import s3_path, glue_db
databases = wr.catalog.databases()

def create_db():
    wr.catalog.create_database(glue_db)

def delete_db(glue_db):
    wr.catalog.delete_database(
        name=glue_db
    )

def crawl_dataset(table_name):
    res = wr.s3.store_parquet_metadata(
        path=s3_path,
        database=glue_db,
        table=table_name,
        dataset=True,
        mode="overwrite")
    return display_schema(table_name)
    
def display_schema(table_name):
    return wr.catalog.table(database=glue_db, table=table_name)


