import awswrangler as wr

from settings import glue_db, s3_path

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
    
    
def display_schema(table_name):
    return wr.catalog.table(database=glue_db, table=table_name)


