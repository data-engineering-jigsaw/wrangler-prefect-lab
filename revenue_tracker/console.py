from etl.tx_drinks.receipts_adapter import coerce_df, write_to_s3
from etl.tx_drinks.receipts_client import find_receipts, find_receipts_after
from athena.migrations import (display_schema, crawl_dataset, delete_db,
create_db)
from athena.queries import read_query, query, find_last_end_date
from aws_utils import delete_from_s3
import awswrangler as wr


def find_and_coerce(name):
    df = find_receipts(name)
    return coerce_df(df)

name = "THE HOUSTON CHEESECAKE FACTORY CORPORATION"


# df = find_and_coerce(name)
# write_to_s3(df)
# delete_from_s3()
# create_db()
# crawl_dataset("receipts")

# last_end_date = find_last_end_date(name) # '2023-07-31 00:00:00'
# df = find_receipts_after(name, last_end_date)