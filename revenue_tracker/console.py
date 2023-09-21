import awswrangler as wr

from etl.tx_drinks.receipts_adapter import coerce_df
from etl.tx_drinks.receipts_client import find_all_receipts, find_receipts_after
from athena.migrations import (display_schema, crawl_dataset, delete_db,
create_db)
from athena.queries import read_query, query, find_last_end_date
from aws_utils import delete_from_s3, write_to_s3
from etl.tx_drinks.seed.restaurant_llcs import write_llcs, read_llcs, llcs_df
from etl.tx_drinks.run import (find_and_coerce_llcs, find_all_receipts,
find_recent, find_and_coerce)


name = "THE HOUSTON CHEESECAKE FACTORY CORPORATION"


# df = find_all_receipts(name)
# write_to_s3(df)
# delete_from_s3()
# create_db()
# crawl_dataset("receipts")
# read_query(query)
# last_end_date = find_last_end_date(name) # '2023-07-31 00:00:00'
# df = find_recent(name, last_end_date)
# 
# new_name = "CONTEX RESTAURANTS, INC."

# find_and_coerce(new_name) # works for both new and existing restaurants 

# write_llcs(llcs_df)
# read_llcs()
