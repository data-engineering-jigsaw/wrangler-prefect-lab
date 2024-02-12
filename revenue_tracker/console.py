import awswrangler as wr

from athena.migrations import (crawl_dataset, create_db, delete_db,
                               display_schema)
from athena.queries import find_last_end_date, query, read_query
from aws_utils import delete_from_s3, write_to_s3
from etl.tx_drinks.receipts_adapter import coerce_df
from etl.tx_drinks.receipts_client import (find_all_receipts,
                                           find_receipts_after)
from etl.tx_drinks.run import (find_all_receipts, find_and_coerce,
                               find_and_coerce_llcs)
from etl.tx_drinks.seed.restaurant_llcs import llcs_df, read_llcs, write_llcs

name = "THE HOUSTON CHEESECAKE FACTORY CORPORATION"

# df = find_all_receipts(name)

# coerced_df = coerce_df(df)

# write_to_s3(coerced_df)
# read_from_s3()

# create_db()
# crawl_dataset("receipts")
# display_schema("receipts")
# read_query(query)


# last_end_date = find_last_end_date(name) # '2023-07-31 00:00:00'
# df = find_recent(name, last_end_date)
# 
# new_name = "CONTEX RESTAURANTS, INC."

# find_and_coerce(new_name) # works for both new and existing restaurants 

# write_llcs(llcs_df)
# read_llcs()
