import pandas as pd

import athena.migrations as migrations
import aws_utils as aws_utils
import etl.tx_drinks.receipts_adapter as receipts_adapter
import etl.tx_drinks.run as run
import etl.tx_drinks.seed.restaurant_llcs as restaurant_llcs
from settings import glue_db


def write_restaurants():
    restaurant_llcs.write_llcs(restaurant_llcs.llcs_df)

def read_restaurants():
    return restaurant_llcs.read_llcs().name.to_list()

def find_all_receipts_and_coerce(llc_names):
    all_receipts = [run.find_all_receipts(name) for name in llc_names]
    result_df = pd.concat(all_receipts, ignore_index=True)
    return receipts_adapter.coerce_df(result_df)

def create_glue_db():
    migrations.create_db()
    migrations.crawl_dataset("receipts")

aws_utils.delete_from_s3()
migrations.delete_db() # if exists, if not, comment out
write_restaurants()
llc_names = read_restaurants()
all_receipts = find_all_receipts_and_coerce(llc_names)

aws_utils.write_to_s3(all_receipts, mode='append')
create_glue_db()