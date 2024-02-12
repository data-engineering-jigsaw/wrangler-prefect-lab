
import aws_utils as aws_utils
from etl.tx_drinks.receipts_adapter import coerce_df
from etl.tx_drinks.receipts_client import (find)
from etl.tx_drinks.receipts_client import (find_all_receipts,
                                           find_receipts_after)
from etl.tx_drinks.seed.restaurant_llcs import read_llcs


def find_and_coerce(name):
    receipts_df = find(name) 
    return coerce_df(receipts_df)
     
def find_and_coerce_llcs():
    llc_names = read_llcs().name.to_list()
    coerced_dfs = []
    for name in llc_names:
        coerced_df = find_and_coerce(name)
        if coerced_df.any().any():
            aws_utils.write_to_s3(coerced_df)
        coerced_dfs.append(coerced_df)
    return coerced_dfs



    