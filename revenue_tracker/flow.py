import pandas as pd
from prefect import flow, task
from prefect.server.schemas.schedules import IntervalSchedule

import aws_utils as aws_utils
import etl.tx_drinks.receipts_adapter as receipts_adapter
import etl.tx_drinks.run as run
import etl.tx_drinks.seed.restaurant_llcs as restaurant_llcs


@task
def find_and_coerce(name):
    return run.find_and_coerce(name)

@task
def read_restaurants():
    return restaurant_llcs.read_llcs().name.to_list()

@task
def write_to_s3(df):
    aws_utils.write_to_s3(df, mode = 'append')

@flow
def find_and_write_receipts():
    llc_names = read_restaurants()
    for name in llc_names:
        coerced_df = find_and_coerce(name)
        if coerced_df.any().any():
            write_to_s3(coerced_df)


if __name__ == "__main__":
    find_and_write_receipts()

    # find_and_write_receipts.serve(
    #     name="get-restaurants-deployment",
    #     schedule=IntervalSchedule(interval=100)
    #     )