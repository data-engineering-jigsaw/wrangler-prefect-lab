from prefect import flow, task
from prefect.server.schemas.schedules import IntervalSchedule

import aws_utils as aws_utils
import etl.tx_drinks.run as run
from etl.tx_drinks.seed.restaurant_llcs import read_llcs


@task
def read_restaurants():
    llc_names = read_llcs().name.to_list()
    return llc_names

@task
def find_and_coerce(name):
    return run.find_and_coerce(name)

@task
def write_to_s3(df):
    aws_utils.write_to_s3(df)

@flow
def find_and_write_receipts():
    llc_names = read_llcs().name.to_list()
    coerced_dfs = []
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

