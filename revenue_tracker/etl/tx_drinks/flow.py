from prefect import task, flow
import receipts_adapter as receipts_adapter
import receipts_client as receipts_client
from prefect.server.schemas.schedules import IntervalSchedule
import athena.queries as queries

@task
def find_and_coerce(name):
    last_end_date = queries.find_last_end_date(name)
    df = receipts_client.find_receipts_after(name, last_end_date)
    return receipts_adapter.coerce_df(df)

@task
def write_to_s3(df):
    receipts_adapter.write_to_s3(df)

@flow
def find_and_write_receipts(name):
    coerced_df = find_and_coerce(name)
    write_to_s3(coerced_df)


if __name__ == "__main__":
    name = "THE HOUSTON CHEESECAKE FACTORY CORPORATION"
    find_and_write_receipts(name)
    # find_and_write_receipts.serve(
    #     name="get-restaurants-deployment",
    #     schedule=IntervalSchedule(interval=100),
    #     parameters={'url': "HONDURAS MAYA CAFE & BAR LLC"}
    #     )
# name = "THE HOUSTON CHEESECAKE FACTORY CORPORATION"
# find_and_write_receipts(name)
