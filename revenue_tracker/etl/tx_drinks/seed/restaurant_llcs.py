import pandas as pd
import awswrangler as wr
from settings import s3_path
restaurant_llcs = [
    "THE HOUSTON CHEESECAKE FACTORY CORPORATION",
    "CHIPOTLE TEXAS, L.L.C."
    "CONTEX RESTAURANTS, INC.",
    "FOUR LEAF VENTURES, LLC",
    "SYNERGY RESTAURANT MANAGEMENT, L.L.C.",
    "SYNERGY RESTAURANT MANAGEMENT - HOUSTON, L.L.C.",
    "CADDO APPLE, INC."
]

llcs_df = pd.Series(restaurant_llcs)

def write_llcs(df):
    # probably should combine with other to_parquet fn
    wr.s3.to_parquet(df=df,
                    path=f"{s3_path}restaurant_llcs.snappy.parquet",
                    mode='overwrite'
                    )