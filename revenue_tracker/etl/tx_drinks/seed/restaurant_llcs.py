import awswrangler as wr
import pandas as pd

from settings import llc_path, s3_path

restaurant_llcs = [
    "THE HOUSTON CHEESECAKE FACTORY CORPORATION",
    "CHIPOTLE TEXAS, L.L.C.",
    "CONTEX RESTAURANTS, INC.",
    "FOUR LEAF VENTURES, LLC",
    "SYNERGY RESTAURANT MANAGEMENT, L.L.C.",
    "SYNERGY RESTAURANT MANAGEMENT - HOUSTON, L.L.C.",
    "CHILI'S BEVERAGE COMPANY, INC.",
    "CADDO APPLE, INC."
]

llcs_df = pd.DataFrame(pd.Series(restaurant_llcs), columns = ['name'])

def write_llcs(df):
    wr.s3.to_parquet(df=df,
                    path=llc_path
                    )
    
def read_llcs():
    df = wr.s3.read_parquet(path=llc_path)
    return df


# write_llcs(llcs_df)