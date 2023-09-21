import pandas as pd
import awswrangler as wr
from settings import s3_path

def coerce_df(df):
    numeric_cols = ['taxpayer_number', 'taxpayer_zip', 'taxpayer_county', 
    'location_zip', 'location_county', 'location_number', 'liquor_receipts',
      'beer_receipts', 'wine_receipts', 'cover_charge_receipts', 'total_receipts']
    dt_cols = ['responsibility_begin_date_yyyymmdd', 'obligation_end_date_yyyymmdd']

    combined_cols = numeric_cols + dt_cols
    numeric_df = df[numeric_cols]
    
    dt_df = df[dt_cols]
    numeric_df_coerced = numeric_df.apply(pd.to_numeric, errors='coerce')
    dt_coerced = dt_df.apply(pd.to_datetime, format='%Y-%m-%d', errors = 'coerce')
    remaining_df = df.drop(columns = combined_cols)
    fully_coerced_df = pd.concat([numeric_df_coerced, dt_coerced, remaining_df], axis = 1)
    renamed_df = fully_coerced_df.rename(columns={"responsibility_begin_date_yyyymmdd": "response_begin_date","responsibility_end_date_yyyymmdd": "response_end_date", "obligation_end_date_yyyymmdd": 'obligation_end_date'})
    return renamed_df

def write_to_s3(df, partition_cols = ["obligation_end_date"]):
    wr.s3.to_parquet(df=df,
                    path=s3_path,
                    mode='append', dataset = True,
                    partition_cols = partition_cols)
    
