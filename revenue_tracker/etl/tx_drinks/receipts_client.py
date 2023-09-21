import requests
import pandas as pd
from athena.queries import find_last_end_date 

def find_all_receipts(name):
    url = "https://data.texas.gov/resource/naix-2893.json"
    response = requests.get(url, params = {'taxpayer_name': name})
    receipts = response.json()
    df = pd.DataFrame(receipts)
    return df


def find_receipts_after(name: str, dt: str):
    date, time = dt.split()
    var = f'{date}T{time}' # eg. '2023-03-31T00:00:00'
    url = "https://data.texas.gov/resource/naix-2893.json"
    response = requests.get(url, params = {'$where': f"taxpayer_name = '{name}' and obligation_end_date_yyyymmdd > '{var}'"})
    receipts = response.json()
    df = pd.DataFrame(receipts)
    return df

def find_recent(name):
    last_end_date = find_last_end_date(name)
    if last_end_date:
        df = find_receipts_after(name, last_end_date)
        
def find(name):
    recent_receipts_df = find_recent(name)
    # checking a boolean value of df is difficult (requires df.any().any())
    if recent_receipts_df:
        return recent_receipts_df
    else:
        receipts_df = find_all_receipts(name)
    return receipts_df