import requests
import pandas as pd
def find_receipts(name):
    url = "https://data.texas.gov/resource/naix-2893.json"
    response = requests.get(url, params = {'taxpayer_name': name})
    receipts = response.json()
    df = pd.DataFrame(receipts)
    return df


def find_receipts_after(name: str, dt: str):
    #eg: dt: '2023-07-31 00:00:00'
    # var = '2023-03-31T00:00:00'
    date, time = dt.split()
    var = f'{date}T{time}'
    url = "https://data.texas.gov/resource/naix-2893.json"
    response = requests.get(url, params = {'$where': f"taxpayer_name = '{name}' and obligation_end_date_yyyymmdd > '{var}'"})
    receipts = response.json()
    
    df = pd.DataFrame(receipts)
    return df