import awswrangler as wr
from settings import s3_path, glue_db
from datetime import datetime

def read_query(query):
    resulting_df = wr.athena.read_sql_query(query, database=glue_db)
    return resulting_df

query = "SELECT * FROM receipts where total_receipts > 100 limit 10"

 
def find_last_end_date(taxpayer_name) -> str:
    df = wr.athena.read_sql_query(
    sql="SELECT max(obligation_end_date) as last_end_date FROM receipts WHERE taxpayer_name=:taxpayer_name",
    database=glue_db,
    params={"taxpayer_name": taxpayer_name, "obligation_end_date": "filtered_city"})
    return df.last_end_date[0]
