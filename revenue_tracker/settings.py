from dotenv import load_dotenv
import os
import sys
load_dotenv()

s3_path = os.environ.get('S3_PATH')
glue_db = os.environ.get('GLUE_DB')
# sys.path.insert(0, os.getenv("PYTHONPATH"))
