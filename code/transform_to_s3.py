import pandas as pd
from sqlalchemy import create_engine
import boto3
from io import BytesIO

DB_URL = 'postgresql://postgres:key@localhost:5432/project3'
S3_BUCKET = "bucketname"
AWS_ACCESS_KEY = "key"
AWS_SECRET_KEY = "secret_key"

engine = create_engine(DB_URL)
s3_client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)

def upload_to_s3_as_parquet(table_name, schema='public'):
    print(f"Processing the table.: {table_name}")
    
    
    df = pd.read_sql(f"SELECT * FROM {schema}.{table_name}", engine)
    
    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
    
    s3_key = f"bronze/{table_name}/{table_name}.parquet"
    s3_client.put_object(
        Bucket=S3_BUCKET, 
        Key=s3_key, 
        Body=parquet_buffer.getvalue()
    )
    print(f"Succes! {table_name} uploaded in s3://{S3_BUCKET}/{s3_key}")


tables = [
    'orders', 'customers', 'products', 
    'order_items', 'order_payments', 'order_reviews', 
    'sellers', 'geolocation', 'product_category_translation'
]

for t in tables:

    upload_to_s3_as_parquet(t)
