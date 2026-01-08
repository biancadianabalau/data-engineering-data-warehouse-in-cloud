import pandas as pd
from sqlalchemy import create_engine
import os


DB_URL = 'postgresql://postgres:postgres123@localhost:5432/project3'
engine = create_engine(DB_URL)

def extract_data():
    
    file_names = {
        'customers': 'olist_customers_dataset.csv',
        'products': 'olist_products_dataset.csv',
        'sellers': 'olist_sellers_dataset.csv',
        'orders': 'olist_orders_dataset.csv',
        'order_items': 'olist_order_items_dataset.csv',
        'order_payments': 'olist_order_payments_dataset.csv',
        'order_reviews': 'olist_order_reviews_dataset.csv',
        'geolocation': 'olist_geolocation_dataset.csv',
        'product_category_translation': 'product_category_name_translation.csv'
    }
    
    raw_tables = {}
    for ref_name, file_name in file_names.items():
        if os.path.exists(file_name):
            raw_tables[ref_name] = pd.read_csv(file_name)
            print(f" Extras: {ref_name} (from {file_name})")
        else:
            print(f" Error: The file {file_name} is missing from the folder")
            
    return raw_tables

def transform_data(raw_tables):
    """Minimal cleanindg and renaming of tables"""
    cleaned_tables = {}
    mapping = {
       'customers': 'customers',           
        'products': 'products',             
        'sellers': 'sellers',
        'orders': 'orders',
        'order_items': 'order_items',
        'order_payments': 'order_payments',
        'order_reviews': 'order_reviews',
        'geolocation': 'geolocation',
        'product_category_translation': 'product_category_translation'
    }

    for old_name, new_name in mapping.items():
        df = raw_tables[old_name].copy()
        

        df = df.drop_duplicates()

        df.columns = [c.lower().replace(' ', '_').strip() for c in df.columns]

        cols_text = df.select_dtypes(include=['object']).columns
        df[cols_text] = df[cols_text].apply(lambda x: x.str.strip() if hasattr(x, 'str') else x)

        cleaned_tables[new_name] = df
        print(f"Table '{new_name}' was cleaned and transformed.")
        
    return cleaned_tables

def load_to_postgres(processed_tables):
    """Load tables to PostgreSQL"""
    for table_name, df in processed_tables.items():
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f"Table '{table_name}' was loaded successfully into Postgres.")

# Execution
if __name__ == "__main__":
    print("start process ETL...")
    date_extrase = extract_data()
    date_curatate = transform_data(date_extrase)
    load_to_postgres(date_curatate)
    print("Process done")
else:
    print(" Error: Check the folder")