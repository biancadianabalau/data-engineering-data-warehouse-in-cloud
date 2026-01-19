import pandas as pd
from sqlalchemy import create_engine
import os



def extract_data():
    base_path = "/opt/airflow/data"

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
    for ref_name, file_names in file_names.items():
        full_path = os.path.join(base_path, file_names)
        if os.path.exists(full_path):
            raw_tables[ref_name] = pd.read_csv(full_path)
            
            print(f" Extras: {ref_name} (from {file_names}) with {raw_tables[ref_name].shape[0]} rows and {raw_tables[ref_name].shape[1]} columns.")
        else:
            print(f" Error: The file {file_names} is missing from the folder")
            
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
    engine = create_engine('postgresql://airflow:airflow@postgres:5432/airflow')
    for table_name, df in processed_tables.items():
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f"Table '{table_name}' was loaded successfully.")
        



