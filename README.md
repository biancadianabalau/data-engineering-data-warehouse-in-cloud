# E-Commerce Cloud - Data Lakehouse Project
Project Overview - in progress

An end-to-end Data Engineering pipeline that ingests, transforms, and standardizes e-commerce data from an on-premise PostgreSQL environment to a scalable Cloud Data Lakehouse on AWS S3 using Databricks.

## Architecture
The project follows the Medallion Architecture (Bronze -> Silver -> Gold) to ensure data quality and reliability.

Tech Stack
- Ingestion: Python (Pandas/SQLAlchemy), PostgreSQL.

- Cloud Storage: AWS S3 (Parquet & Delta formats).

- Data Processing: Databricks SQL, Apache Spark.

- Data Governance: Delta Lake (ACID Transactions, Time Travel).

- Orchestration: Airflow (Work in Progress).

<img width="1553" height="612" alt="arhitectura drawio" src="https://github.com/user-attachments/assets/2987e0f6-5fb9-484b-b77c-1e73d37c868d" />


## Pipeline Phases
### 1. Ingestion & Pre-processing
Original CSV datasets are loaded into PostgreSQL via Python.

A Python script extracts data from PostgreSQL and offloads it to AWS S3 as Parquet files to reduce storage costs and improve read performance.

### 2. Medallion Layering (Databricks)
- Bronze Layer: Raw data is registered in Databricks as external tables pointing to S3 Parquet files.

- Silver Layer: Data cleaning: Whitespace removal (TRIM), case standardization (UPPER).
  - Type Casting: Precision handling for financial data (DECIMAL), coordinates (DOUBLE), and timestamps (ISO 8601).
  - Storage: The Silver layer is migrated back to S3 using the Delta Lake format, providing ACID compliance and schema enforcement.
  
- Gold Layer: Data Modeling: Implementation of a Star Schema architecture consisting of 5 specialized tables (2 Fact tables and 3 Dimension tables) optimized for Business Intelligence
  - Fact Tables: Calculation of key financial metrics such as Total Order Value (Price + Freight) and multi-level granularity (Order-level vs. Item-level).
  - Dimension Tables: Enrichment of master data with performance tiers for sellers and geographical segmentation for customers.

 -Storage: Final analytical tables are materialized as External Delta Tables in S3, ensuring high-speed query performance for Power BI while maintaining low compute costs.

 <img width="800" height="750" alt="gold-layer drawio" src="https://github.com/user-attachments/assets/f092a48f-77e6-441a-a20d-42d16dcea09f" />


## Key Features
- Data Integrity: Handled missing delivery timestamps as NULL to maintain business logic accuracy.

- Performance: Optimized storage by using Delta Lake, enabling faster queries and data versioning.

- Cloud Scalability: Decoupled compute (Databricks) from storage (S3).


## Data Source
This project utilizes the Brazilian E-Commerce Public Dataset by Olist, a comprehensive collection of 100k real-world anonymized orders from 2016 to 2018, providing a complex relational structure ideal for demonstrating large-scale data integration.

https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce/discussion?sort=hotness


<img width="800" height="626" alt="tables_relationshi drawio" src="https://github.com/user-attachments/assets/ae947aa7-0f49-4aa9-8b33-f6fb441f5ebe" />


