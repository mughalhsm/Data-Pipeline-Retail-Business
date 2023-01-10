# Data Pipeline - Retail Business

## Application
- Extract, Transform and Load data from a prepared source into a data lake and warehouse hosted in AWS.
- Two S3 buckets (one for ingested data and one for processed data). Both buckets are structured and well-organised so that data is easy to find.
- A Python application that ingests all tables from the `totesys` database (details below). The data saved in files in the "ingestion" S3 bucket in a csv format.
  - operate automatically on a schedule
  - trigger email alerts in the event of failures
  - follows good security practices (for example, preventing SQL injection and maintaining password security).
- A Python application that remodels data into a predefined schema suitable for a data warehouse and stores the data in `parquet` format in the "processed" S3 bucket.
  - triggers automatically when it detects the completion of an ingested data job
- A Python application that loads the data into a prepared data warehouse at defined intervals. Again the application should be adequately logged and monitored.

All Python code thoroughly tested.

Deployed via a scripted solution written in Python.

You should be able to demonstrate that a change to the source database will be reflected in the data warehouse within 30 minutes at most.

## Technical Details
The primary data source for the project is a moderately complex database called `totesys` which is meant to simulate the back end data of a commercial application. Data is inserted and updated into this database several times a day.


The data remodelled for this warehouse into three overlapping star schemas. You can find the ERDs for these star schemas:
 - ["Sales" schema](https://dbdiagram.io/d/637a423fc9abfc611173f637)
 - ["Purchases" schema](https://dbdiagram.io/d/637b3e8bc9abfc61117419ee)
 - ["Payments" schema](https://dbdiagram.io/d/637b41a5c9abfc6111741ae8)

The overall structure of the resulting data warehouse is shown [here](https://dbdiagram.io/d/637b4c6dc9abfc6111741e65).

### Components
1. A job scheduler to run the ingestion job. AWS Eventbridge is used to do this. 
1. An S3 bucket which will act as a "landing zone" for ingested data.
1. A Python application to check for the changes to the database tables and ingest any new or updated data. 
1. A second S3 bucket for "processed" data.
1. A Python application to transform data landing in the "ingestion" S3 bucket and place the results in the "processed" S3 bucket. The data transformed to conform to the warehouse schema (see below). The job triggered by either an S3 event triggered when data lands in the ingestion bucket, or on a schedule. 
1. A Python application that will periodically schedule an update of the data warehouse from the data in S3.

The tables to be ingested from the `totesys` source database are:
|tablename|
|----------|
|counterparty|
|currency|
|department|
|design|
|staff|
|sales_order|
|address|
|payment|
|purchase_order|
|payment_type|
|transaction|

The list of tables in the complete warehouse is:
|tablename|
|---------|
|fact_sales_order|
|fact_purchase_orders|
|fact_payment|
|dim_transaction|
|dim_staff|
|dim_payment_type|
|dim_location|
|dim_design|
|dim_date|
|dim_currency|
|dim_counterparty|

