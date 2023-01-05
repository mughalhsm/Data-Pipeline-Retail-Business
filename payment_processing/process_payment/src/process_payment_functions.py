import boto3
import sys
import os
import pandas as pd
from pandasql import sqldf
import fastparquet
import csv
import io


# Accessing the S3 bucket using boto3
def access_bucket():
    s3_client = boto3.client("s3")
    s3_ingest_bucket_name = "bosch-deploy-23-12-22-v2-ingest-bucket"
    s3_processed_bucket_name = "bosch-deploy-23-12-22-v2-processed-bucket"
    try:
        s3 = boto3.resource("s3")
            # aws_access_key_id= "",
            # aws_secret_access_key="")
        return s3, s3_ingest_bucket_name, s3_processed_bucket_name
    except Exception as e:
        return e



## CREATING LISTS OF FILENAMES IN THE BUCKET

# "payment" files
def payment_files_list(s3_ingest_bucket_name = access_bucket()[1]):
    bucket_list_payment = []
    s3_paginator = boto3.client("s3").get_paginator("list_objects_v2")

    pagination_parameters = {"Bucket": s3_ingest_bucket_name, 
                                "Prefix": "TableName/payment/R"}
    file_iterator = s3_paginator.paginate(**pagination_parameters)
    
    try:
        for file in file_iterator:
            if "Contents" not in file:
                return []
            else:
                for item in file["Contents"]:
                    bucket_list_payment.append(item["Key"])
        
        return bucket_list_payment

    except Exception as e:
        return e


# "payment_type" files
def payment_type_files_list(s3_ingest_bucket_name = access_bucket()[1]):
    bucket_list_payment_type = []
    s3_paginator = boto3.client("s3").get_paginator("list_objects_v2")

    pagination_parameters = {"Bucket": s3_ingest_bucket_name, 
                                "Prefix": "TableName/payment_type/R"}
    file_iterator = s3_paginator.paginate(**pagination_parameters)

    try:
        for file in file_iterator:
            if "Contents" not in file:
                return []
            else:
                for item in file["Contents"]:
                    bucket_list_payment_type.append(item["Key"])

        return bucket_list_payment_type

    except Exception as e:
        return e


# "transaction" files
def transaction_files_list(s3_ingest_bucket_name = access_bucket()[1]):
    bucket_list_transaction = []
    s3_paginator = boto3.client("s3").get_paginator("list_objects_v2")

    pagination_parameters = {"Bucket": s3_ingest_bucket_name, 
                                "Prefix": "TableName/transaction/R"}
    file_iterator = s3_paginator.paginate(**pagination_parameters)

    try:
        for file in file_iterator:
            if "Contents" not in file:
                return []
            else:
                for item in file["Contents"]:
                    bucket_list_transaction.append(item["Key"])

        return bucket_list_transaction

    except Exception as e:
        return e
    

# "currency" files
def currency_files_list(s3_ingest_bucket_name = access_bucket()[1]):
    bucket_list_currency = []
    s3_paginator = boto3.client("s3").get_paginator("list_objects_v2")

    pagination_parameters = {"Bucket": s3_ingest_bucket_name, 
                                "Prefix": "TableName/currency/R"}
    file_iterator = s3_paginator.paginate(**pagination_parameters)

    try:
        for file in file_iterator:
            if "Contents" not in file:
                return []
            else:
                for item in file["Contents"]:
                    bucket_list_currency.append(item["Key"])

        return bucket_list_currency

    except Exception as e:
        return e


# "counterparty" files
def counterparty_files_list(s3_ingest_bucket_name = access_bucket()[1]):
    bucket_list_counterparty = []
    s3_paginator = boto3.client("s3").get_paginator("list_objects_v2")

    pagination_parameters = {"Bucket": s3_ingest_bucket_name,
                                "Prefix": "TableName/counterparty/R"}
    file_iterator = s3_paginator.paginate(**pagination_parameters)

    try:
        for file in file_iterator:
            if "Contents" not in file:
                return []
            else:
                for item in file["Contents"]:
                    bucket_list_counterparty.append(item["Key"])

        return bucket_list_counterparty

    except Exception as e:
        return e


# "address" files
def address_files_list(s3_ingest_bucket_name = access_bucket()[1]):
    bucket_list_address = []
    s3_paginator = boto3.client("s3").get_paginator("list_objects_v2")

    pagination_parameters = {"Bucket": s3_ingest_bucket_name,
                                "Prefix": "TableName/address/R"}
    file_iterator = s3_paginator.paginate(**pagination_parameters)

    try:
        for file in file_iterator:
            if "Contents" not in file:
                return []
            else:
                for item in file["Contents"]:
                    bucket_list_address.append(item["Key"])

        return bucket_list_address

    except Exception as e:
        return e




# READING FILES 
# Turning the contents of every file into a dataframe contained within a list

# payment
def df_list_payment(bucket_list_payment = payment_files_list(),
        s3 = access_bucket()[0], s3_ingest_bucket_name = access_bucket()[1]):
    
    df_list_payment = []

    try:
        for file in bucket_list_payment:
            obj = s3.Object(s3_ingest_bucket_name, file)
            data=obj.get()["Body"].read()
            df_list_payment.append(pd.read_csv(io.BytesIO(data), header=0,
            delimiter=",", low_memory=False))

        return df_list_payment

    except Exception as e:
        return e


# payment_type
def df_list_payment_type(bucket_list_payment_type = payment_type_files_list(),
        s3 = access_bucket()[0], s3_ingest_bucket_name = access_bucket()[1]):
    
    df_list_payment_type = []

    try:
        for file in bucket_list_payment_type:
            obj = s3.Object(s3_ingest_bucket_name, file)
            data=obj.get()["Body"].read()
            df_list_payment_type.append(pd.read_csv(io.BytesIO(data), header=0,
            delimiter=",", low_memory=False))

        return df_list_payment_type

    except Exception as e:
        return e


# transaction
def df_list_transaction(bucket_list_transaction = transaction_files_list(),
        s3 = access_bucket()[0], s3_ingest_bucket_name = access_bucket()[1]):
    
    df_list_transaction = []

    try:
        for file in bucket_list_transaction:
            obj = s3.Object(s3_ingest_bucket_name, file)
            data=obj.get()["Body"].read()
            df_list_transaction.append(pd.read_csv(io.BytesIO(data), header=0,
            delimiter=",", low_memory=False))

        return df_list_transaction

    except Exception as e:
            return e


# currency
def df_list_currency(bucket_list_currency = currency_files_list(),
        s3 = access_bucket()[0], s3_ingest_bucket_name = access_bucket()[1]):
    
    df_list_currency = []

    try:
        for file in bucket_list_currency:
            obj = s3.Object(s3_ingest_bucket_name, file)
            data=obj.get()["Body"].read()
            df_list_currency.append(pd.read_csv(io.BytesIO(data), header=0,
            delimiter=",", low_memory=False))

        return df_list_currency

    except Exception as e:
        return e


# counterparty
def df_list_counterparty(bucket_list_counterparty = counterparty_files_list(),
        s3 = access_bucket()[0], s3_ingest_bucket_name = access_bucket()[1]):
    
    df_list_counterparty = []

    try:
        for file in bucket_list_counterparty:
            obj = s3.Object(s3_ingest_bucket_name, file)
            data=obj.get()["Body"].read()
            df_list_counterparty.append(pd.read_csv(io.BytesIO(data), header=0,
            delimiter=",", low_memory=False))

        return df_list_counterparty

    except Exception as e:
        return e


# address
def df_list_address(bucket_list_address = address_files_list(),
        s3 = access_bucket()[0], s3_ingest_bucket_name = access_bucket()[1]):
    
    df_list_address = []

    try:
        for file in bucket_list_address:
            obj = s3.Object(s3_ingest_bucket_name, file)
            data=obj.get()["Body"].read()
            df_list_address.append(pd.read_csv(io.BytesIO(data), header=0, 
            delimiter=",", low_memory=False))

        return df_list_address

    except Exception as e:
        return e




# COUNTING PROCESSED FILES
# Number of items in processed buckets

# payment
def payment_files_processed_list(s3_processed_bucket_name=access_bucket()[2]):
    processed_bucket_list_payment = []
    s3_paginator = boto3.client("s3").get_paginator("list_objects_v2")

    pagination_parameters = {"Bucket": s3_processed_bucket_name,
                                "Prefix": "Payment/fact_payment/f"}
    file_iterator = s3_paginator.paginate(**pagination_parameters)

    try:
        for file in file_iterator:
            if "Contents" not in file:
                return 0, []
            else:
                for item in file["Contents"]:
                    processed_bucket_list_payment.append(item["Key"])

        return [len(processed_bucket_list_payment), processed_bucket_list_payment]

    except Exception as e:
        return e


# payment type
def payment_type_files_processed_list(
                                s3_processed_bucket_name = access_bucket()[2]):
    processed_bucket_list_payment_type = []
    s3_paginator = boto3.client("s3").get_paginator("list_objects_v2")

    pagination_parameters = {"Bucket": s3_processed_bucket_name,
                                "Prefix": "Payment/dim_payment_type/d"}
    file_iterator = s3_paginator.paginate(**pagination_parameters)

    try:
        for file in file_iterator:
            if "Contents" not in file:
                return 0, []
            else:
                for item in file["Contents"]:
                    processed_bucket_list_payment_type.append(item["Key"])

        return (len(processed_bucket_list_payment_type),
            processed_bucket_list_payment_type)

    except Exception as e:
        return e


# transaction
def transaction_files_processed_list(
                                s3_processed_bucket_name = access_bucket()[2]):
    processed_bucket_list_transaction = []
    s3_paginator = boto3.client("s3").get_paginator("list_objects_v2")

    pagination_parameters = {"Bucket": s3_processed_bucket_name,
                                "Prefix": "Payment/dim_transaction/d"}
    file_iterator = s3_paginator.paginate(**pagination_parameters)

    try:
        for file in file_iterator:
            if "Contents" not in file:
                return 0, []
            else:
                for item in file["Contents"]:
                    processed_bucket_list_transaction.append(item["Key"])

        return (len(processed_bucket_list_transaction),
            processed_bucket_list_transaction)

    except Exception as e:
        return e


# currency
def currency_files_processed_list(s3_processed_bucket_name=access_bucket()[2]):
    processed_bucket_list_currency = []
    s3_paginator = boto3.client("s3").get_paginator("list_objects_v2")

    pagination_parameters = {"Bucket": s3_processed_bucket_name,
                                "Prefix": "Payment/dim_currency/d"}
    file_iterator = s3_paginator.paginate(**pagination_parameters)

    try:
        for file in file_iterator:
            if "Contents" not in file:
                return 0, []
            else:
                for item in file["Contents"]:
                    processed_bucket_list_currency.append(item["Key"])

        return (len(processed_bucket_list_currency),
            processed_bucket_list_currency)

    except Exception as e:
        return e


#counterparty
def counterparty_files_processed_list(
                                s3_processed_bucket_name = access_bucket()[2]):
    processed_bucket_list_counterparty = []
    s3_paginator = boto3.client("s3").get_paginator("list_objects_v2")

    pagination_parameters = {"Bucket": s3_processed_bucket_name,
                                "Prefix": "Payment/dim_counterparty/d"}
    file_iterator = s3_paginator.paginate(**pagination_parameters)

    try:
        for file in file_iterator:
            if "Contents" not in file:
                return 0, []
            else:
                for item in file["Contents"]:
                    processed_bucket_list_counterparty.append(item["Key"])

        return (len(processed_bucket_list_counterparty),
            processed_bucket_list_counterparty)

    except Exception as e:
        return e


# date
def date_files_processed_list(s3_processed_bucket_name = access_bucket()[2]):
    processed_bucket_list_date = []
    s3_paginator = boto3.client("s3").get_paginator("list_objects_v2")

    pagination_parameters = {"Bucket": s3_processed_bucket_name,
                                "Prefix": "Payment/dim_date/d"}
    file_iterator = s3_paginator.paginate(**pagination_parameters)

    try:
        for file in file_iterator:
            if "Contents" not in file:
                return 0, []
            else:
                for item in file["Contents"]:
                    processed_bucket_list_date.append(item["Key"])

        return len(processed_bucket_list_date), processed_bucket_list_date

    except Exception as e:
        return e





# # SELECT QUERIES FOR FACT AND DIM TABLES

# fact_payment
def fact_payment_tables(s3_processed_bucket_name = access_bucket()[2],
                        processed_count = payment_files_processed_list(), #should be [0] but testing doesn't like it for some reason
                        df_list_payment = df_list_payment()):

    s3_client = boto3.client("s3")

    try:
        for df in df_list_payment[processed_count:]:
            processed_count += 1
            df_fact_payment = sqldf(
                f"""SELECT payment_id, DATE(created_at) AS created_date,
                TIME(created_at) AS created_time, DATE(last_updated)
                AS last_updated_date, TIME(last_updated) AS last_updated_time,
                transaction_id, counterparty_id, payment_amount, currency_id,
                payment_type_id, paid, payment_date FROM df
                ORDER BY payment_id;""")
            df_fact_payment.insert(0, "payment_record_id",
                range(1, 1 + len(df_fact_payment)))
            df_fact_payment.to_parquet(io.BytesIO())
            s3_client.put_object(
                Body=io.BytesIO().getvalue(), Bucket=s3_processed_bucket_name,
                Key=f"""Payment/fact_payment/fact_payment_{processed_count:04}.parquet""")
        return df_fact_payment # for testing purposes
    
    except Exception as e:
        print(f"Query failed on fact_payment table number: {processed_count:04}")
        return e


# dim_payment_type
def dim_payment_type_tables(s3_processed_bucket_name = access_bucket()[2],
                                processed_count = payment_type_files_processed_list(), #should be [0] but testing doesn't like it for some reason
                                df_list_payment_type = df_list_payment_type()):

    s3_client = boto3.client("s3")

    try:
        for df in df_list_payment_type[processed_count:]:
            processed_count += 1
            df_dim_payment_type = sqldf(f"""SELECT payment_type_id,
                payment_type_name FROM df;""")
            df_dim_payment_type.to_parquet(io.BytesIO())
            s3_client.put_object(
                Body=io.BytesIO().getvalue(), Bucket=s3_processed_bucket_name,
                Key=f"""Payment/dim_payment_type/dim_payment_type_{processed_count:04}.parquet""")
        return df_dim_payment_type # for testing purposes
    
    except Exception as e:
        print(f"Query failed on dim_payment_type table number: {processed_count:04}")
        return e


# dim_transaction
def dim_transaction_tables(s3_processed_bucket_name = access_bucket()[2],
                            processed_count = transaction_files_processed_list(), #should be [0] but testing doesn't like it for some reason
                            df_list_transaction = df_list_transaction()):

    s3_client = boto3.client("s3")

    try:
        for df in df_list_transaction[processed_count:]:
            processed_count += 1
            df_dim_transaction = sqldf(
                f"""SELECT transaction_id, transaction_type, printf('%.0f',
                sales_order_id) AS sales_order_id, printf('%.0f',
                purchase_order_id) AS purchase_order_id FROM df;""")
            df_dim_transaction.to_parquet(io.BytesIO())
            s3_client.put_object(
                Body=io.BytesIO().getvalue(), Bucket=s3_processed_bucket_name,
                Key=f"""Payment/dim_transaction/dim_transaction_{processed_count:04}.parquet""")
        return df_dim_transaction # for testing purposes
    
    except Exception as e:
        print(f"Query failed on transaction table number: {processed_count:04}")
        return e


# dim_currency
def dim_currency_tables(s3_processed_bucket_name = access_bucket()[2],
                        processed_count = currency_files_processed_list(), #should be [0] but testing doesn't like it for some reason
                        df_list_currency = df_list_currency()):

    s3_client = boto3.client("s3")

    try:
        for df in df_list_currency[processed_count:]:
            processed_count += 1
            df_dim_currency = sqldf(f"""SELECT currency_id, currency_code
                                        FROM df;""")
            df_dim_currency = df_dim_currency.assign(
                currency_name = ["British Pound", "US Dollar", "Euro"])
            df_dim_currency.to_parquet(io.BytesIO())
            s3_client.put_object(Body=io.BytesIO().getvalue(),
                Bucket=s3_processed_bucket_name,
                Key=f"""Payment/dim_currency/dim_currency_{processed_count:04}.parquet""")
        return df_dim_currency # for testing purposes

    except Exception as e:
        print(f"Query failed on currency table number: {processed_count:04}")
        return e


# dim_counterparty
def dim_counterparty_tables(s3_processed_bucket_name = access_bucket()[2],
                            processed_count = counterparty_files_processed_list(), #should be [0] but testing doesn't like it for some reason
                            df_list_counterparty = df_list_counterparty(),
                            df_list_address = df_list_address()):

    s3_client = boto3.client("s3")

    try:
        for df in df_list_counterparty[processed_count:]:
            print(len(df_list_address), "DF_L_A")
            print(len(df_list_counterparty), "DF_L_C")
            print(df, "DF")
            print(processed_count, "PROCESSED COUNT")
            df2 = df_list_address[processed_count]
            print(df2, "DF2")
            processed_count += 1
            print(0, "--->")
            df_dim_counterparty = sqldf(
                f"""SELECT counterparty_id, counterparty_legal_name,
                address_line_1 AS counterparty_legal_address_line_1,
                address_line_2 AS counterparty_legal_address_line_2, district
                AS counterparty_legal_district, city AS
                counterparty_legal_city, postal_code AS
                counterparty_legal_postal_code, country AS
                counterparty_legal_country, phone AS
                counterparty_legal_phone_number FROM df INNER JOIN df2 ON
                df.legal_address_id = df2.address_id;""")
            print(df_dim_counterparty, "DF_D_C")    
            df_dim_counterparty.to_parquet(io.BytesIO())
            s3_client.put_object(Body=io.BytesIO().getvalue(),
                Bucket=s3_processed_bucket_name,
                Key=f"""Payment/dim_counterparty/dim_counterparty_{processed_count:04}.parquet""")
        print(df_dim_counterparty)
        return df_dim_counterparty # for testing purposes
    
    except Exception as e:
        print(f"Query failed on counterparty table number: {processed_count:04}")
        return e


# dim_date
def dim_date_tables(s3_processed_bucket_name = access_bucket()[2],
                    processed_count = date_files_processed_list(), #should be [0] but testing doesn't like it for some reason
                    df_list_payment = df_list_payment()):

    s3_client = boto3.client("s3")

    df_dim_date = sqldf(f"""WITH RECURSIVE dates(date) AS (VALUES("2022-11-03")
    UNION ALL SELECT date(date, "+1 day") FROM dates WHERE date < "2023-12-31")
    SELECT date AS date_id, strftime("%Y", date) AS year, strftime("%m", date)
    AS month, strftime("%d", date) AS day, CASE strftime("%w", date) WHEN "0" 
    THEN "7" WHEN "1" THEN "1" WHEN "2" THEN "2" WHEN "3" THEN "3" WHEN "4"
    THEN "4" WHEN "5" THEN "5" WHEN "6" THEN "6" END AS day_of_week, CASE
    strftime("%w", date) WHEN "0" THEN "Sunday" WHEN "1" THEN "Monday" WHEN
    "2" THEN "Tuesday" WHEN "3" THEN "Wednesday" WHEN "4" THEN "Thursday" WHEN
    "5" THEN "Friday" WHEN "6" THEN "Saturday" END AS day_name, CASE
    strftime("%m", date) WHEN "01" THEN "January" WHEN "02" THEN "Febuary" WHEN
    "03" THEN "March" WHEN "04" THEN "April" WHEN "05" THEN "May" WHEN "06"
    THEN "June" WHEN "07" THEN "July" WHEN "08" THEN "August" WHEN "09" THEN
    "September" WHEN "10" THEN "October" WHEN "11" THEN "November" WHEN "12"
    THEN "December" END AS month_name, (strftime("%m", date) + 2) / 3 AS
    quarter FROM dates;""")

    try:
        print(len(df_list_payment), "test")
        for n in range(0, len(df_list_payment)):
            processed_count += 1
            df_dim_date.to_parquet(io.BytesIO())
            s3_client.put_object(Body=io.BytesIO().getvalue(),
            Bucket=s3_processed_bucket_name,
            Key=f"Payment/dim_date/dim_date_{processed_count:04}.parquet")
        return df_dim_date # for testing purposes
    
    except Exception as e:
        print(f"Query failed on date table number: {processed_count:04}")
        return e
