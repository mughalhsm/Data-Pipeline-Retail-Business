import boto3
import sys
import os
import pandas as pd
from pandasql import sqldf
import fastparquet
import csv
import io

from process_payment.src.process_payment_functions import (access_bucket, 
 payment_files_list, payment_type_files_list, transaction_files_list, 
 currency_files_list, counterparty_files_list, address_files_list, 
 df_list_payment, df_list_payment_type, df_list_transaction, df_list_currency, 
 df_list_counterparty, df_list_address, payment_files_processed_list, 
 payment_type_files_processed_list, transaction_files_processed_list, 
 currency_files_processed_list, counterparty_files_processed_list, 
 date_files_processed_list, fact_payment_tables, dim_payment_type_tables, 
 dim_transaction_tables, dim_currency_tables, dim_counterparty_tables, 
 dim_date_tables)


fact_payment_tables()
dim_payment_type_tables()
dim_transaction_tables()
dim_currency_tables()
dim_counterparty_tables()
dim_date_tables()