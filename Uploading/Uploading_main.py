import boto3
import io 
import pandas as pd 
import time
import psycopg2
from psycopg2 import sql, extensions, Error, extras
import re


conn = psycopg2.connect(database="postgres", user="project_team_3", ## Need to hide these credentials
password="e8yBk4a7EJGX2Nz", host="nc-data-eng-project-dw-prod.chpsczt8h1nu.eu-west-2.rds.amazonaws.com", port="5432")

all_table_names = ['dim_counterparty', 'dim_currency', 'dim_date', 'dim_design', 'dim_location',
'dim_payment_type', 'dim_staff', 'dim_transaction', 'fact_payment', 'fact_purchase_order', 'fact_sales_order']

cur = conn.cursor()
bucket_name = 'bosch-test-run-2-processed-bucket'
file_key = 'Ben-Test/Sales-Parquet/date/RunNum:331.parquet'


def set_table_to_update():
    '''Uses a regex of the key to determine the name of the table in question, then iterates
    over all the table names until it finds the specific table, then sets that as the global variable
    so that the rest of the file can use it '''
    print('setting table to use')
    match = re.search(r'/.*?/([^/]+)', file_key)

    if match:
        table_to_update = match.group(1)
        print(table_to_update) 

    for i in range (0,len(all_table_names)):
        if table_to_update in all_table_names[i].split('_'):
            print('specific table being uploaded = ', all_table_names[i])
            global specific_table
            specific_table = all_table_names[i]
            print('table set')


def get_parquet_data(bucket_name, key):
    '''Gets data from ingested bucket in parquet format'''
    buffer = io.BytesIO()
    client = boto3.resource('s3', aws_access_key_id="AKIAWQUV2VD3MDK4LMG3", aws_secret_access_key="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")
    object = client.Object(bucket_name, key)
    object.download_fileobj(buffer)


    global global_df
    global_df = pd.read_parquet(buffer)
    print('reading', global_df)    
    global_df.to_csv('output.csv', index=False)
    print('finished reading')

def creating_insert_string(string_count):
    '''Making a way to change the amount of values placeholder so that the insert
    works for any of the incoming tables''' 
    print('Creating insert query string with length:', string_count)

    insert_string = 'INSERT INTO {} VALUES ( %s )'         #.format(specific_table)
    placeholderStr = insert_string.split("%s")


    changing_query_string = placeholderStr[0] + "%s, " * string_count + placeholderStr[1] 
    index = changing_query_string.rfind(",")

    final_string = str(changing_query_string[:index] + changing_query_string[index+1:]).format(specific_table)
    print('This is the query string', final_string)


    return final_string



def insert_row_data(data):
    '''Inserts rows of data into the specific table'''
    print('attempting to count columns')
    string_count=0
    columns = global_df.columns
    for column in columns:
        string_count+=1
    insertString = creating_insert_string(string_count)


    print('attempting to insert row data')
    delete = 'DELETE FROM {}'.format(specific_table)
    insert = insertString
    cur.execute(delete)
    conn.commit()
    for row in data:
        print(f'Inserting row {row}')
        cur.execute(insert, row)
        conn.commit()



def get_row_values():
    '''Gets all the rows of data within the dataframe'''
    data = []
    # Iterate through each row of the dataframe
    for index, row in global_df.iterrows():
        row_data = tuple(row)
        data.append(row_data)

    insert_row_data(data)

# if file_key == 'Ben-Test/Sales-Parquet/currency/RunNum:331.parquet':   
get_parquet_data(bucket_name=bucket_name, key=file_key)

set_table_to_update()
#get_parquet_data(bucket_name=bucket_name, key=file_key)
get_row_values()





cur.close()
conn.close()





# def lambda_handler(event, context):
#     # Get the key of the object that triggered the event
#     object_key = event['Records'][0]['s3']['object']['key']
#     print(f'Object key: {object_key}')

# So if this works, then you know what key youre getting back, and then can choose which specific table to name it
# iterative if but it works 












# columns = global_df.columns.tolist()
# specific_table = "dim_currency"


# ## def insert_data(column, values):
#     tupleVals = tuple(values)
#     values = [tupleVals]
#     print(column, values)   

#     # cur.execute(SQL("INSERT INTO %s {} VALUES %s").format(Identifier(column)), (specific_table, values))
#     # conn.commit() 


# def get_column_values(columns):
#     '''Iterates over column titles of the specific dataframe, then gets the values ONLY
#     Also gets the specific column name for use in insertnig'''
#     print('Getting values for each column')
#     print(columns)
#     count = 0
#     for column in columns:
#         column_name = global_df[column]
#         values = column_name.values.tolist()

#         column_title = global_df.columns[count]
#         count+=1

#         insert_data(column_title, values)
#         # print(column, values)



# get_column_values(columns)