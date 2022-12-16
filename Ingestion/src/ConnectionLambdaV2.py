#RUN 2ND after RunNumber has been incremented
import pg8000
import pandas as pd
import datetime
import boto3
import logging
from botocore.exceptions import ClientError
from RunNumberTracker import check_input_details_correct, check_bucket, create_initial_time_stamp_file, getting_last_object, check_if_empty_bucket, push_updated_file_back_to_bucket, increment_run_number
import time
def wrap():
    s3res = boto3.resource('s3')
    bucket_name = 'cees-nc-test-bucket-2'
    prefix = 'Run-tracker'


    check_input_details_correct()
    bucket = s3res.Bucket(bucket_name)
#wrap function and invoke at bottom if not using lambda handler

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    s3=boto3.client("s3")
    ## setting authentication using export in cli and environment variables
                        
    # Getting the current date and time for use as timestamp
    dt = datetime.datetime.now()

    # Connect to the database
    try:
        conn = pg8000.connect(
            host="nc-data-eng-totesys-production.chpsczt8h1nu.eu-west-2.rds.amazonaws.com",
            port=5432,
            user="project_user_3",
            password="EbD7qkwt5xWYUwnx2nbhdvkC",
            database="totesys"
        )
        logger.info('DB Connection successful')

    except pg8000.InterfaceError as DBConnectionError:
        logging.error("An interface error occurred:", DBConnectionError, 'Your credentials may be incorrect')
        quit()



    if check_if_empty_bucket() == 0:  
        print('Bucket is empty...')  
        create_initial_time_stamp_file()   
    else:
        print('Bucket is not empty') 

    if check_bucket(bucket)==True: ### This means i have access to the bucket, now we obtain last item which allows us to increment
        increment = getting_last_object(bucket_name, prefix)
        print(increment)
    if increment == None:
        print('Something is very wrong, check prefix and bucket_name are correct')
        quit()

    logging.info('Run number updated successfully, can continue to ingestion')

   

    cursor = conn.cursor()

    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'") ## gets all table_names from the DB, assuming public
    tables = cursor.fetchall()
    logging.info(f"All table names within the database: {tables}")



    #iterative processs begins, formats data, then ingests into specific parts
    for table in tables:     ## Goes over each table, then selects all from the table
        table_name = table[0] 

        cursor.execute(f"SELECT * FROM {table_name}")
        rows = cursor.fetchall() ## This is all the data, excluding columns
        logger.info(f"Queried table '{table_name}' with {len(rows)} rows")

        cursor.execute(
        f"SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = '{table_name}'")
        column_names=cursor.fetchall() 
        

        ## Now you have the columns and rows as lists inside of tuples
        table_row_data = list(rows)
        list_of_column_names=[]
        for name in column_names:
            list_of_column_names.append(name[0])
        ##formatted above for pandas, have colums as a list and data as nested list containing rows
        ## now to combine into a dataframe to upload column names with their respective data

        Database_df = pd.DataFrame(table_row_data, columns=list_of_column_names)        
        dataframe_as_csv = Database_df.to_csv(index=False)



        print(table_name)
        print('New run count', increment)
        print('Ingesting...')
        logging.info(f'Ingestion for {table_name}')
        try:
            s3.put_object(Bucket='cees-nc-test-bucket-2', Key=f"TableName/{table_name}/RunNum:{increment}.csv", Body=dataframe_as_csv),
        except ClientError as ce:
            logger.error(ce)
            quit()
        ## So now above, i have the table together, now need to put it in the bucket, with a distinct file name
        ## after putting the dataframe into a csv which is needed for put object


    cursor.close()  
    conn.close()

wrap()


