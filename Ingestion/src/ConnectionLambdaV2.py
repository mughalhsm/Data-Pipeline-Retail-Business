#RUN 2ND after RunNumber has been incremented
import pg8000
import pandas as pd
import datetime
import boto3
import logging
from botocore.exceptions import ClientError
from DeleteLastRunNum import delete_last_run_num_object
from Credentials import get_credentials
from RunNumberTracker import num_track_run_func, check_input_details_correct, check_bucket, create_initial_time_stamp_file, getting_last_object, check_if_empty_bucket, push_updated_file_back_to_bucket, increment_run_number
import time
#wrap function and invoke at bottom if not using lambda handler
def my_handler(event, context):
    def wrap():
        s3res = boto3.resource('s3')
        bucket_name = 'cees-nc-test-bucket-2'
        prefix = 'Run-tracker'
        check_input_details_correct()
        bucket = s3res.Bucket(bucket_name)
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)

        s3=boto3.client("s3")
        ## setting authentication using export in cli and environment variables
                            
        # Getting the current date and time for use as timestamp
        dt = datetime.datetime.now()
        try:
            credentials = get_credentials('totesys_credentials')
        except TypeError as te:
            logging.error('Missing argument name')
        # Connect to the database
        try:
            conn = pg8000.connect(
                host=credentials[2],
                port=5432,
                user=credentials[0],
                password=credentials[1],
                database="totesys"
            )
            logger.info('DB Connection successful')

        except pg8000.InterfaceError as DBConnectionError:
            logging.error("An interface error occurred:", DBConnectionError, 'Your credentials may be incorrect')
            quit()
        except IndexError as ie:
            logging.error('Incorrect tuple usage', ie)
            quit()
        

        increment = num_track_run_func()

        cursor = conn.cursor()
        cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'") ## gets all table_names from the DB, assuming public
        tables = cursor.fetchall()
        logging.info(f"All table names within the database: {tables}")



        #iterative processs begins, formats data, then ingests into specific parts
        try:
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





                logging.info(f'Ingestion succesful for: {table_name}, run count: {increment}')
                try:
                    s3.put_object(Bucket='cees-nc-test-bucket-2', Key=f"TableName/{table_name}/RunNum:{increment}.csv", Body=dataframe_as_csv),
                except ClientError as ce:
                    logging.error(ce)
                    quit()
        except Exception as e:
            logging.error('Check that file name and run num still aligns', e)
            delete_last_run_num_object(bucket_name, prefix)


            ## So now above, i have the table together, now need to put it in the bucket, with a distinct file name
            ## after putting the dataframe into a csv which is needed for put object


        cursor.close()  
        conn.close()

    wrap()


