import boto3
from datetime import datetime as dt
import pandas as pd
from io import StringIO, BytesIO
import fastparquet
from botocore.exceptions import ClientError


def get_run_number():
    try:
        s3 = boto3.client('s3')
        run_num_dict = s3.list_objects_v2(Bucket='bosch-test-run-2-ingest-bucket', Prefix='Run-tracker')['Contents']
        run_num_dict = run_num_dict[1:]

        run_num_list = [int(run['Key'][22:len(run['Key'])-4]) for run in run_num_dict]
        
        latest = max(run_num_list)
        print(latest)
        return latest
    except KeyError:
        raise KeyError('No runs found')
    except ClientError:
        raise ImportError('Error connecting to AWS')
    except Exception as e:
        raise Exception('Run Number retrieval failed')

def retrieve_ingested_csv(table_name, run_number):
    try:
        s3 = boto3.client('s3')

        table_names = ['address','counterparty','currency','department','design','sales_order','staff']
        if table_name not in table_names:
            raise LookupError('Invalid table name')
        elif isinstance(run_number,int) == False:
            raise LookupError('Invalid run number')
        
        file_key = f'TableName/{table_name}/RunNum:{run_number}.csv'

        file = s3.get_object(Bucket='bosch-test-run-2-ingest-bucket', Key=file_key)['Body'].read().decode()
        return file

    except LookupError:
        raise LookupError('Invalid Table Name')
    except Exception:
        raise Exception('Error retrieving from ingested bucket')


    
def csvString_to_dict(csv_data):
    rows = csv_data.split('\n')
  
    rows_list = []
    for row in rows:
        if len(row) > 0:
            separated = row.split(',')
            rows_list.append(separated)
    headers = rows_list[0]
    data_dict = {}
    for i in range(len(headers)):
        column = []
        for j in range(1, len(rows_list)):
            column.append(rows_list[j][i].replace('<comma>',','))
        data_dict[headers[i]] = column

    return data_dict
    

def flatten(value):

	if type(value) is not list:
		yield value
		return

	for subvalue in value:
		yield from flatten(subvalue)







def create_fact_sales_order_dataframe(sales_order):
    try:
        new_sales_order = pd.DataFrame()

        
        # convert id columns to ints
        column_names = sales_order.columns.values.tolist()
        for column in column_names:
            if column.endswith('_id'):
                new_sales_order[column] = sales_order[column].astype('int')
        # add units sold/price here to preserve order
            if column == 'units_sold':
                new_sales_order['units_sold'] = sales_order['units_sold'].astype('int')
            if column == 'unit_price':
                new_sales_order['unit_price'] = sales_order['unit_price'].astype('float')
        # convert agreed delivery and payment dates from strings to dates
            if column.startswith('agreed') and not column.endswith('id'):
                new_sales_order[column] = pd.to_datetime(sales_order[column]).dt.date
        
        # convert string to datetime then split into date and time columns
        new_sales_order['created_at'] = pd.to_datetime(sales_order['created_at'])

        new_sales_order['created_date'] = new_sales_order['created_at'].dt.date
        new_sales_order['created_time'] = new_sales_order['created_at'].dt.time

        new_sales_order['last_updated'] = pd.to_datetime(sales_order['last_updated'])

        new_sales_order['last_updated_date'] = new_sales_order['last_updated'].dt.date
        new_sales_order['last_updated_time'] = new_sales_order['last_updated'].dt.time
        # drop redundant datetime columns
        new_sales_order.drop(['created_at','last_updated'], axis=1, inplace=True)
                
        return new_sales_order
    except KeyError:
        raise KeyError('Passed Dataframe does not have the required columns')
    except Exception:
        raise Exception('Error creating sales order dataframe')


def create_staff_dim_table(staff_df,dept_df):
    try:
        # add names, emails and staff id to new dataframe, convert staff id from string to int
        new_df = pd.DataFrame()
        new_df['staff_id'] = staff_df['staff_id'].astype('int')
        new_df['first_name'] = staff_df['first_name']
        new_df['last_name'] = staff_df['last_name']
        new_df['email_address'] = staff_df['email_address']
        # get location and department name from from dept df using id
        temp_df = pd.merge(staff_df,dept_df, on='department_id')
    
        new_df['department_name'] = temp_df['department_name']
        new_df['location'] = temp_df['location']
        
        return new_df
    except KeyError:
        raise KeyError('Passed Dataframe does not have the required columns')
    except Exception:
        raise Exception('Error creating staff dataframe')

def create_currency_dim_table(currency_df):
    try:
        # currency name not in source data, create a lookup df that can be added to if more currencies are accepted
        name_lookup = pd.DataFrame({'currency_code':['GBP', 'USD', 'EUR'], 'currency_name':['Great British Pound', 'US Dollar', 'Euro']})
        # merge currency and name lookup
        new_df = pd.merge(currency_df,name_lookup, on='currency_code')
        # convert id to type INT and drop unwanted columns
        new_df['currency_id'] = new_df['currency_id'].astype('int')
        new_df.drop(['created_at', 'last_updated'], axis=1, inplace=True)
    except KeyError:
        raise KeyError('Passed currency Dataframe does not have the required columns')
    except Exception:
        raise Exception('Error creating currency dataframe')


    return new_df

def create_design_dim_table(design_df):
    try:
        new_df = pd.DataFrame()
        # all the desired columns are present in design df. Add them, converting the id from string to INT
        new_df['design_id'] = design_df['design_id'].astype('int')
        new_df['design_name'] = design_df['design_name']
        new_df['file_location'] = design_df['file_location']
        new_df['file_name'] = design_df['file_name']
        return new_df
    except KeyError:
        raise KeyError('Passed design Dataframe does not have the required columns')
    except Exception:
        raise Exception('Error creating design dataframe')
    

def create_counterparty_dim_table(counterparty_df, address_df):
    try:
        # merge the dfs on address id
        counterparty_df['address_id'] = counterparty_df['legal_address_id']
        temp_df = pd.merge(counterparty_df,address_df,on='address_id')
        # select the desired columns, no type changes required
        new_df = pd.DataFrame()
        new_df['counterparty_id'] = temp_df['counterparty_id'].astype('int')
        new_df['counterparty_legal_name'] = temp_df['counterparty_legal_name']
        new_df['counterparty_legal_address_line_1'] = temp_df['address_line_1']
        new_df['counterparty_legal_address_line_2'] = temp_df['address_line_2']
        new_df['counterparty_legal_district'] = temp_df['district']
        new_df['counterparty_legal_city'] = temp_df['city']
        new_df['counterparty_legal_postal_code'] = temp_df['postal_code']
        new_df['counterparty_legal_country'] = temp_df['country']
        new_df['counterparty_legal_phone_number'] = temp_df['phone']
        return new_df
    except KeyError:
        raise KeyError('Passed Dataframe does not have the required columns')
    except Exception:
        raise Exception('Error creating counterparty dataframe')

def create_date_dim_table(sales_df):
    try:
        date_column_names = ['created_at','last_updated','agreed_delivery_date','agreed_payment_date']
        # get list of all dates in all date colums
        dates_list = []
        temp_df = pd.DataFrame()
        for column in date_column_names:      
            temp_df['date'] = pd.to_datetime(sales_df[column]).dt.date
            dates = temp_df['date'].values.tolist()
            dates_list.append(dates)
        # ensure list is flat and unique
        flat_list = list(flatten(dates_list))
        date_set = set(flat_list)
        unique_dates = list(date_set)

        # make new df for the dates and for quarters
        date_df = pd.DataFrame({'date_id':unique_dates})
        quarter_df = pd.DataFrame({'month':[1,2,3,4,5,6,7,8,9,10,11,12],'quarter':[1,1,1,2,2,2,3,3,3,4,4,4]})

        # use date id column to produce required columns except quarter and convert to required data type
        
        date_df['year'] = pd.to_datetime(pd.to_datetime(date_df['date_id'])).dt.year.astype('int')
        date_df['month'] = pd.to_datetime(date_df['date_id']).dt.month.astype('int')
        date_df['day'] = pd.to_datetime(date_df['date_id']).dt.day.astype('int')
        date_df['day_of_week'] = pd.to_datetime(date_df['date_id']).dt.day_of_week.astype('int')
        date_df['day_name'] = pd.to_datetime(date_df['date_id']).dt.day_name().astype('str')
        date_df['month_name'] = pd.to_datetime(date_df['date_id']).dt.month_name().astype('str')

        # merge quarter df

        date_df = pd.merge(date_df,quarter_df, on='month')

        return date_df
    except KeyError:
        raise KeyError('Passed Dataframe does not have the required columns')
    except Exception:
        raise Exception('Error creating date dataframe')


def create_location_dim_table(address_df):
    try:
        loc_df = pd.DataFrame()

        loc_df['location_id'] = address_df['address_id']
        straight_swap_columns = ['address_line_1','address_line_2', 'district', 'city', 'postal_code', 'country', 'phone']
        for column in straight_swap_columns:
            loc_df[column] = address_df[column]
        
        return loc_df

    except KeyError:
        raise KeyError('Passed address dataframe invalid')
    except Exception:
        raise Exception('Error creating location dataframe')

def save_to_processed_sales_bucket_as_csv(table_name, run_num, dataframe):
    try:
        table_names = ['address','counterparty','currency','department','design','sales_order','staff']
        
        if table_name not in table_names:
            raise LookupError()
        elif run_num.isinstance(int) == False:
            raise TypeError()

        s3 = boto3.client('s3')
        csv_buffer = StringIO()
        dataframe.to_csv(csv_buffer)

        s3.put_object(Body=csv_buffer.getvalue(), Bucket='bosch-test-run-2-processed-bucket', Key=f'Ben-Test/Sales/{table_name}/RunNum:{run_num}.csv')

        print(f'{table_name},{run_num} Saved!')
    except TypeError:
        raise TypeError('Invalid Run Number')
    except LookupError:
        raise LookupError('Invalid Table Name')
    except Exception:
        raise Exception(f'Error writing processed table file for {table_name} {run_num}')



def save_to_processed_sales_bucket(table_name, run_num, dataframe):
    try:
        table_names = ['location','counterparty','currency','department','design','sales_order','staff', 'date']

        if table_name not in table_names:
            raise LookupError()
        elif isinstance(run_num, int) == False:
            raise TypeError()
        

        s3 = boto3.client('s3')
        parquet_buffer = BytesIO()
        dataframe.to_parquet(parquet_buffer)

        s3.put_object(Body=parquet_buffer.getvalue(), Bucket='bosch-test-run-3-processed-bucket', Key=f'Ben-Test/Sales-Parquet/{table_name}/RunNum:{run_num}.parquet')

        print(f'{table_name},{run_num} Saved!')
    except TypeError:
        raise TypeError('Invalid Run Number')
    except LookupError:
        raise LookupError(f'Invalid Table Name {table_name}')
    except Exception:
        raise Exception(f'Error writing processed table file for {table_name} {run_num}')


