import pandas as pd

def create_staff_dim_dataframe(staff_df, department_df):
    try:
        # Joining staff and department tables on department ID.
        joined_df = pd.merge(left = staff_df, right = department_df, how = 'inner', on = 'department_id')
        
        # Altering column values : dropping and changing order.
        new_df = joined_df.drop(['created_at_x', 'last_updated_x', 'manager', 'created_at_y', 'last_updated_y', 'last_updated_y', 'department_id'], axis=1)
        new_df = new_df.sort_values(by=['staff_id']).reset_index(drop=True)
        new_df = new_df.loc[:,['staff_id','first_name','last_name', 'department_name', 'location', 'email_address']]
    except Exception as e:
        raise e
    else:
        return new_df

def create_counterparty_dim_dataframe(counterparty_df, address_df):
    try:
        ## Joining counterparty and address table on address ID
        joined_df = pd.merge(left = counterparty_df, right = address_df, how = 'inner', left_on = 'legal_address_id', right_on = 'address_id')

        ## Altering column values. 
        new_df = joined_df.drop(['delivery_contact', 'commercial_contact', 'created_at_x', 'last_updated_x', 'created_at_y', 'last_updated_y', 'legal_address_id'], axis=1)
        new_df = new_df.sort_values(by=['counterparty_id']).reset_index(drop=True)
        new_df.rename(columns = {
            'address_id':'counterparty_address_id', 
            'address_line_1':'counterparty_address_line_1', 
            "address_line_2" : "counterparty_address_line_2", 
            'district' : "counterparty_district",
            'city' : "counterparty_city",
            'postal_code' : "counterparty_postal_code",
            'country' : "counterparty_country",
            'phone' : "counterparty_phone"
            }, inplace = True)
        print(new_df.columns)
    except KeyError as e:
        print("Incorrect dataframe as arguement or order of arguements incorrect - please check")
        raise e
    except TypeError as e:
        print('Incorrect type of arguement - arguements must be dataframes.')
        raise e
    except Exception as e:
        raise e
    else: 
        return new_df


def create_currency_dim_dataframe(currency_df):
    try:
        # Creating currency dataframe.
        currency_df = currency_df.drop(['created_at', 'last_updated'], axis=1)
        currency_dict = {'USD' : "US Dollars", 'GBP' : "Pound Sterling", 'EUR' : 'Euro'}
        currency_df['currency_name'] = currency_df['currency_code'].map(currency_dict)
    except KeyError as e:
        print("Incorrect dataframe as arguement or order of arguements incorrect - please check")
        raise e
    except AttributeError as e:
        print('Incorrect type of arguement - arguements must be dataframes.')
        raise e
    except Exception as e:
        raise e
    else:
        return currency_df

def create_location_dim_dataframe(address_df):
    try:
        #  Creating address dataframe.
        if "address_id" not in address_df.columns:
            raise KeyError
        address_df = address_df.drop(['created_at', 'last_updated'], axis=1)
        address_df.rename(columns = { "address_id" : "location_id"})
    except Exception as e:
        raise e
    else:
        return address_df

def create_date_dim_dataframe():
    try:
        # Creating date dim dataframe
        df = pd.DataFrame(pd.date_range('1/1/2020','12/31/2025'), columns=['date_id'])
        df['year'] = df['date_id'].dt.year
        df['month'] = df['date_id'].dt.month
        df["day"] = df['date_id'].dt.day
        df["day_of_week"] = df['date_id'].dt.dayofweek
        df['day_name'] = df['date_id'].dt.strftime("%A")
        df['month_name'] = df['date_id'].dt.strftime("%B")
        df['quarter'] = df['date_id'].dt.quarter
    except Exception as e:
        raise e
    else:
        return df


