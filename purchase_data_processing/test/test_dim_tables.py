from purchase_data_processing.src.dim_tables import create_staff_dim_dataframe, create_counterparty_dim_dataframe, create_currency_dim_dataframe, create_location_dim_dataframe, create_date_dim_dataframe
import pandas as pd
import pytest

def test_return_correct_staff_dim_dataframe_when_given_correct_tables_as_argument_in_correct_order():
    staff_df = pd.read_csv('test_table_files/staff.csv')
    department_df = pd.read_csv('test_table_files/department.csv')
    staff_dim_df = create_staff_dim_dataframe(staff_df, department_df)
    list_column_names = list(staff_dim_df.columns)
    excepted_list_column_name = ['staff_id','first_name','last_name', 'department_name', 'location', 'email_address']
    assert list_column_names == excepted_list_column_name
    assert len(staff_dim_df.index) == 20


def test_return_correct_staff_dim_dataframe_when_given_correct_tables_as_argument_incorrect_order():
    staff_df = pd.read_csv('test_table_files/staff.csv')
    department_df = pd.read_csv('test_table_files/department.csv')
    staff_dim_df = create_staff_dim_dataframe(department_df, staff_df)
    list_column_names = list(staff_dim_df.columns)
    excepted_list_column_name = ['staff_id','first_name','last_name', 'department_name', 'location', 'email_address']
    assert list_column_names == excepted_list_column_name
    assert len(staff_dim_df.index) == 20



def test_create_staff_dim_throws_error_when_given_incorrect_datatype_as_argument():
    with pytest.raises(TypeError):
        create_staff_dim_dataframe("12", 123)



def test_return_correct_counterparty_dim_dataframe_with_correct_arguements():
    counterparty_df = pd.read_csv('test_table_files/counterparty.csv')
    address_df = pd.read_csv('test_table_files/address.csv')
    counterparty_dim_df = create_counterparty_dim_dataframe(counterparty_df, address_df)
    list_column_names = list(counterparty_dim_df.columns)
    excepted_list_column_name = ['counterparty_id', 'counterparty_legal_name', 'counterparty_address_id',
       'counterparty_address_line_1', 'counterparty_address_line_2',
       'counterparty_district', 'counterparty_city',
       'counterparty_postal_code', 'counterparty_country',
       'counterparty_phone']
    assert list_column_names == excepted_list_column_name
    assert len(counterparty_dim_df.index) == 20


    
def test_create_counterparty_dim_throws_error_when_argumements_incorrect_order():
    counterparty_df = pd.read_csv('test_table_files/counterparty.csv')
    address_df = pd.read_csv('test_table_files/address.csv')
    with pytest.raises(KeyError):
        create_counterparty_dim_dataframe(address_df, counterparty_df)



def test_create_counterparty_dim_throws_error_when_incorrect_arguements():
    counterparty_df = pd.read_csv('test_table_files/counterparty.csv')
    address_df = pd.read_csv('test_table_files/address.csv')
    with pytest.raises(TypeError):
        create_counterparty_dim_dataframe('Hello', True)



def test_create_currency_dim_table_created_correctly():
    currency_df = pd.read_csv('test_table_files/currency.csv')
    currency_dim_df = create_currency_dim_dataframe(currency_df)
    list_column_names = list(currency_dim_df.columns)
    excepted_list_column_name = ['currency_id', 'currency_code', 'currency_name']
    currency_dataframe_values = currency_dim_df.values
    expected_values = [[1, 'GBP', 'Pound Sterling'], [2, 'USD', 'US Dollars'], [3, 'EUR', 'Euro']]
    assert list_column_names == excepted_list_column_name
    assert currency_dataframe_values.tolist() == expected_values



def test_create_currency_dim__raises_error_incorrect_arguements():
    counterparty_df = pd.read_csv('test_table_files/counterparty.csv')
    with pytest.raises(KeyError):
        create_currency_dim_dataframe(counterparty_df)
    with pytest.raises(AttributeError):
        create_currency_dim_dataframe(12)



def test_create_location_dim_table_created_correctly():
    address_df = pd.read_csv('test_table_files/address.csv')
    location_dim_df = create_location_dim_dataframe(address_df)
    list_column_names = list(location_dim_df.columns)
    excepted_list_column_name = ['address_id', 'address_line_1', 'address_line_2', 'district', 'city', 'postal_code', 'country', 'phone']
    assert list_column_names == excepted_list_column_name
    assert len(location_dim_df.index) == 30


def test_create_location_dim__raises_error_incorrect_arguements():
    counterparty_df = pd.read_csv('test_table_files/counterparty.csv')
    with pytest.raises(AttributeError):
        create_currency_dim_dataframe(12)
    with pytest.raises(KeyError):
        create_location_dim_dataframe(counterparty_df)


def test_create_date_dim_table_correctly():
   date_dim_df = create_date_dim_dataframe()
   date_dim_column_values = date_dim_df.columns.tolist()
   expected_column_values =  ['date_id', 'year', 'month', 'day', 'day_of_week', 'day_name', 'month_name', 'quarter']
   assert date_dim_column_values == expected_column_values




