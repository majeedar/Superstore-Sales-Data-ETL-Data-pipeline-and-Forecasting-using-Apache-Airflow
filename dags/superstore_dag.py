from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from datetime import datetime, timedelta
import pandas as pd
import os

MSSQL_SOURCE_CONN_ID = "mssql_maj_db"
MSSQL_TARGET_CONN_ID = "mssql_default"  # Assuming this is your production DB connection for OLTP
MSSQL_TARGET_FORECAST_CONN_ID = "mssql_prod2"  # New connection for forecast data (OLAP)
OUTPUT_DIR = '/opt/airflow/logs/extracted_data'
OLD_RECORDS_FILE = os.path.join(OUTPUT_DIR, 'old_records.csv')
NEW_RECORDS_FILE = os.path.join(OUTPUT_DIR, 'new_records.csv')
MODIFIED_RECORDS_FILE = os.path.join(OUTPUT_DIR, 'modified_records.csv')
AGGREGATE_SALES_FILE = os.path.join(OUTPUT_DIR, 'aggregate_sales.csv')
PAST_FUTURE_SALES_FILE = os.path.join(OUTPUT_DIR, 'past_future_sales.csv')
FORECAST_METRICS_FILE = os.path.join(OUTPUT_DIR, 'forecast_metrics.csv')
FACT_TABLE_FILE = os.path.join(OUTPUT_DIR, 'fact_table.csv')
DIM_PRODUCT_FILE = os.path.join(OUTPUT_DIR, 'dim_product.csv')
DIM_ORDER_FILE = os.path.join(OUTPUT_DIR, 'dim_order.csv')
DIM_CUSTOMER_FILE = os.path.join(OUTPUT_DIR, 'dim_customer.csv')
DIM_LOCATION_FILE = os.path.join(OUTPUT_DIR, 'dim_location.csv')
DIM_DATE_FILE = os.path.join(OUTPUT_DIR, 'dim_date.csv')


def extract_superstore_data():
    hook = MsSqlHook(mssql_conn_id=MSSQL_SOURCE_CONN_ID)
    query = "SELECT * FROM superstore"
    df = hook.get_pandas_df(sql=query)
    return df


def compare_and_save_records(**kwargs):
    ti = kwargs['ti']
    df_new = ti.xcom_pull(task_ids='extract_superstore_data', key='return_value')

    df_old = None
    if os.path.exists(OLD_RECORDS_FILE):
        df_old = pd.read_csv(OLD_RECORDS_FILE)
        for col in df_new.columns:
            if col in df_old.columns:
                if df_new[col].dtype != df_old[col].dtype:
                    try:
                        df_old[col] = df_old[col].astype(df_new[col].dtype)
                    except Exception as e:
                        print(f"Warning: Could not align data type for column '{col}': {e}")

    new_records = pd.DataFrame()
    modified_records = pd.DataFrame()

    if df_old is not None and not df_old.empty:
        new_records = df_new[~df_new.astype(str).apply(tuple, axis=1).isin(df_old.astype(str).apply(tuple, axis=1))]
        merged_df = pd.merge(df_old, df_new, how='inner', on=list(df_old.columns), suffixes=('_old', '_new'),
                                  indicator=True)
        modified_df = merged_df[merged_df['_merge'] == 'both']
        modified_records_list = []
        for index, row in modified_df.iterrows():
            old_row = row[[col for col in row.index if col.endswith('_old')]].rename(
                {col: col[:-4] for col in row.index if col.endswith('_old')})
            new_row = row[[col for col in row.index if col.endswith('_new')]].rename(
                {col: col[:-4] for col in row.index if col.endswith('_new')})
            if not old_row.equals(new_row):
                modified_records_list.append(new_row)
        if modified_records_list:
            modified_records = pd.DataFrame(modified_records_list)

    else:
        new_records = df_new

    print(f"Number of new records/rows added: {len(new_records)}")
    print(f"Number of old records modified: {len(modified_records)}")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    if not new_records.empty:
        new_records.to_csv(NEW_RECORDS_FILE, index=False)
    elif os.path.exists(NEW_RECORDS_FILE):
        os.remove(NEW_RECORDS_FILE)

    if not modified_records.empty:
        modified_records.to_csv(MODIFIED_RECORDS_FILE, index=False)
    elif os.path.exists(MODIFIED_RECORDS_FILE):
        os.remove(MODIFIED_RECORDS_FILE)

    df_new.to_csv(OLD_RECORDS_FILE, index=False)
    return df_new


def transform_superstore_data(**kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='compare_and_save_records', key='return_value')  # Pull from compare_records

    if df is None:
        print("Error: DataFrame not received from compare_and_save_records")
        return None, None, None, None, None, None, None

    print("Columns in the DataFrame received for transformation:")
    print(df.columns)

    # Convert 'Order Date' to datetime early on
    df['Order Date'] = pd.to_datetime(df['Order Date'])

    df['unit_price'] = df['Sales'] / df['Quantity']

    # Create aggregate sales DataFrame
    aggregate_sales_df = df.groupby('Order Date')['Sales'].sum().reset_index()
    aggregate_sales_df.rename(columns={'Order Date': 'ds', 'Sales': 'y'}, inplace=True)

    # Create dimension tables
    dim_customer = df[['Customer ID', 'Customer Name', 'Segment']].drop_duplicates(subset=['Customer ID']).reset_index(
        drop=True)
    dim_customer['CustomerID_SK'] = dim_customer.index + 1

    dim_location = df[['Postal Code', 'City', 'State', 'Country', 'Region']].drop_duplicates(
        subset=['Postal Code']).reset_index(drop=True)
    dim_location['LocationID_SK'] = dim_location.index + 1

    dim_date = df[['Order Date']].drop_duplicates().reset_index(drop=True)
    dim_date['DateID_SK'] = dim_date.index + 1
    dim_date['Order Year'] = dim_date['Order Date'].dt.year
    dim_date['Order Month'] = dim_date['Order Date'].dt.month
    dim_date['Order Day'] = dim_date['Order Date'].dt.day

    dim_product = df[['Product ID', 'Category', 'Sub-Category', 'Product Name']].drop_duplicates(
        subset=['Product ID']).reset_index(drop=True)
    dim_product['ProductID_SK'] = dim_product.index + 1

    dim_order = df[['Order ID', 'Ship Date', 'Ship Mode']].drop_duplicates(subset=['Order ID']).reset_index(drop=True)
    dim_order['OrderID_SK'] = dim_order.index + 1

    # Create fact table with foreign keys
    fact_df = df[['Product ID', 'Order ID', 'Sales', 'Quantity', 'Discount', 'Profit', 'Order Date', 'Customer ID',
                   'Postal Code']]  # Include join keys
    fact_df['unit_price'] = fact_df['Sales'] / fact_df['Quantity']

    fact_df = pd.merge(fact_df, dim_customer[['Customer ID', 'CustomerID_SK']], on='Customer ID', how='left')
    fact_df = pd.merge(fact_df, dim_location[['Postal Code', 'LocationID_SK']], on='Postal Code', how='left')
    fact_df = pd.merge(fact_df, dim_date[['Order Date', 'DateID_SK']], on='Order Date', how='left')
    fact_df = pd.merge(fact_df, dim_product[['Product ID', 'ProductID_SK']], on='Product ID', how='left')
    fact_df = pd.merge(fact_df, dim_order[['Order ID', 'OrderID_SK']], on='Order ID', how='left')
    
    # Create facts table
    fact_df = fact_df[
        ['OrderID_SK', 'ProductID_SK', 'CustomerID_SK', 'LocationID_SK', 'DateID_SK',
         'Sales', 'Quantity', 'Discount', 'Profit', 'unit_price']]

    return fact_df, dim_product, dim_order, dim_customer, dim_location, dim_date, aggregate_sales_df


def write_data_to_csv(**kwargs):
    ti = kwargs['ti']
    results = ti.xcom_pull(task_ids='transform_data', key='return_value')
    if results:
        fact_df, product_dim, dim_order, dim_customer, dim_location, dim_date, aggregate_sales_df = results

        os.makedirs(OUTPUT_DIR, exist_ok=True)

        fact_file_path = os.path.join(OUTPUT_DIR, 'fact_table.csv')
        product_dim_file_path = os.path.join(OUTPUT_DIR, 'dim_product.csv')
        order_dim_file_path = os.path.join(OUTPUT_DIR, 'dim_order.csv')
        customer_dim_file_path = os.path.join(OUTPUT_DIR, 'dim_customer.csv')
        location_dim_file_path = os.path.join(OUTPUT_DIR, 'dim_location.csv')
        date_dim_file_path = os.path.join(OUTPUT_DIR, 'dim_date.csv')
        aggregate_sales_file_path = os.path.join(OUTPUT_DIR, 'aggregate_sales.csv')

        fact_df.to_csv(fact_file_path, index=False)
        product_dim.to_csv(product_dim_file_path, index=False)
        dim_order.to_csv(order_dim_file_path, index=False)
        dim_customer.to_csv(customer_dim_file_path, index=False)
        dim_location.to_csv(location_dim_file_path, index=False)
        dim_date.to_csv(date_dim_file_path, index=False)
        aggregate_sales_df.to_csv(aggregate_sales_file_path, index=False)

        print("Fact, dimension, and aggregate sales tables saved as CSV in the extract folder.")
    else:
        print("Error: DataFrames not received from transform_data")




def load_data_to_prod_db(**kwargs):
    ti = kwargs['ti']
    mssql_target_conn_id = kwargs['mssql_target_conn_id']
    hook = MsSqlHook(mssql_conn_id=mssql_target_conn_id)

    fact_file_path = os.path.join(OUTPUT_DIR, 'fact_table.csv')
    product_dim_file_path = os.path.join(OUTPUT_DIR, 'dim_product.csv')
    order_dim_file_path = os.path.join(OUTPUT_DIR, 'dim_order.csv')
    customer_dim_file_path = os.path.join(OUTPUT_DIR, 'dim_customer.csv')
    location_dim_file_path = os.path.join(OUTPUT_DIR, 'dim_location.csv')
    date_dim_file_path = os.path.join(OUTPUT_DIR, 'dim_date.csv')
    past_future_sales_file_path = os.path.join(OUTPUT_DIR, 'past_future_sales.csv')
    forecast_metrics_file_path = os.path.join(OUTPUT_DIR, 'forecast_metrics.csv')

    def create_table_if_not_exists(table_name, csv_file_path, hook):
        if not os.path.exists(csv_file_path):
            print(f"CSV file not found: {csv_file_path}. Skipping table creation for {table_name}.")
            return

        try:
            df_example = pd.read_csv(csv_file_path, nrows=1)
            columns_definitions = []
            for col_name, dtype in df_example.dtypes.items():
                sql_type = "VARCHAR(MAX)"  # Default
                if dtype in ['int64', 'int32', 'int16', 'int8']:
                    sql_type = "INT"
                elif dtype in ['float64', 'float32']:
                    sql_type = "FLOAT"
                elif dtype == 'datetime64[ns]':
                    sql_type = "DATETIME"
                elif dtype == 'bool':
                    sql_type = "BIT"
                columns_definitions.append(f"[{col_name}] {sql_type}")

            create_table_sql = f"""
            IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table_name}')
            BEGIN
                CREATE TABLE {table_name} ({", ".join(columns_definitions)});
                PRINT 'Table {table_name} created successfully.';
            END
            """
            print(f"Checking and potentially creating table: {table_name}")
            hook.run(create_table_sql)
            print(f"Table {table_name} exists or was just created.")
        except pd.errors.EmptyDataError:
            print(f"Warning: CSV file {csv_file_path} is empty. Skipping table creation for {table_name}.")
        except Exception as e:
            print(f"Error creating table {table_name}: {e}")

    def load_csv_to_mssql(table_name, csv_file_path, hook, commit_every=1000):
        if os.path.exists(csv_file_path):
            df = pd.read_csv(csv_file_path)
            print(f"Truncating table: {table_name}")
            hook.run(f"TRUNCATE TABLE {table_name}")
            print(f"Loading {len(df)} rows into {table_name}")
            hook.insert_rows(table=table_name, rows=df.to_records(index=False), target_fields=df.columns.tolist(),
                            commit_every=commit_every)
        else:
            print(f"CSV file not found: {csv_file_path}. Skipping load to {table_name}.")

    # Create tables if they don't exist
    create_table_if_not_exists('prod_fact_table', fact_file_path, hook)
    create_table_if_not_exists('prod_dim_product', product_dim_file_path, hook)
    create_table_if_not_exists('prod_dim_order', order_dim_file_path, hook)
    create_table_if_not_exists('prod_dim_customer', customer_dim_file_path, hook)
    create_table_if_not_exists('prod_dim_location', location_dim_file_path, hook)
    create_table_if_not_exists('prod_dim_date', date_dim_file_path, hook)


    # Load data into the tables
    load_csv_to_mssql('prod_fact_table', fact_file_path, hook)
    load_csv_to_mssql('prod_dim_product', product_dim_file_path, hook)
    load_csv_to_mssql('prod_dim_order', order_dim_file_path, hook)
    load_csv_to_mssql('prod_dim_customer', customer_dim_file_path, hook)
    load_csv_to_mssql('prod_dim_location', location_dim_file_path, hook)
    load_csv_to_mssql('prod_dim_date', date_dim_file_path, hook)

with DAG(
    dag_id='superstore_dag',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2024, 1, 1),
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    schedule_interval=None,
    catchup=False,
) as dag:
    extract_data = PythonOperator(
        task_id='extract_superstore_data',
        python_callable=extract_superstore_data,
    )

    compare_records = PythonOperator(
        task_id='compare_and_save_records',
        python_callable=compare_and_save_records,
    )

    transform_data = PythonOperator(
        task_id='transform_data',
        python_callable=transform_superstore_data,
    )

    write_csv = PythonOperator(
        task_id='write_data_to_csv',
        python_callable=write_data_to_csv,
    )

    load_to_prod = PythonOperator(
        task_id='load_data_to_prod_db',
        python_callable=load_data_to_prod_db,
        op_kwargs={'mssql_target_conn_id': MSSQL_TARGET_CONN_ID,
                    'mssql_forecast_conn_id': MSSQL_TARGET_FORECAST_CONN_ID},  # Pass forecast connection
    )

    extract_data >> compare_records >> transform_data >> write_csv >> load_to_prod
