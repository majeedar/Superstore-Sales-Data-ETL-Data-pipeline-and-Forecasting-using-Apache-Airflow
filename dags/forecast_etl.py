import pandas as pd
from prophet import Prophet
import numpy as np
from sklearn.metrics import mean_squared_error
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
import io
import json

OUTPUT_DIR = '/opt/airflow/logs/extracted_data'
MSSQL_TARGET_FORECAST_CONN_ID = "mssql_default"

def forecast_aggregate_sales(**kwargs):
    ti = kwargs['ti']
    aggregate_sales_df = pd.read_csv(os.path.join(OUTPUT_DIR, 'aggregate_sales.csv'))

    aggregate_sales_df['ds'] = pd.to_datetime(aggregate_sales_df['ds'])

    model = Prophet()
    model.fit(aggregate_sales_df)

    future = model.make_future_dataframe(periods=365)
    forecast = model.predict(future)

    forecast_df = forecast[['ds', 'yhat']].rename(columns={'ds': 'Order Date', 'yhat': 'Aggregate Sales'})

    forecast_df['Order Date'] = pd.to_datetime(forecast_df['Order Date']).dt.date

    aggregate_sales_df_new = aggregate_sales_df.rename(columns={'ds': 'Order Date', 'y': 'Aggregate Sales'})

    last_past_date = aggregate_sales_df_new['Order Date'].max().date()

    future_df = forecast_df[forecast_df['Order Date'] > last_past_date].copy()


    past_future_sales = pd.concat([
        aggregate_sales_df_new.assign(label='past'),
        future_df.assign(label='future')
    ])

    # Change 'Order Date' to YYYY-MM-DD format
    past_future_sales['Order Date'] = pd.to_datetime(past_future_sales['Order Date']).dt.strftime('%Y-%m-%d')

    past_predictions = forecast.iloc[:len(aggregate_sales_df)]['yhat']
    actual_sales = aggregate_sales_df['y'].values
    rmse = np.sqrt(mean_squared_error(actual_sales, past_predictions))

    forecast_metrics_df = pd.DataFrame([{'Metric': 'RMSE', 'Value': rmse}])

    past_future_sales.to_csv(os.path.join(OUTPUT_DIR, 'past_future_sales.csv'), index=False)
    forecast_metrics_df.to_csv(os.path.join(OUTPUT_DIR, 'forecast_metrics.csv'), index=False)

    ti.xcom_push(key='past_future_sales_csv', value=past_future_sales.to_csv(index=False))
    ti.xcom_push(key='forecast_metrics', value=forecast_metrics_df.to_json(orient='records'))

    print("Aggregate sales forecast completed and results saved.")

def load_data_to_prod_db(**kwargs):
    ti = kwargs['ti']
    hook = MsSqlHook(mssql_conn_id=MSSQL_TARGET_FORECAST_CONN_ID)

    past_future_sales_csv = ti.xcom_pull(task_ids='forecast_aggregate_sales', key='past_future_sales_csv')
    forecast_metrics_json = ti.xcom_pull(task_ids='forecast_aggregate_sales', key='forecast_metrics')

    past_future_sales_df = pd.read_csv(io.StringIO(past_future_sales_csv))

    # Ensure 'Order Date' is in proper datetime format
    past_future_sales_df["Order Date"] = pd.to_datetime(past_future_sales_df["Order Date"], errors='coerce')

    past_future_sales_df['label'] = past_future_sales_df['label'].astype(str)
    past_future_sales_df['Aggregate Sales'] = past_future_sales_df['Aggregate Sales'].astype(float).round(2)
    past_future_sales_df['Aggregate Sales'] = pd.to_numeric(past_future_sales_df['Aggregate Sales'], errors='coerce')


    forecast_metrics_df = pd.read_json(forecast_metrics_json, orient='records')
    # Change column formats:
    forecast_metrics_df['Metric'] = forecast_metrics_df['Metric'].astype(str)
    forecast_metrics_df['Value'] = forecast_metrics_df['Value'].astype(float)
    print(past_future_sales_df.head(10))
    print(past_future_sales_df.tail(10))
    print(past_future_sales_df.dtypes)
    print(past_future_sales_df.head())
    print(past_future_sales_df['Aggregate Sales'].apply(lambda x: isinstance(x, (int, float))).all())
    print(past_future_sales_df[past_future_sales_df['Aggregate Sales'].apply(lambda x: not isinstance(x, (int, float)))])
    print(past_future_sales_df['Aggregate Sales'].isnull().values.any())
    print(past_future_sales_df['Aggregate Sales'].isnull().sum())

    print(past_future_sales_df['Aggregate Sales'].isin([float('inf'), float('-inf')]).values.any())

    print(past_future_sales_df.dtypes)


    def create_table_if_not_exists(table_name, df_example, hook):
        try:
            columns_definitions = []
            for col_name, dtype in df_example.dtypes.items():
                sql_type = "VARCHAR(MAX)"
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
        except Exception as e:
            print(f"Error creating table {table_name}: {e}")

    def load_df_to_mssql(table_name, df, hook, commit_every=1000):
        print(f"Truncating table: {table_name}")
        hook.run(f"TRUNCATE TABLE {table_name}")
        print(f"Loading {len(df)} rows into {table_name}")

        # Explicitly format the 'Order Date' column for SQL Server
        df_copy = df.copy()
        if 'Order Date' in df_copy.columns:
            df_copy['Order Date'] = df_copy['Order Date'].dt.strftime('%Y-%m-%d %H:%M:%S') # Include time

        hook.insert_rows(table=table_name, rows=df_copy.to_records(index=False), target_fields=df_copy.columns.tolist(),
                            commit_every=commit_every)

    create_table_if_not_exists('forecast_past_future_sales', past_future_sales_df, hook)
    create_table_if_not_exists('forecast_metrics', forecast_metrics_df, hook)

    load_df_to_mssql('forecast_past_future_sales', past_future_sales_df, hook)
    load_df_to_mssql('forecast_metrics', forecast_metrics_df, hook)

    print("Forecast data loaded into production database.")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

dag = DAG(
    dag_id='forecast_sales_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)

forecast_task = PythonOperator(
    task_id='forecast_aggregate_sales',
    python_callable=forecast_aggregate_sales,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data_to_prod_db',
    python_callable=load_data_to_prod_db,
    op_kwargs={'mssql_target_conn_id': MSSQL_TARGET_FORECAST_CONN_ID},
    dag=dag,
)

forecast_task >> load_task