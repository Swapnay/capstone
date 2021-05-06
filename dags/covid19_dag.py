from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

import os

default_args1 = {
    'start_date':datetime(2021,4, 29)
    # 'retries': 2,
    # 'retry_delay': timedelta(minutes=10)

}

dag = DAG(
    'covid19',
    schedule_interval='0 18 * * *',
    description='A simple DAG',
    default_args=default_args1)
dag_stocks = DAG(
    'stocks',
    schedule_interval='0 18 * * *',
    description='A Stocks DAG',
    default_args=default_args1)

dag_monthly = DAG(
    'unemployment_monthly',
    schedule_interval='0 4 1 * *',
    description='Monthly dag',
    default_args=default_args1)
dag_housing = DAG(
    'Housing_monthly',
    schedule_interval='0 4 1 * *',
    description='Housing Monthly dag',
    default_args=default_args1)

start_daily = DummyOperator(task_id="start", dag=dag)
stop_daily = DummyOperator(task_id="stop", dag=dag)
start_monthly = DummyOperator(task_id="start", dag=dag_monthly)
stop_monthly = DummyOperator(task_id="stop", dag=dag_monthly)
start_stocks = DummyOperator(task_id="start_stocks", dag=dag_stocks)
stop_stocks = DummyOperator(task_id="stop_stocksw", dag=dag_stocks)
start_housing = DummyOperator(task_id="start_housing", dag=dag_housing)
stop_housing = DummyOperator(task_id="stop_housing", dag=dag_housing)

extract_point_covid = os.path.join(os.environ["SPARK_HOME"], "app", "sparktasks","covid","extract.py")
transform_load_point_covid = os.path.join(os.environ["SPARK_HOME"], "app", "sparktasks","covid","transform_load.py")
analytics_covid = os.path.join(os.environ["SPARK_HOME"], "app", "sparktasks","covid","analyticsetl.py")

extract_point_stocks = os.path.join(os.environ["SPARK_HOME"], "app", "sparktasks","stocks","extract.py")
transform_load_point_stocks = os.path.join(os.environ["SPARK_HOME"], "app", "sparktasks","stocks","transform_load.py")
analytics_stocks = os.path.join(os.environ["SPARK_HOME"], "app", "sparktasks","stocks","analyticsetl.py")

unemployment =os.path.join(os.environ["SPARK_HOME"], "app", "sparktasks","unemployment")
extract_point_unemployment = os.path.join(unemployment,"extract.py")
transform_load_point_unemployment = os.path.join(unemployment,"transform_load.py")
analytics_unemployment = os.path.join(unemployment,"analyticsetl.py")

extract_point_housing = os.path.join(os.environ["SPARK_HOME"], "app", "sparktasks","housing","extract.py")
transform_load_housing = os.path.join(os.environ["SPARK_HOME"], "app", "sparktasks","housing","transform_load.py")
analytics_housing = os.path.join(os.environ["SPARK_HOME"], "app", "sparktasks","housing","analyticsetl.py")

dependency_path = os.path.join(os.environ["SPARK_HOME"], "app","sparktasks.zip")
jars = os.path.join(os.environ["SPARK_HOME"], "assembly","target", "scala-2.12",
                    "jars","mysql-connector-java-8.0.22.jar")
spark_master ="spark://spark:7077"

extract_covid_data = SparkSubmitOperator(
    application=extract_point_covid,
    py_files=dependency_path,
    jars=jars,
    name = "Extract",
    task_id='Extract_covid_data',
    dag=dag)

transform_load_data_covid = SparkSubmitOperator(
    application=transform_load_point_covid,
    py_files=dependency_path,
    jars=jars,
    name = "Transform Covid",
    task_id='Transform_load_covid_data',
    dag=dag)

extract_stock_data = SparkSubmitOperator(
    application=extract_point_stocks,
    py_files=dependency_path,
    jars=jars,
    name = "Extract Stocks Data",
    task_id='Extract_stocks_data',
    dag=dag_stocks)

transform_load_data_stocks = SparkSubmitOperator(
    application=transform_load_point_stocks,
    py_files=dependency_path,
    jars=jars,
    name = "Transform Stocks data",
    task_id='Transform_load_stocks_data',
    dag=dag_stocks)

extract_unemployment_data = SparkSubmitOperator(
    application=extract_point_unemployment,
    py_files=dependency_path,
    jars=jars,
    name = "Extract unemployment Data",
    task_id='Extract_unemployment_data',
    dag=dag_monthly)

transform_load_data_unemployment = SparkSubmitOperator(
    application=transform_load_point_unemployment,
    py_files=dependency_path,
    jars=jars,
    name = "Transform Unemployment data",
    task_id='Transform_load_unemployment_data',
    dag=dag_monthly)

extract_housing_data = SparkSubmitOperator(
    application=extract_point_housing,
    py_files=dependency_path,
    jars=jars,
    name = "Extract housing Data",
    task_id='Extract_housing_data',
    dag=dag_housing)

transform_load_data_housing = SparkSubmitOperator(
    application=transform_load_housing,
    py_files=dependency_path,
    jars=jars,
    name = "Transform housing data",
    task_id='Transform_load_housing_data',
    dag=dag_housing)

covid_analytics_tables = SparkSubmitOperator(
    application=analytics_covid,
    py_files=dependency_path,
    jars=jars,
    name = "Analytics Covid",
    task_id='Covid_analytics_tables',
    dag=dag)

stock_analytics_tables = SparkSubmitOperator(
    application=analytics_stocks,
    py_files=dependency_path,
    jars=jars,
    name = "Analytics Stocks",
    task_id='Stock_analytics_tables',
    dag=dag_stocks)

employment_analytics_tables = SparkSubmitOperator(
    application=analytics_unemployment,
    py_files=dependency_path,
    jars=jars,
    name = "Analytics unemployment",
    task_id='employment_analytics_tables',
    dag=dag_monthly)

housing_analytics_tables = SparkSubmitOperator(
    application=analytics_housing,
    py_files=dependency_path,
    jars=jars,
    name = "Analytics housing",
    task_id='housing_analytics_tables',
    dag=dag_housing)

start_daily>>extract_covid_data>>transform_load_data_covid>>covid_analytics_tables>>stop_daily
start_stocks>>extract_stock_data>>transform_load_data_stocks>>stock_analytics_tables>>stop_stocks
start_monthly>>extract_unemployment_data>>transform_load_data_unemployment>>employment_analytics_tables>>stop_monthly
start_housing>>extract_housing_data>>transform_load_data_housing>>housing_analytics_tables>>stop_housing


