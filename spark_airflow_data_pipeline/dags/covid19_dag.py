from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

import os

default_args1 = {
    'start_date':datetime(2021,3, 10)
    #'retries': 2,
    #'retry_delay': timedelta(minutes=2)

}

dag = DAG(
    'covid19',
    schedule_interval='0 18 * * *',
    description='A simple DAG',
    default_args=default_args1)
dag1 = DAG(
    'covid191',
    schedule_interval='0 18 * * *',
    description='A simple DAG1',
    default_args=default_args1)

extract_point_covid = os.path.join(os.environ["SPARK_HOME"], "app", "sparktasks","covid","extract.py")
transform_load_point_covid = os.path.join(os.environ["SPARK_HOME"], "app", "sparktasks","covid","transform_load.py")

extract_point_stocks = os.path.join(os.environ["SPARK_HOME"], "app", "sparktasks","stocks","extract.py")
transform_load_point_stocks = os.path.join(os.environ["SPARK_HOME"], "app", "sparktasks","stocks","transform_load.py")

extract_point_unemployment = os.path.join(os.environ["SPARK_HOME"], "app", "sparktasks","unemployment","extract.py")
transform_load_point_unemployment = os.path.join(os.environ["SPARK_HOME"], "app", "sparktasks","unemployment","transform_load.py")

extract_point_housing = os.path.join(os.environ["SPARK_HOME"], "app", "sparktasks","housing","extract.py")
transform_load_housing = os.path.join(os.environ["SPARK_HOME"], "app", "sparktasks","housing","transform_load.py")

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
    dag=dag)

transform_load_data_stocks = SparkSubmitOperator(
    application=transform_load_point_stocks,
    py_files=dependency_path,
    jars=jars,
    name = "Transform Stocks data",
    task_id='Transform_load_stocks_data',
    dag=dag)

extract_unemployment_data = SparkSubmitOperator(
    application=extract_point_unemployment,
    py_files=dependency_path,
    jars=jars,
    name = "Extract unemployment Data",
    task_id='Extract_unemployment_data',
    dag=dag)

transform_load_data_unemployment = SparkSubmitOperator(
    application=transform_load_point_unemployment,
    py_files=dependency_path,
    jars=jars,
    name = "Transform Unemployment data",
    task_id='Transform_load_unemployment_data',
    dag=dag)

extract_housing_data = SparkSubmitOperator(
    application=extract_point_housing,
    py_files=dependency_path,
    jars=jars,
    name = "Extract housing Data",
    task_id='Extract_housing_data',
    dag=dag)

transform_load_data_housing = SparkSubmitOperator(
    application=transform_load_housing,
    py_files=dependency_path,
    jars=jars,
    name = "Transform housing data",
    task_id='Transform_load_housing_data',
    dag=dag)


# transform_load_data_employment = SparkSubmitOperator(
#     application=extract_load_point_employment,
#     py_files=dependency_path,
#     jars=jars,
#     name = "Transform stocks",
#     task_id='extract_load_point_employment',
#     # conf={"spark.master":spark_master,"spark.sql.execution.arrow.enabled":"false",
#     #       "spark.hadoop.fs.azure.account.key.covid19systorage.blob.core.windows.net":"test",
#     #       "spark.hadoop.fs.wasbs.impl":"org.apache.hadoop.fs.azure.NativeAzureFileSystem"},
#     dag=dag)
#
# transform_load_data_housing = SparkSubmitOperator(
#     application=extract_point_housing,
#     py_files=dependency_path,
#     jars=jars,
#     name = "Transform housing",
#     task_id='transform_load_data_housing',
#     conf={"spark.master":spark_master,"spark.sql.execution.arrow.enabled":"false",
#           "spark.hadoop.fs.azure.account.key.covid19systorage.blob.core.windows.net":"test",
#           "spark.hadoop.fs.wasbs.impl":"org.apache.hadoop.fs.azure.NativeAzureFileSystem"},
#     dag=dag)

extract_covid_data>>transform_load_data_covid
transform_load_data_stocks
extract_unemployment_data>>transform_load_data_unemployment

transform_load_data_housing