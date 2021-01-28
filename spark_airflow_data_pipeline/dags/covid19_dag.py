from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
import os

default_args1 = {
    'start_date':datetime(2021,1, 10),
    'retries': 2,
    'retry_delay': timedelta(minutes=2)

}

dag = DAG(
    'covid19',
    schedule_interval='0 18 * * *',
    description='A simple DAG',
    default_args=default_args1)

extract_point = os.path.join(os.environ["SPARK_HOME"], "app", "sparktasks","covid","covid19_extract_load.py")
dependency_path = os.path.join(os.environ["SPARK_HOME"], "app","sparktasks.zip")+","+ os.path.join(os.environ["SPARK_HOME"],"resources","data","United_States_COVID-19_Cases_and_Deaths_by_State_over_Time.csv")
jars = os.path.join(os.environ["SPARK_HOME"], "assembly","target", "scala-2.12",
                    "jars","mysql-connector-java-8.0.22.jar")
spark_master ="spark://spark:7077"
extract_data = SparkSubmitOperator(
    application=extract_point,
    py_files=dependency_path,
    jars=jars,
    name = "Extract",
    task_id='extract_data',
    conn_id='spark_default',
    conf={"spark.master":spark_master,"spark.sql.execution.arrow.enabled":"false",
          "spark.hadoop.fs.azure.account.key.covid19systorage.blob.core.windows.net":"test",
          "spark.hadoop.fs.wasbs.impl":"org.apache.hadoop.fs.azure.NativeAzureFileSystem"},
    dag=dag)


extract_data

