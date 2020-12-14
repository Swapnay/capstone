export STORAGE_ACCOUNT=<storage account name>
export STORAGE_ACCOUNT_KEY=<key>
export AZURE_CONTAINER=<container>
SPARK_PRINT_LAUNCH_COMMAND=true /spark/bin/spark-submit --py-files toazureblob.zip --master spark://spark-master:7077 --jars azure-storage-2.0.0.jar,hadoop-azure-2.7.3.jar /toazureblob/upload_main.py
