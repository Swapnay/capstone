

### Build airflow Docker

Inside the airflow-spark/docker/docker-airflow

    $ docker build --rm --force-rm \
    -t docker-airflow-spark:1.10.14_2.4.7 . \
    --build-arg AIRFLOW_VERSION=1.10.14 \
    --build-arg SPARK_VERSION=2.4.7 \
    --build-arg HADOOP_VERSION=2.7

Spark and hadoop versions follow the versions as defined at Spark download page: https://spark.apache.org/downloads.html

Airflow versions can be found here: https://pypi.org/project/apache-airflow/#history

If you change the name or the tag of the docker image when building, remember to update the name/tag in docker-compose file.

### Build Jupyter docker

Inside the airflow-spark/docker/docker-jupyter

    $ docker build --rm --force-rm -t jupyter/pyspark-notebook:3.0.1 .

Optionally, you can override the arguments in the build to choose specific Spark and Hadoop versions. As an example, here is how to build an image containing Spark version `2.4.7` and Hadoop version `2.7`.

    $ docker build --rm --force-rm \
    -t jupyter/pyspark-notebook:2.4.7 . \
    --build-arg spark_version=2.4.7 \
    --build-arg hadoop_version=2.7

Spark and hadoop versions follow the versions as defined at Spark download page: https://spark.apache.org/downloads.html

If you change the name or the tag of the docker image when building, remember to update the name/tag in docker-compose file.
### Start containers

Navigate to airflow-spark/docker and:

    $ docker-compose up

If you want to run in background:

    $ docker-compose up -d

Note: when running the docker-compose for the first time, the images postgres:9.6 and bitnami/spark:3.0.1 will be downloaded before the containers started.

### Check if you can access

Airflow: http://localhost:8282

Spark Master: http://localhost:8181

PostgreSql - Database Test:

* Server: localhost:5432
* Database: test
* User: test
* Password: postgres

Postgres - Database airflow:

* Server: localhost:5432
* Database: airflow
* User: airflow
* Password: airflow

Jupyter Notebook: http://127.0.0.1:8888
  * For Jupyter notebook, you must copy the URL with the token generated when the container is started and paste in your browser. The URL with the token can be taken from container logs using:
  
        $ docker logs -f docker_jupyter-spark_1


## Increasing the number of Spark Workers

You can increase the number of Spark workers just adding new services based on `bitnami/spark:3.0.1` image to the `docker-compose.yml` file like following:

```
spark-worker-n:
        image: bitnami/spark:3.0.1
        networks:
            - default_net
        environment:
            - SPARK_MODE=worker
            - SPARK_MASTER_URL=spark://spark:7077
            - SPARK_WORKER_MEMORY=1G
            - SPARK_WORKER_CORES=1
            - SPARK_RPC_AUTHENTICATION_ENABLED=no
            - SPARK_RPC_ENCRYPTION_ENABLED=no
            - SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED=no
            - SPARK_SSL_ENABLED=no
        volumes:
            - ../spark/app:/usr/local/spark/app # Spark scripts folder (Must be the same path in airflow and Spark Cluster)
            - ../spark/resources/data:/usr/local/spark/resources/data #Data folder (Must be the same path in airflow and Spark Cluster)

```

## Adding Airflow Extra packages

Rebuild Dockerfile (in this example, adding GCP extra):

    $ docker build --rm --build-arg AIRFLOW_DEPS="gcp" -t docker-airflow-spark:1.10.7_3.0.1 .

After successfully built, run docker-compose to start container:

    $ docker-compose up

More info at: https://github.com/puckel/docker-airflow#build

## Useful docker commands

    List Images:
    $ docker images <repository_name>

    List Containers:
    $ docker container ls

    Check container logs:
    $ docker logs -f <container_name>

    To build a Dockerfile after changing sth (run inside directoty containing Dockerfile):
    $ docker build --rm -t <tag_name> .

    Access container bash:
    $ docker exec -i -t <container_name> /bin/bash

## Useful docker-compose commands

    Start Containers:
    $ docker-compose -f <compose-file.yml> up -d

    Stop Containers:
    $ docker-compose -f <compose-file.yml> down --remove-orphans
    

