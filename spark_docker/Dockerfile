FROM ubuntu:18.04

ARG SPARK_VERSION=2.4.7
ARG HADOOP_VERSION=2.7

RUN apt update -qq \
 && apt install -y -qq --no-install-recommends wget ipython3 python3-pandas python3-pip openjdk-8-jdk-headless vim curl \
 && rm -rf /var/lib/apt/list/*

 RUN apt-get install -y zip

#FROM openjdk:8-alpine
#RUN apk --update add wget tar bash
RUN wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
 && tar xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -C / \
 && rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
 && ln -s /spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /spark

RUN pip3 install --upgrade pip
RUN pip3 install -U setuptools
RUN pip3 install yfinance
RUN pip3 install pandas_datareader

RUN pip3 install requests

COPY /toazureblob /toazureblob/
RUN zip -r /toazureblob.zip /toazureblob
#COPY toazureblob.zip /toazureblob.zip

COPY start-master.sh /start-master.sh
COPY start-worker.sh /start-worker.sh
COPY start_script.sh /start_script.sh


RUN wget https://repo1.maven.org/maven2/commons-lang/commons-lang/2.6/commons-lang-2.6.jar 

RUN wget https://repo1.maven.org/maven2/org/mortbay/jetty/jetty-util/6.1.26/jetty-util-6.1.26.jar 

RUN wget https://repo1.maven.org/maven2/com/microsoft/azure/azure-storage/2.0.0/azure-storage-2.0.0.jar 

RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure/2.7.3/hadoop-azure-2.7.3.jar 

RUN wget https://repo1.maven.org/maven2/com/microsoft/azure/azure-data-lake-store-sdk/2.1.5/azure-data-lake-store-sdk-2.1.5.jar 

RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure-datalake/3.0.0-alpha3/hadoop-azure-datalake-3.0.0-alpha3.jar 

RUN wget https://raw.githubusercontent.com/dharmeshkakadia/k8s-azure-spark/master/spark-avro_2.11-4.0.0.jar 

RUN wget https://raw.githubusercontent.com/dharmeshkakadia/k8s-azure-spark/master/sqljdbc42.jar

ENV PYSPARK_DRIVER_PYTHON=ipython3 PYSPARK_PYTHON=python3

