# Spark Cluster

This is a project can be used to run a basic flask app with MySQL as DB using docker-compose.

## Getting Started

**Step 1:** Make sure git is installed on your os. I will be using macOS for the project.

On macOS, you can install the git using Homebrew using ```brew install git```


### Prerequisites

**1. Docker**

Make sure you have Docker installed. Please follow the below link for official documentation from Docker to install latest version of docker on your os. For this project I am using Docker CE (18.09).

```https://docs.docker.com/docker-for-mac/install/```

### Installing

**Step 1:** Change to the directory where the project was cloned in previous step.

```
cd capstone
```

**Step 2:** Make sure Docker is up and running. You can start the docker engine from desktop icon on Mac.

**Step 3:** Cluster Operation
1. Build and run

1.Start the cluster: ```docker-compose up -d```
2.(Optional) Visit ```http://localhost:8080 ``` in the browser to see the WebUI
3.(Optional) Watch cluster logs: ```docker-compose logs -f```
4.(Optional) Add more workers (e.g. up to 3): ```docker-compose up -d --scale spark-worker=3```
5.(Optional) Watch cluster resource usage in real time: ```docker stats```
6.Shutdown the cluster and clean up: ```docker-compose down```

**Step 4:** Run
```docker run --rm -it --network azurespark_spark-network swapna/spark:latest /bin/sh```
 Change start_script to reflect storage account name, key and container name.
 ```./start_script ```
## Running the tests

## Deployment

## Built With

* [Docker](http://www.dropwizard.io/1.0.2/docs/) -  Deployment model
* [Python](https://rometools.github.io/rome/) - programming language
* [pip](https://rometools.github.io/rome/) - Package and dependency manager
*[spark](https://archive.apache.org/dist/spark/spark-2.4.7/spark-2.4.7-bin-hadoop2.7.tgz )


## Contributing

## Versioning

## Authors

## License

## Acknowledgments
