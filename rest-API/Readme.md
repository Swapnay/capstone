# Java, Maven, Spring Boot, Spring Security, MySQL, JPA, Hibernate Rest API

REST API built on top of Spring Boot + Security + MySql (Azure Mysql)

## Requirements

1. Java 8

2. Maven 

3. Spring Boot

4. Mysql 

## Steps to Setup

**1. cd RestAPI
```

**2. Create Mysql (OR AZUuresql) database, set username and password**


+ open `src/main/resources/application.properties`

+ change `spring.datasource.username` and `spring.datasource.password` as per your mysql installation

**3. Run the app using maven ( if not using docker  skip this step and go to 4)**

```bash
mvn spring-boot:run
```

**4 Create Jar using **
```bash
mvn clean package -DskipTests
```
** 5 Build Docker image **
```
docker build -t covid-impact-data:latest .
```
** 6. Run using docker-compose ( If running standalone) **
```
docker-compose up
```

The app will start running at <http://localhost:8080>.

## Lok at Swagger API - http://localhost:8080/swagger-ui.html

 
+ open `SecurityJavaConfig`and you can find 2 roles and credentials



+ API  implementation and validation of parameters.
+ Error handling to build the response (code + message).
+ Supports Security for /api requests requests
+ Swagger integration
+ spring-boot integration tests




