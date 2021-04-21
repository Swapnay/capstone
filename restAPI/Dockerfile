# Maven build container 

#FROM maven:3.6.3-openjdk-11 AS maven_build
#
#COPY pom.xml /tmp/
#
#COPY src /tmp/src/
#
#WORKDIR /tmp/
#
#RUN mvn clean package -DskipTests

#pull base image

FROM openjdk:8-jdk-alpine
RUN addgroup -S spring && adduser -S spring -G spring
USER spring:spring
ARG JAR_FILE=target/*.jar
COPY ${JAR_FILE} covid-impact-data-1.0.0.jar
EXPOSE 8080
ENTRYPOINT ["java","-jar","/covid-impact-data-1.0.0.jar"]

