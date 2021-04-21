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
COPY ${JAR_FILE} rebanking-1.0.0.jar
EXPOSE 8080
ENTRYPOINT ["java","-jar","/rebanking-1.0.0.jar"]

#FROM openjdk
#
#
##expose port 8080
#
#
##default command
##CMD java  --add-opens java.base/java.lang=ALL-UNNAMED -jar /rebanking-1.0.0.jar
#
##copy hello world to docker image from builder image
#
#COPY /target/rebanking-1.0.0.jar /rebanking-1.0.0.jar
#ENTRYPOINT ["/entrypoint.sh"]
