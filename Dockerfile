FROM maven:3.8.1-openjdk-11-slim as build

LABEL "maintainers"="Pol Ajazi; Flavio Amurrio Moya"

WORKDIR /root/kafkaSurveyBuildFolder

COPY . .

RUN mvn compile war:war 

# FROM tomcat:10.0-jdk15
FROM tomcat:10.0.5-jdk15-openjdk-slim-buster

COPY --from=build /root/kafkSurveyBuildFolder/target/SurveyOnKafka.war /usr/local/tomcat/webapps/

# debugging 
RUN apt-get update;
RUN apt-get -y install curl;
