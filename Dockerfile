FROM egovio/apline-jre:8u121
MAINTAINER Sivaprakash Ramasamy<sivaprakash.ramasamy@tarento.com>
 # INSTRUCTIONS ON HOW TO BUILD JAR:
# Move to the location where pom.xml is exist in project and build project using below command
# "mvn clean package"
COPY target/egov-es-to-kafka-connector-1.0-SNAPSHOT-jar-with-dependencies.jar /opt/egov/egov-es-to-kafka-connector.jar
COPY start.sh /usr/bin/start.sh
RUN chmod +x /usr/bin/start.sh
CMD ["/usr/bin/start.sh"]
