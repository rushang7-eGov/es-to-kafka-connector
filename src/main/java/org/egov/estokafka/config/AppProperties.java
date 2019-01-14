package org.egov.estokafka.config;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Properties;

@Slf4j
@Getter
public class AppProperties {

    private Integer batchSize;

    private Integer scrollTime;

    private String esURL;

    private String kafkaBootstrapServerConfig;

    private String sourceIndex;

    private String destinationTopic;

    public AppProperties() {

        if(System.getenv("BATCH_SIZE") != null)
            batchSize = Integer.parseInt(System.getenv("BATCH_SIZE"));


        if(System.getenv("SCROLL_TIME") != null)
            scrollTime = Integer.parseInt(System.getenv("SCROLL_TIME"));

        kafkaBootstrapServerConfig = System.getenv("KAFKA_BOOTSTRAP_SERVER_CONFIG");

        esURL = System.getenv("ES_URL");

        sourceIndex = System.getenv("ES_SOURCE_INDEX");

        destinationTopic = System.getenv("KAFKA_DESTINATION_TOPIC");

        Properties properties = new Properties();

        try {
            properties.load(getClass().getClassLoader().getResourceAsStream("application.properties"));
        } catch (IOException e) {
            log.error("Error reading application.properties");
        }

        if(batchSize == null)
            batchSize = Integer.parseInt(properties.getProperty("BATCH_SIZE"));

        if(scrollTime == null)
            scrollTime = Integer.parseInt(properties.getProperty("SCROLL_TIME"));

        if (kafkaBootstrapServerConfig == null)
            kafkaBootstrapServerConfig = properties.getProperty("KAFKA_BOOTSTRAP_SERVER_CONFIG");

        if (esURL == null)
            esURL = properties.getProperty("ES_URL");

        if(sourceIndex == null)
            sourceIndex = properties.getProperty("ES_SOURCE_INDEX");

        if(destinationTopic == null)
            destinationTopic = properties.getProperty("KAFKA_DESTINATION_TOPIC");

    }

}
