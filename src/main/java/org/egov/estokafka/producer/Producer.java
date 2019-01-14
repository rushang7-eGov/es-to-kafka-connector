package org.egov.estokafka.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.egov.estokafka.config.AppProperties;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.Properties;

@Slf4j
public class Producer {

    private KafkaProducer kafkaProducer;

    private AppProperties appProperties;

    public Producer() {
        appProperties = new AppProperties();

        Properties configProperties = new Properties();
        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,appProperties.getKafkaBootstrapServerConfig());

        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");

        kafkaProducer = new KafkaProducer(configProperties);
    }

    public void push(String topic, JSONObject jsonObject) {
        ProducerRecord<String, String> producerRecord = new ProducerRecord(topic, jsonObject.toString());
        kafkaProducer.send(producerRecord);
    }

    public void pushBulk(String topic, JSONArray jsonArray) {
        for(Object jsonObject : jsonArray)  {
            push(topic, (JSONObject) jsonObject);
        }
    }

}
