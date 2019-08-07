package com.chudov.examples.kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {

    private static Logger log = LoggerFactory.getLogger(ProducerDemoKeys.class);

    private static final String DEFAULT_BOOTSTRAP_SERVER = "127.0.0.1:9092";
    public static final String FIRST_TOPIC_NAME = "first_topic";

    public static void main(String[] args) {


        // create producer's properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, DEFAULT_BOOTSTRAP_SERVER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i <10; i++) {
            // create a producer record
            String value = "new value " + i;
            // message with same key always goes to same partition!
            String key = "id_" + i;

            ProducerRecord<String, String> record =
                    new ProducerRecord<>(FIRST_TOPIC_NAME, key, value);

            // send data - asynchronous. With CALLBACK
            producer.send(record, ProducerDemoKeys::checkCallback);

            // flush data
            producer.flush();
        }
        // flush and close producer
        producer.close();
    }

    private static void checkCallback(RecordMetadata recordMetadata, Exception ex) {
        // executes every time a record is successfully sent or an exception is thrown
        if (ex == null) {
            log.info("Received new metadata: ");
            log.info("Topic: " + recordMetadata.topic());
            log.info("Partition: " + recordMetadata.partition());
            log.info("Offset: " + recordMetadata.offset());
            log.info("Timestamp: " + recordMetadata.timestamp());
        } else {
            log.error("Error while producing: " + ex);
        }
    }
}
