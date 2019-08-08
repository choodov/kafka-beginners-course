package kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoAssingSeek {

    private static final String DEFAULT_BOOTSTRAP_SERVER = "127.0.0.1:9092";
    private static final String OFFSET_CONFIG = "earliest"; // earliest (from start of topic)/ latest (new messages) / none
    private static final String FIRST_TOPIC_NAME = "first_topic";
    private static final int PARTITION_NUMBER_ZERO = 0;
    private static final long OFFSET_TO_READ_FROM = 2L;
    private static final String EXITING_THE_APP = "Exiting the app";
    private static final String LOG_VALUE = "Value: ";
    private static final String LOG_PARTITION = "Partition: ";
    private static final String LOG_OFFSET = "Offset: ";

    // RUN SEVERAL TIMES to see work of consumers in a group
    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ConsumerDemoAssingSeek.class);

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, DEFAULT_BOOTSTRAP_SERVER);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OFFSET_CONFIG);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // assign and seek are mostly used to replay data
        // or fetch a specific message

        // assign
        TopicPartition partitionToReadFrom = new TopicPartition(FIRST_TOPIC_NAME, PARTITION_NUMBER_ZERO);
        consumer.assign(Collections.singletonList(partitionToReadFrom));

        // seek
        consumer.seek(partitionToReadFrom, OFFSET_TO_READ_FROM);

        int numberOfMessagesToRead = 5;
        int numberOfMessagesReadSoFar = 0;
        boolean keepOnReading = true;

        while (keepOnReading) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                numberOfMessagesReadSoFar += 1;
                logger.info(LOG_VALUE + record.value());
                logger.info(LOG_PARTITION + record.partition());
                logger.info(LOG_OFFSET + record.offset());
                if (numberOfMessagesReadSoFar >= numberOfMessagesToRead) {
                    keepOnReading = false;
                    break;
                }
            }
        }

        logger.info(EXITING_THE_APP);
    }
}
