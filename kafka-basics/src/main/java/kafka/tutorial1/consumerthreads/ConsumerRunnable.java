package kafka.tutorial1.consumerthreads;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerRunnable implements Runnable {

    public static final String LOG_KEY = "Key: ";
    public static final String LOG_VALUE = "Value: ";
    public static final String LOG_PARTITION = "Partition: ";
    public static final String LOG_OFFSET = "Offset: ";
    Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);

    private static final String MESSAGE_RECEIVED_SHUTDOWN_SIGNAL = "Received shutdown signal";
    private static final String OFFSET_CONFIG = "earliest"; // earliest (from start of topic)/ latest (new messages) / none

    private final CountDownLatch latch;
    private final KafkaConsumer<String, String> consumer;

    public ConsumerRunnable(String topic, String bootsrapServer, String groupId, CountDownLatch latch) {
        consumer = new KafkaConsumer<>(getKafkaProperties(bootsrapServer, groupId));
        consumer.subscribe(Collections.singleton(topic));
        this.latch = latch;
    }

    private Properties getKafkaProperties(String bootsrapServer, String groupId) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootsrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OFFSET_CONFIG);
        return properties;
    }

    @Override
    public void run() {
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    logger.info(LOG_KEY + record.key());
                    logger.info(LOG_VALUE + record.value());
                    logger.info(LOG_PARTITION + record.partition());
                    logger.info(LOG_OFFSET + record.offset());
                }
            }
        } catch (WakeupException e) {
            logger.info(MESSAGE_RECEIVED_SHUTDOWN_SIGNAL);
        } finally {
            consumer.close();
            latch.countDown();
        }
    }

    public void shutdown() {
        // to interrupt consumer.poll() by throw WakeupException
        consumer.wakeup();
    }
}
