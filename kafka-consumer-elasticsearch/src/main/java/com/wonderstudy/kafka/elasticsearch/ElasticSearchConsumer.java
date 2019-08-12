package com.wonderstudy.kafka.elasticsearch;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ElasticSearchConsumer {

    private static Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class);
    private static JsonParser jsonParser = new JsonParser();

    private static final String DEFAULT_BOOTSTRAP_SERVER = "127.0.0.1:9092";
    private static final String GROUP_ID = "kafka_elasticsearch";
    private static final String OFFSET_CONFIG = "earliest"; // earliest (from start of topic)/ latest (new messages) / none
    private static final String TOPIC_NAME = "twitter_tweets";
    private static final String ID_STR = "id_str";

    public static void main(String[] args) throws IOException {
        RestHighLevelClient client = createClient();

        // use kafka consumer
        KafkaConsumer<String, String> consumer = createConsumer();

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            logger.info("Received " + records.count() + "records");
            for (ConsumerRecord<String, String> record : records) {

                // 2 strategies for idempotent id generation:
                // 1) kafka generic ID
                // String id = record.topic() + "_" + record.partition() + "_" + record.offset();
                // 2) twitter feed specific id
                String id = extractIdFromTweet(record.value());

                // insert data to ES
                IndexRequest indexRequest = new IndexRequest("twitter", "tweets", id) // id - make consumer idempotent
                        .source(record.value(), XContentType.JSON);

                IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
                logger.info("Id: " + indexResponse.getId());

                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                logger.info("Committing offsets");
                consumer.commitAsync();
                logger.info("Offsets have been committed");
            }
        }
//        client.close();

        // To see json in ES: GET /twitter/tweets/<id>
    }

    private static String extractIdFromTweet(final String tweetJson) {
        return jsonParser.parse(tweetJson).getAsJsonObject().get(ID_STR).getAsString();
    }

    private static RestHighLevelClient createClient() {
        // TODO: add your ES credentials
        String hostmane = "";
        String username = "";
        String password = "";

        // don't do if you run local ES
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

        RestClientBuilder builder = RestClient.builder(new HttpHost(hostmane, 443, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                        return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });

        return new RestHighLevelClient(builder);
    }

    private static KafkaConsumer<String, String> createConsumer() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, DEFAULT_BOOTSTRAP_SERVER);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OFFSET_CONFIG);
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // disable auto commit of offsets
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singleton(TOPIC_NAME));
        return consumer;
    }
}
