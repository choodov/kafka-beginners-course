package com.wonderstudy.kafka.streams;

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class StreamsFilterTweets {

    private static Logger logger = LoggerFactory.getLogger(StreamsFilterTweets.class);
    private static JsonParser jsonParser = new JsonParser();

    private static final String USER = "user";
    private static final String FOLLOWERS_COUNT = "followers_count";
    private static final String IMPORTANT_TWEETS_TOPIC = "important_tweets";
    private static final String TWITTER_TWEETS_TOPIC = "twitter_tweets";
    private static final String DEMO_KAFKA_STREAMS = "demo-kafka-streams";
    private static final String DEFAULT_BOOTSTRAP = "127.0.0.1:9092";
    private static final int ZERO_INT = 0;
    private static final int MAX_FOLLOWERS = 100;

    public static void main(String[] args) {
        // create properties
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, DEFAULT_BOOTSTRAP);
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, DEMO_KAFKA_STREAMS);
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        // create a topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // input topic
        KStream<String, String> inputTopic = streamsBuilder.stream(TWITTER_TWEETS_TOPIC);
        // filter for tweets which has a user of over 10000 followers
        KStream<String, String> filteredStream = inputTopic.filter(
                (k, jsonTweet) -> extractUserFollowersInTweets(jsonTweet) > MAX_FOLLOWERS
        );
        filteredStream.to(IMPORTANT_TWEETS_TOPIC);

        // build the  topology
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);

        // start streams app
        kafkaStreams.start();
    }

    private static Integer extractUserFollowersInTweets(final String tweetJson) {
        try {
            return jsonParser.parse(tweetJson)
                    .getAsJsonObject()
                    .get(USER)
                    .getAsJsonObject()
                    .get(FOLLOWERS_COUNT)
                    .getAsInt();
        } catch (NullPointerException e) {
            return ZERO_INT;
        }
    }
}
