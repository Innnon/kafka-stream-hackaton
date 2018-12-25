package com.kafkastreams.formatter;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;

/**
 * @author lakshay13@gmail.com
 */
public class AnomalyDetectionStream {

    public static void main(final String[] args) throws Exception {

        final Properties streamsConfiguration = new Properties();
        // streams application is given a unique name.
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "anomaly-detection-lambda-example");
        // Kafka broker(s).
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "10.8.120.20:9094");
        // default serializers/de-serializers for record keys and for record values.
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        // Set the commit interval to 500ms so that any changes are flushed frequently. The low latency
        // would be important for anomaly detection.
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 500);

        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();

        final StreamsBuilder builder = new StreamsBuilder ();

        final KStream<String, String> ktable = builder.stream("topic1");

        // KTable
        // 1. generates a map with key as userName
        // 2. group by key
        // 3. count in an interval of 1 minute
        // 4. filter the ones whose count exceeds 3
        final KTable<Windowed<String>, Long> anomalousUsersTable = ktable
                .map((key, userName) -> new KeyValue<>(userName, userName))
                .groupByKey()
                .windowedBy(TimeWindows.of(60 * 1000L))
                .count()
                .filter((windowUserId, count) -> count >= 3);



        // KStream
        final KStream<String, Long> anomalousUsersStream = anomalousUsersTable
                .toStream()
                .filter((windowedUserId, count) -> count != null)
                .map((windowedUserId, count) -> new KeyValue<>(windowedUserId.toString(), count));


        anomalousUsersStream.to( "topic2");
        final KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamsConfiguration);
        kafkaStreams.start();
    }
}