package com.kafkastreams.formatter;

import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

@Service
public class AnomalyDetectionApp {

    @Value("${application.id:anomaly-detection-app2}")
    private String applicationId;

    @Value("${kafka.bootstrap.server:127.0.0.1:9092}")
    private String bootstrapServer;

    @Value("${input.stream.topic:host-meter-topic5}")
    private String inputTopic;

    @Value("${output.stream.topic:output-topic}")
    private String outputTopic;

    public void start(Double anomalyValue, Integer threshold) {
        final Properties streamsConfiguration = new Properties();

        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> hostMeterStream =
                builder.stream(inputTopic);
        hostMeterStream
                //remove bad inputs
                .filter(((key, value) -> value.matches("-?\\d+(\\.\\d+)?")))
                //filter keys with anomaly value
                .filter((key, value) -> Double.parseDouble(value) > anomalyValue)
                //group by host
                .groupByKey()
                //split by time window
                .windowedBy(TimeWindows.of(TimeUnit.MINUTES.toMillis(1))
                        .advanceBy(TimeUnit.MINUTES.toMillis(1)))
                ///count anomaly per time window
                .count()
                .toStream()
                //raise alarm = 1, no alarm = 0
                .map((windowed, count) ->
                        new KeyValue<>(windowed.toString(), count > threshold ? 1 : 0))

                //publish the data
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.Integer()));

        KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);

        streams.start();

        // print the topology
        System.out.println(streams.toString());

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
