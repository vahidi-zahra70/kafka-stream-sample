package com.practice;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.util.Arrays;
import java.util.Properties;

@Slf4j
public class WordCountApp {

    public Topology createTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> textLines = builder.stream("word-count-input");
        KTable<String, Long> wordCounts = textLines
                .filter(((key, value) -> value != null))
                .flatMapValues(value -> Arrays.asList(value.toLowerCase().split(" ")))
                .selectKey((key, word) -> word)
                .groupByKey()
                .count(Materialized.as("counts"));

        // writes the running counts as a changelog stream to the output topic.
        wordCounts.toStream().to("word-count-output", Produced.with(Serdes.String(), Serdes.Long()));

        return builder.build();
    }

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-stream-practice");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        WordCountApp wordCountApp = new WordCountApp();
        Topology topology = wordCountApp.createTopology();
        KafkaStreams kafkaStreams = new KafkaStreams(topology, properties);
        kafkaStreams.start();
        log.info("topology {}", topology.describe());

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }
}