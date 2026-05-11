package com.practice;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Arrays;
import java.util.Properties;

public class StreamStarterApp {
    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-stream-practice");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());


        KStreamBuilder builder = new KStreamBuilder();
        KStream<String, String> textLines = builder.stream("word-count-input");


        KTable<String, Long> wordCounts = textLines
                // Split each text line, by whitespace, into words.
                .flatMapValues(value -> Arrays.asList(value.toLowerCase().split(" ")))
                .selectKey((key, word) -> word)
                // Group the text words as message keys
                .groupByKey()
                // Count the occurrences of each word (message key).
                .count("counts");

        // writes the running counts as a changelog stream to the output topic.
        wordCounts.to(Serdes.String(), Serdes.Long(), "word-count-output");

        KafkaStreams kafkaStreams=new KafkaStreams(builder,properties);
        kafkaStreams.start();
        System.out.println(kafkaStreams);

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }
}