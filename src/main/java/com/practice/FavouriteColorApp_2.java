package com.practice;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Properties;

@Slf4j
public class FavouriteColorApp_2 {

    public Topology createTopology() {
        //read as kTable and then write as kTable
        StreamsBuilder builder = new StreamsBuilder();
        KTable<String, String> colorTable = builder.table("favourite-color-input",
                Consumed.with(Serdes.String(), Serdes.String()));

        KTable<String, Long> table = colorTable
                .groupBy((user, color) -> new KeyValue<>(color, color))
                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("counts-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long()));

//                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("counts-store"));

        table.toStream().to("favourite-color-output", Produced.with(Serdes.String(), Serdes.Long()));

        return builder.build();
    }

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "favourite-color-5");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        //it disables the internal cache for state stores
        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        //Solution 2
        Topology topology = new FavouriteColorApp_2().createTopology();
        KafkaStreams kafkaStreams = new KafkaStreams(topology, properties);
        kafkaStreams.cleanUp();
        kafkaStreams.start();
        log.info("topology {}", topology.describe());
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }
}