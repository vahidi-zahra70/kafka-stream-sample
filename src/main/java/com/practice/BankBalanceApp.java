package com.practice;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSerializer;
import com.practice.dto.BalanceDTO;
import com.practice.dto.TransactionDTO;
import com.practice.util.InstantAdapter;
import com.practice.util.Util;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.kafka.support.serializer.JsonSerde;


import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Properties;

@Slf4j
public class BankBalanceApp {

    public Topology createTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        Serde<TransactionDTO> transactionSerde =
                new JsonSerde<>(TransactionDTO.class,Util.OBJECT_MAPPER);

        Serde<BalanceDTO> balanceSerde =
                new JsonSerde<>(BalanceDTO.class, Util.OBJECT_MAPPER);

        //read as KStream and then write as kTable
        KStream<String, TransactionDTO> transactionsStream = builder.stream("bank-balance-input", Consumed.with(Serdes.String(), transactionSerde));

        KTable<String, BalanceDTO> balance = transactionsStream
                .selectKey((nullKey, transactionDTO) -> transactionDTO.getName())
                .groupByKey(Grouped.with(Serdes.String(), transactionSerde))
                .aggregate(BalanceDTO::new, (key, value, currentAggBalance) -> {
                            currentAggBalance.addTransaction(value);

                            return currentAggBalance;
                        },
                        Materialized.<String, BalanceDTO, KeyValueStore<Bytes, byte[]>>as("balance")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(balanceSerde));

        balance.toStream().to("bank-balance-output", Produced.with(Serdes.String(), balanceSerde));

        return builder.build();
    }

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "back-balance-2");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        //we must not use that on production,it disables the internal cache for state stores
        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);


        BankBalanceApp bankBalanceApp = new BankBalanceApp();
        Topology topology = bankBalanceApp.createTopology();
        KafkaStreams kafkaStreams = new KafkaStreams(topology, properties);
        kafkaStreams.cleanUp();
        kafkaStreams.start();
        log.info("topology {}", topology.describe());

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }


//        final Serializer<JsonNode> serializer = new JsonSerializer();
//        final Deserializer<JsonNode> deserializer = new JsonDeserializer();
//        final Serde<JsonNode> serde = Serdes.serdeFrom(serializer, deserializer);

}