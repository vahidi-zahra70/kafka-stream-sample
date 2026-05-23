package com.practice;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.practice.dto.BalanceDTO;
import com.practice.dto.TransactionDTO;
import com.practice.util.InstantAdapter;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Properties;

public class BankBalanceApp {
    public static void main(String[] args) {

        Gson gson = new GsonBuilder()
                .setPrettyPrinting()
                .registerTypeAdapter(Instant.class, new InstantAdapter())
                .create();

        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "back-balance-2");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        //we must not use that on production,it disables the internal cache for state stores
        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

        KStreamBuilder builder = new KStreamBuilder();


//        final Serializer<JsonNode> serializer = new JsonSerializer();
//        final Deserializer<JsonNode> deserializer = new JsonDeserializer();
//        final Serde<JsonNode> serde = Serdes.serdeFrom(serializer, deserializer);

        //Solution 2
        //read as kTable and then write as kTable
        KStream<String, String> transactionsStream = builder.stream(Serdes.String(),Serdes.String(),"bank-balance-input");

        KTable<String, String> balance = transactionsStream
                .selectKey((nullKey, value) -> {
                    try {
                        TransactionDTO transactionDTO = gson.fromJson(value, TransactionDTO.class);
                        if (transactionDTO == null) {
                            return null;
                        } else return transactionDTO.getName();
                    } catch (Exception e) {
                        e.printStackTrace();
                        return null;
                    }
                })
                .groupByKey(Serdes.String(), Serdes.String())
                .aggregate(() -> gson.toJson(new BalanceDTO()), (key, value, currentAggBalance) -> {
                            TransactionDTO transactionDTO = gson.fromJson(value, TransactionDTO.class);
                            BalanceDTO balanceDTO = gson.fromJson(currentAggBalance, BalanceDTO.class);
                            balanceDTO.addTransaction(transactionDTO);

                            return gson.toJson(balanceDTO);
                        },
                        Serdes.String(), "balance");

        balance.to(Serdes.String(), Serdes.String(), "bank-balance-output");
        KafkaStreams kafkaStreams = new KafkaStreams(builder, properties);
        kafkaStreams.cleanUp();
        kafkaStreams.start();

        System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>> bank balance " + kafkaStreams);

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }
}