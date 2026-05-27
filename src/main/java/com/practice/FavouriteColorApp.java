//package com.practice;
//
//import org.apache.kafka.common.serialization.Serdes;
//import org.apache.kafka.streams.*;
//import org.apache.kafka.streams.kstream.*;
//
//import java.util.Arrays;
//import java.util.Properties;
//public class FavouriteColorApp {
//    public static void main(String[] args) {
//
//        Properties properties = new Properties();
//        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "favourite-color-3");
//        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
//        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
//        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
//        //it disables the internal cache for state stores
//        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
//
//        KStreamBuilder builder = new KStreamBuilder();
//
//        //Solution 1
//        //Read as KStream and then write to an intermediate topic and then read from that as kTable
////        KStream<String, String> inputLines = builder.stream("favourite-color-input");
////
////        inputLines
////                .mapValues((String::toLowerCase))
////                .to("favourite-color-intermediate");
////
////        KTable<String, String> colorTable = builder.table("favourite-color-intermediate");
////
////        KTable<String, Long> table = colorTable
////                .groupBy((user, color) -> new KeyValue<>(color, color))
////                .count("counts");
////
////        table.to(Serdes.String(), Serdes.Long(), "favourite-color-output");
//
//        //if the producer doesn't define the key
////        inputLines
////                .selectKey((key,value) -> value.split(",")[0].toLowerCase())
////                .mapValues((value -> value.split(",")[1].toLowerCase()))
////                .to("favourite-color-intermediate");
//
//        //Solution 2
//        //read as kTable and then write as kTable
//        KTable<String, String> colorTable = builder.table("favourite-color-input");
//        KTable<String, Long> table = colorTable
//                .groupBy((user, color) -> new KeyValue<>(color, color))
//                .count("counts");
//
//        table.to(Serdes.String(), Serdes.Long(), "favourite-color-output");
//        KafkaStreams kafkaStreams = new KafkaStreams(builder, properties);
//        kafkaStreams.cleanUp();
//        kafkaStreams.start();
//
//        System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>> favourite color " + kafkaStreams);
//
//        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
//    }
//}