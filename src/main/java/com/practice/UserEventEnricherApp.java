package com.practice;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.practice.dto.*;
import com.practice.util.InstantAdapter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.time.Instant;
import java.util.Properties;

@Slf4j
public class UserEventEnricherApp {
    public static void main(String[] args) {

        Gson gson = new GsonBuilder()
                .setPrettyPrinting()
                .registerTypeAdapter(Instant.class, new InstantAdapter())
                .create();

        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "user-event-enricher-2");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        KStreamBuilder builder = new KStreamBuilder();


        GlobalKTable<String, String> userTable = builder.globalTable("user-info-table");
        KStream<String, String> userPurchaseStream = builder.stream("user-purchase-stream");

        //inner join
        KStream<String, String> innerJoinStream = userPurchaseStream.join(userTable, (key, value) -> key, (purchaseInfo, userInfo) -> {
            try {

                log.info("inner join purchaseInfo {} userInfo{}",purchaseInfo,userInfo);
                PurchaseDTO purchaseDTO = gson.fromJson(purchaseInfo, PurchaseDTO.class);
                UserDTO userDTO = gson.fromJson(userInfo, UserDTO.class);

                UserPurchaseDTO userPurchaseDTO = UserPurchaseDTO.builder()
                        .firstName(userDTO.getFirstName())
                        .lastName(userDTO.getLastName())
                        .title(purchaseDTO.getTitle()).build();

                return gson.toJson(userPurchaseDTO);
            } catch (Exception e) {
                e.printStackTrace();
                log.error("bad input inner join");
                return null;
            }
        });

        //left join
        KStream<String, String> leftJoinStream = userPurchaseStream.leftJoin(userTable, (key, value) -> key, (purchaseInfo, userInfo) -> {
            try {
                PurchaseDTO purchaseDTO = gson.fromJson(purchaseInfo, PurchaseDTO.class);
                UserPurchaseDTO userPurchaseDTO;
                log.info("left join purchaseInfo {}  {}",purchaseInfo,userInfo);
                if(userInfo !=null) {
                    UserDTO userDTO = gson.fromJson(userInfo, UserDTO.class);
                     userPurchaseDTO = UserPurchaseDTO.builder()
                            .firstName(userDTO.getFirstName())
                            .lastName(userDTO.getLastName())
                            .title(purchaseDTO.getTitle()).build();
                }
                else{
                     userPurchaseDTO = UserPurchaseDTO.builder()
                            .title(purchaseDTO.getTitle()).build();
                }

                return gson.toJson(userPurchaseDTO);
            } catch (Exception e) {
                e.printStackTrace();
                log.error("bad input left join");
                return null;
            }
        });

        innerJoinStream.to(Serdes.String(), Serdes.String(), "user-purchase-inner-join");
        leftJoinStream.to(Serdes.String(), Serdes.String(), "user-purchase-left-join");


        KafkaStreams kafkaStreams = new KafkaStreams(builder, properties);
        kafkaStreams.cleanUp();
        kafkaStreams.start();
        System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>> user purchase " + kafkaStreams);
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }
}