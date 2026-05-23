package com.practice.producer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.practice.dto.UserDTO;
import com.practice.dto.PurchaseDTO;
import com.practice.util.InstantAdapter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class UserDataProducer {

    static Gson gson = new GsonBuilder()
            .setPrettyPrinting()
            .registerTypeAdapter(Instant.class, new InstantAdapter())
            .create();

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        Properties properties = new Properties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.RETRIES_CONFIG, "3");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.put(ProducerConfig.LINGER_MS_CONFIG, "1");
        Producer<String, String> producer = new KafkaProducer<>(properties);

        //new user with their purchase
//        ProducerRecord<String,String> user_1= createUserRecord("123",UserDTO.builder()
//                .firstName("zahra").build());
//        producer.send(user_1).get();
//
//        ProducerRecord<String,String> purchase_1= createPurchaseRecord("123",PurchaseDTO.builder()
//                .title("fish").build());
//        producer.send(purchase_1).get();
//
//        Thread.sleep(10000);
//
//        //new purchase, but the user doesn't exist
//        ProducerRecord<String,String> purchase_2= createPurchaseRecord("124",PurchaseDTO.builder()
//                .title("book").build());
//        producer.send(purchase_2).get();
//
//        Thread.sleep(10000);
//
//        //new purchase for the same user, and update the user info
//        ProducerRecord<String,String> user_3= createUserRecord("123",UserDTO.builder()
//                .firstName("zizi").build());
//        producer.send(user_3).get();
//
//        ProducerRecord<String,String> purchase_3= createPurchaseRecord("123",PurchaseDTO.builder()
//                .title("flower").build());
//        producer.send(purchase_3).get();
//
//        Thread.sleep(10000);
//
//
//        //new purchase for the same user, and update the user info
//        ProducerRecord<String,String> purchase_4= createPurchaseRecord("125",PurchaseDTO.builder()
//                .title("cookie").build());
//        producer.send(purchase_4).get();
//
//        ProducerRecord<String,String> user_4= createUserRecord("125",UserDTO.builder()
//                .firstName("ali").build());
//        producer.send(user_4).get();
//
//        ProducerRecord<String,String> purchase_5= createPurchaseRecord("125",PurchaseDTO.builder()
//                .title("bread").build());
//        producer.send(purchase_5).get();
//
//        ProducerRecord<String,String> user_5= createUserRecord("125",null);
//        producer.send(user_5).get();
//
//        Thread.sleep(10000);


        //deleting the user by passing the null value
        ProducerRecord<String,String> user_6= createUserRecord("126",UserDTO.builder()
                .firstName("amir").build());
        producer.send(user_6).get();


        ProducerRecord<String,String> user_7= createUserRecord("126",null);
        producer.send(user_7).get();

        ProducerRecord<String,String> purchase_6= createPurchaseRecord("126",PurchaseDTO.builder()
                .title("love").build());
        producer.send(purchase_6).get();


        Thread.sleep(10000);

    }

    static ProducerRecord<String, String> createUserRecord(String key, UserDTO dto) {
        return new ProducerRecord<>("user-info-table", key, dto == null ? null : gson.toJson(dto));
    }

    static ProducerRecord<String, String> createPurchaseRecord(String key, PurchaseDTO dto) {
        return new ProducerRecord<>("user-purchase-stream", key, dto == null ? null : gson.toJson(dto));
    }
}
