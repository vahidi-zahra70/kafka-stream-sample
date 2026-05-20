package com.practice.producer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.practice.dto.TransactionDTO;
import com.practice.util.InstantAdapter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class BankTransactionProducer {
    static Gson gson = new GsonBuilder()
            .setPrettyPrinting()
            .registerTypeAdapter(Instant.class, new InstantAdapter())
            .create();

    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.RETRIES_CONFIG, "3");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.put(ProducerConfig.LINGER_MS_CONFIG, "1");
        Producer<String, String> producer = new KafkaProducer<>(properties);

        while (true) {

            try {
                TransactionDTO transactionDTO = TransactionDTO.builder()
                        .name("zahra")
                        .time(Instant.now())
                        .amount((long) ThreadLocalRandom.current().nextInt(0, 200)).build();

                producer.send(createRecord(transactionDTO));

                Thread.sleep(100);

                TransactionDTO transactionDTO_2 = TransactionDTO.builder()
                        .name("ali")
                        .time(Instant.now())
                        .amount((long) ThreadLocalRandom.current().nextInt(0, 200)).build();

                producer.send(createRecord(transactionDTO_2));

                Thread.sleep(100);

                TransactionDTO transactionDTO_3 = TransactionDTO.builder()
                        .name("maman")
                        .time(Instant.now())
                        .amount((long) ThreadLocalRandom.current().nextInt(0, 200)).build();

                producer.send(createRecord(transactionDTO_3));

                Thread.sleep(100);
            } catch (InterruptedException e) {
                break;
            }
        }
        producer.close();


    }

    static ProducerRecord<String, String> createRecord(TransactionDTO dto) {
        return new ProducerRecord<>("bank-balance-input", null, gson.toJson(dto));
    }
}
