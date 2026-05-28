package com.practice.producer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.practice.dto.TransactionDTO;
import com.practice.util.InstantAdapter;
import com.practice.util.Util;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.time.Instant;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

public class BankTransactionProducer {

    public static void main(String[] args) {
        Serde<TransactionDTO> transactionSerde=new JsonSerde<>(TransactionDTO.class, Util.OBJECT_MAPPER);
        Properties properties = new Properties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, transactionSerde.serializer().getClass().getName());
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.RETRIES_CONFIG, "3");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.put(ProducerConfig.LINGER_MS_CONFIG, "1");
        Producer<String, TransactionDTO> producer = new KafkaProducer<>(properties);

        while (true) {

            try {
                TransactionDTO transactionDTO = TransactionDTO.builder()
                        .uuid(UUID.randomUUID().toString())
                        .name("zahra")
                        .time(Instant.now())
                        .amount((long) ThreadLocalRandom.current().nextInt(0, 200)).build();

                producer.send(createRecord(transactionDTO));

                Thread.sleep(100);

                TransactionDTO transactionDTO_2 = TransactionDTO.builder()
                        .uuid(UUID.randomUUID().toString())
                        .name("ali")
                        .time(Instant.now())
                        .amount((long) ThreadLocalRandom.current().nextInt(0, 200)).build();

                producer.send(createRecord(transactionDTO_2));

                Thread.sleep(100);

                TransactionDTO transactionDTO_3 = TransactionDTO.builder()
                        .uuid(UUID.randomUUID().toString())
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

    static ProducerRecord<String, TransactionDTO> createRecord(TransactionDTO dto) {
        return new ProducerRecord<>("bank-balance-input", null, dto);
    }
}
