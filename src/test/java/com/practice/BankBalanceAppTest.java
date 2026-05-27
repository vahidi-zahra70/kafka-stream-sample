package com.practice;

import com.practice.dto.BalanceDTO;
import com.practice.dto.TransactionDTO;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.time.Instant;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class BankBalanceAppTest {

    private TopologyTestDriver topologyTestDriver;
    private TestInputTopic<String, TransactionDTO> inputTopic;
    private TestOutputTopic<String, BalanceDTO> outputTopic;

    private final Serde<TransactionDTO> transactionSerde =
            new JsonSerde<>(TransactionDTO.class);

    private final Serde<BalanceDTO> balanceSerde =
            new JsonSerde<>(BalanceDTO.class);

    @BeforeEach
    void setup() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:29092");

        topologyTestDriver =
                new TopologyTestDriver(
                        new BankBalanceApp().createTopology(),
                        properties
                );

        inputTopic =
                topologyTestDriver.createInputTopic(
                        "bank-balance-input",
                        new StringSerializer(),
                        transactionSerde.serializer()
                );

        outputTopic =
                topologyTestDriver.createOutputTopic(
                        "bank-balance-output",
                        new StringDeserializer(),
                        balanceSerde.deserializer()
                );
    }

    @AfterEach
    void cleanup() {
        topologyTestDriver.close();
    }

    void pushNewRecord(TransactionDTO value) {
        inputTopic.pipeInput(null, value);
    }

    KeyValue<String, BalanceDTO> readOutputRecord() {
        return outputTopic.readKeyValue();
    }

    @Test
    void given_inputValues_when_push_then_mustCount() {
        // when
        this.pushNewRecord(TransactionDTO.builder().name("zahra").time(Instant.now()).amount(100L).build());
        this.pushNewRecord(TransactionDTO.builder().name("ali").amount(200L).time(Instant.now()).build());
        this.pushNewRecord(TransactionDTO.builder().name("zahra").amount(100L).time(Instant.now()).build());
        this.pushNewRecord(TransactionDTO.builder().name("mahdi").amount(100L).time(Instant.now()).build());

        // then
        assertKeyValue(readOutputRecord(),"zahra", BalanceDTO.builder()
                .balance(100L).count(1L).build());

//        Assertions.assertTrue(outputTopic.isEmpty());
    }

    private void assertKeyValue(KeyValue<String,BalanceDTO> actual,String expectedKey, BalanceDTO expectedBalance) {
        assertEquals(expectedKey, actual.key);
        assertEquals(expectedBalance.getBalance(), actual.value.getBalance());
        assertEquals(expectedBalance.getCount(), actual.value.getCount());
    }
}