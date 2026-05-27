package com.practice;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.*;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class WordCountAppTest {

    private TopologyTestDriver topologyTestDriver;

    private TestInputTopic<String, String> inputTopic;

    private TestOutputTopic<String, Long> outputTopic;

    StringSerializer stringSerializer = new StringSerializer();

    @BeforeEach
    void setup() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:29092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        topologyTestDriver = new TopologyTestDriver(new WordCountApp().createTopology(), properties);

        inputTopic = topologyTestDriver.createInputTopic("word-count-input",
                stringSerializer,
                stringSerializer);

        outputTopic = topologyTestDriver.createOutputTopic("word-count-output",
                new StringDeserializer(),
                new LongDeserializer());
    }

    @AfterEach
    void cleanup() {
        topologyTestDriver.close();
    }

    void pushNewRecord(String value) {
        inputTopic.pipeInput(null, value);
    }

    KeyValue<String, Long> readOutputRecord() {
        return outputTopic.readKeyValue();
    }

    @Test
    void given_inputValues_when_push_then_mustCount() {
        //given
        String value_1 = "Kafka test me";
        String value_2 = "test me again";
        String value_3 = "kafka again";
        String value_4 = null;
        String value_5 = "         ";

        //when
        this.pushNewRecord(value_1);
        this.pushNewRecord(value_2);
        this.pushNewRecord(value_3);
        this.pushNewRecord(value_4);
        this.pushNewRecord(value_5);

        //then
        assertKeyValue(readOutputRecord(), "kafka", 1L);
        assertKeyValue(readOutputRecord(), "test", 1L);
        assertKeyValue(readOutputRecord(), "me", 1L);

        assertKeyValue(readOutputRecord(), "test", 2L);
        assertKeyValue(readOutputRecord(), "me", 2L);
        assertKeyValue(readOutputRecord(), "again", 1L);

        assertKeyValue(readOutputRecord(), "kafka", 2L);
        assertKeyValue(readOutputRecord(), "again", 2L);
        Assertions.assertTrue(outputTopic.isEmpty());

    }

    private void assertKeyValue(KeyValue<String, Long> actual, String expectedKey, Long expectedValue) {
        assertEquals(expectedKey, actual.key);
        assertEquals(expectedValue, actual.value);
    }

}