package com.practice;

import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.KeyValue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FavouriteColorAppTest {

    private TopologyTestDriver topologyTestDriver;
    private TestInputTopic<String, String> inputTopic;
    private TestOutputTopic<String, Long> outputTopic;

    @BeforeEach
    void setup() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:29092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        topologyTestDriver =
                new TopologyTestDriver(
                        new FavouriteColorApp_2().createTopology(),
                        properties
                );

        inputTopic =
                topologyTestDriver.createInputTopic(
                        "favourite-color-input",
                        new StringSerializer(),
                        new StringSerializer()
                );

        outputTopic =
                topologyTestDriver.createOutputTopic(
                        "favourite-color-output",
                        new StringDeserializer(),
                        new LongDeserializer()
                );
    }

    @AfterEach
    void cleanup() {
        topologyTestDriver.close();
    }

    void pushNewRecord(String key, String value) {
        inputTopic.pipeInput(key, value);
    }

    KeyValue<String, Long> readOutputRecord() {
        return outputTopic.readKeyValue();
    }

    @Test
    void given_inputValues_when_push_then_mustCount() {
        // when
        this.pushNewRecord("zahra", "orange");
        this.pushNewRecord("ali", "black");
        this.pushNewRecord("mama", "yellow");
        this.pushNewRecord("zahra", "yellow");
        this.pushNewRecord("ahmad", "white");

        // then
        assertKeyValue(readOutputRecord(), "orange", 1L);
        assertKeyValue(readOutputRecord(), "black", 1L);
        assertKeyValue(readOutputRecord(), "yellow", 1L);
        assertKeyValue(readOutputRecord(), "orange", 0L);
        assertKeyValue(readOutputRecord(), "yellow", 2L);
        assertKeyValue(readOutputRecord(), "white", 1L);
    }

    private void assertKeyValue(KeyValue<String, Long> actual, String expectedKey, Long expectedValue) {
        assertEquals(expectedKey, actual.key);
        assertEquals(expectedValue, actual.value);
    }
}