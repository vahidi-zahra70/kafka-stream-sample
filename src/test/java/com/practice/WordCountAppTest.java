//package com.practice;
//
//import org.apache.kafka.clients.producer.ProducerRecord;
//import org.apache.kafka.common.serialization.LongDeserializer;
//import org.apache.kafka.common.serialization.Serdes;
//import org.apache.kafka.common.serialization.StringDeserializer;
//import org.apache.kafka.common.serialization.StringSerializer;
//import org.apache.kafka.streams.StreamsConfig;
//import org.apache.kafka.streams.TopologyTestDriver;
//import org.apache.kafka.streams.test.ConsumerRecordFactory;
//import org.apache.kafka.streams.test.OutputVerifier;
//import org.junit.jupiter.api.AfterEach;
//import org.junit.jupiter.api.Assertions;
//import org.junit.jupiter.api.BeforeEach;
//import org.junit.jupiter.api.Test;
//
//import java.util.Properties;
//
//import static org.junit.jupiter.api.Assertions.*;
//
//class WordCountAppTest {
//
//    private TopologyTestDriver topologyTestDriver;
//
//    StringSerializer stringSerializer=new StringSerializer();
//    private final ConsumerRecordFactory<String, String > consumerRecordFactory=new ConsumerRecordFactory<>(stringSerializer,stringSerializer);
//
//    @BeforeEach
//    void setup(){
//        Properties properties = new Properties();
//        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
//        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:29092");
//        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
//        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
//
//        topologyTestDriver=new TopologyTestDriver(new WordCountApp().createTopology(),properties);
//    }
//
//    @AfterEach
//    void cleanup(){
//        topologyTestDriver.close();
//    }
//
//    void pushNewRecord(String value){
//        topologyTestDriver.pipeInput(consumerRecordFactory.create("word-count-input",null,value));
//    }
//
//    ProducerRecord<String,Long> readOutputRecord(){
//      return   topologyTestDriver.readOutput("word-count-output",new StringDeserializer(),new LongDeserializer());
//    }
//
//    @Test
//    void given_inputValues_when_push_then_mustCount(){
//        //given
//        String value_1="Kafka test me";
//        String value_2="test me again";
//        String value_3="kafka again";
//        String value_4=null;
//        String value_5="         ";
//
//        //when
//        this.pushNewRecord(value_1);
//        this.pushNewRecord(value_2);
//        this.pushNewRecord(value_3);
//        this.pushNewRecord(value_4);
//        this.pushNewRecord(value_5);
//
//        //then
//        OutputVerifier.compareKeyValue(readOutputRecord(),"kafka",1L);
//        OutputVerifier.compareKeyValue(readOutputRecord(),"test",1L);
//        OutputVerifier.compareKeyValue(readOutputRecord(),"me",1L);
//
//        OutputVerifier.compareKeyValue(readOutputRecord(),"test",2L);
//        OutputVerifier.compareKeyValue(readOutputRecord(),"me",2L);
//        OutputVerifier.compareKeyValue(readOutputRecord(),"again",1L);
//
//        OutputVerifier.compareKeyValue(readOutputRecord(),"kafka",2L);
//        OutputVerifier.compareKeyValue(readOutputRecord(),"again",2L);
//        Assertions.assertNull(readOutputRecord());
//
//    }
//
//}