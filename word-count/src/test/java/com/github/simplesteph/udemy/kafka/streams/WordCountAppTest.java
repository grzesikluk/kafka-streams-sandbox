package com.github.simplesteph.udemy.kafka.streams;

import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.*;

public class WordCountAppTest {

    TopologyTestDriver testDriver;

    StringSerializer stringSerializer = new StringSerializer();
    TestInputTopic inputTopic;
    TestOutputTopic outputTopic;


    @Before
    public void setUp(){
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        WordCountApp wordCountApp = new WordCountApp();
        Topology topology = wordCountApp.createTopology();
        testDriver = new TopologyTestDriver(topology, config);
        inputTopic = testDriver.createInputTopic("word-count-input", new StringSerializer(), new StringSerializer());
        outputTopic = testDriver.createOutputTopic("word-count-output", new StringDeserializer(), new LongDeserializer());
    }

    @After
    public void tearDown(){
        testDriver.close();
    }

    public void pushNewInputRecord(String value){
        inputTopic.pipeInput(null, value);
    }

    public TestRecord<String, Long> readOutput(){
        return outputTopic.readRecord();
    }

    @Test
    public void makeSureCountsAreCorrect(){
        String firstExample = "testing Kafka Streams";
        pushNewInputRecord(firstExample);
        assertEquals(outputTopic.readKeyValue(), new KeyValue<>("testing", 1L));
        assertEquals(outputTopic.readKeyValue(), new KeyValue<>("kafka", 1L));
        assertEquals(outputTopic.readKeyValue(), new KeyValue<>("streams", 1L));
        assertTrue(outputTopic.isEmpty());

        String secondExample = "testing Kafka again";
        pushNewInputRecord(secondExample);
        assertEquals(outputTopic.readKeyValue(), new KeyValue<>("testing", 2L));
        assertEquals(outputTopic.readKeyValue(), new KeyValue<>("kafka", 2L));
        assertEquals(outputTopic.readKeyValue(), new KeyValue<>("again", 1L));
        assertTrue(outputTopic.isEmpty());

    }

    @Test
    public void makeSureWordsBecomeLowercase(){
        String upperCaseString = "KAFKA kafka Kafka";
        pushNewInputRecord(upperCaseString);
        assertEquals(outputTopic.readKeyValue(), new KeyValue<>("kafka", 1L));
        assertEquals(outputTopic.readKeyValue(), new KeyValue<>("kafka", 2L));
        assertEquals(outputTopic.readKeyValue(), new KeyValue<>("kafka", 3L));
        assertTrue(outputTopic.isEmpty());

    }
}
