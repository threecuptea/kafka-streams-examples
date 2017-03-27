package org.freemind.kafka.streams.integration;

import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KeyValue;
import org.freemind.kafka.streams.examples.wordcount.WordCountLambdaDemo;
import org.freemind.kafka.streams.integration.utils.EmbeddedKafkaServer;
import org.freemind.kafka.streams.integration.utils.TestUtils;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;


/**
 * Created by fandev on 3/23/17.
 */
public class WordCountIntegrationTest {


    @ClassRule
    public static final EmbeddedKafkaServer SERVER = new EmbeddedKafkaServer();

    @BeforeClass
    public static void before() throws IOException, InterruptedException {
        SERVER.createTopic(WordCountLambdaDemo.TOPIC_IN);
        SERVER.createTopic(WordCountLambdaDemo.TOPIC_OUT);
        Thread.sleep(5000);
    }

    @Test
    public void testWordCountDemo() throws Exception {
        /*
        * Step 1: WordCountLambdaDemo streams started (EmbeddedKafkaServer started and input and output topic were created)
        * pass in Embedded bootServer to WordCountLambdaDemo for StreamsConfig
        */
        Properties producerProp = TestUtils.producerConfig(SERVER.bootstrapServers(),
                StringSerializer.class, StringSerializer.class);
        Properties consumerProp = TestUtils.consumerConfig(SERVER.bootstrapServers(),
                StringDeserializer.class, LongDeserializer.class);

        final List<String> inputValues = Arrays.asList(
                "all streams lead to kafka",
                "kafka streams",
                "join kafka summit");

        final List<KeyValue<String, Long>> expectedOutput =
                Arrays.asList(
                        new KeyValue<>("all", 1L),
                        new KeyValue<>("lead", 1L),
                        new KeyValue<>("to", 1L),
                        new KeyValue<>("hello", 1L),
                        new KeyValue<>("streams", 2L),
                        new KeyValue<>("join", 1L),
                        new KeyValue<>("kafka", 3L),
                        new KeyValue<>("submit", 1L)
                );

         /*
        * Step 2: Publish some messages following https://kafka.apache.org/documentation/#gettingStarted
        * pass in Embedded bootServer to WordCountLambdaDemo for StreamsConfig
        */
        TestUtils.produceValuesSynchronously(WordCountLambdaDemo.TOPIC_IN, inputValues,
                producerProp, Time.SYSTEM);
        Thread.sleep(3000);

        WordCountLambdaDemo wordCount = new WordCountLambdaDemo(SERVER.bootstrapServers(),
                TestUtils.tempDirectory().getPath());
        wordCount.execute();
        //
        // Step 3: Verify the application's output data.
        //
        List<KeyValue<String, Long>> actualOuput = TestUtils.waitUntilMinKeyValueRecordsReceived(consumerProp, WordCountLambdaDemo.TOPIC_OUT,
                8, 10 * 1000);
        System.out.println(actualOuput.size());
        //assertThat(actualOuput, equalTo(expectedOutput));

    }


}
