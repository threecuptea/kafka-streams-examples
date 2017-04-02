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
import org.junit.*;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

//import static org.hamcrest.CoreMatchers.equalTo;
//import static org.junit.Assert.assertThat;


/**
 * Created by fandev on 3/23/17.
 */
public class WordCountIntegrationTest {

    private String streamTempDir;
    private WordCountLambdaDemo wordCountDemo;

    @ClassRule
    public static final EmbeddedKafkaServer SERVER = new EmbeddedKafkaServer();

    @Before
    public void before() throws IOException, InterruptedException {
        SERVER.createTopic(WordCountLambdaDemo.TOPIC_IN);
        SERVER.createTopic(WordCountLambdaDemo.TOPIC_OUT);
    }

    @After
    public void whenShuttingDown() throws IOException {
        wordCountDemo.closeStream();
       TestUtils.deleteFile(new File(streamTempDir));
    }

    @Test
    public void testWordCountDemo() throws Exception {
        /*
        * Step 1: WordCountLambdaDemo streams started (EmbeddedKafkaServer started and input and output topic were created)
        * pass in Embedded bootServer to WordCountLambdaDemo for StreamsConfig
        */
        final Properties producerProp = TestUtils.producerConfig(SERVER.bootstrapServers(),
                StringSerializer.class, StringSerializer.class);
        final Properties consumerProp = TestUtils.consumerConfig(SERVER.bootstrapServers(),
                StringDeserializer.class, LongDeserializer.class);
  //              StringDeserializer.class, StringDeserializer.class);

        final List<String> inputValues = Arrays.asList(
                "all streams lead to kafka",
                "hello kafka streams",
                "join kafka summit");

        final List<KeyValue<String, Long>> expectedOutput_0_cache =
                Arrays.asList(
                        new KeyValue<>("all", 1L),
                        new KeyValue<>("streams", 1L),
                        new KeyValue<>("lead", 1L),
                        new KeyValue<>("to", 1L),
                        new KeyValue<>("kafka", 1L),
                        new KeyValue<>("hello", 1L),
                        new KeyValue<>("kafka", 2L),
                        new KeyValue<>("streams", 2L),
                        new KeyValue<>("join", 1L),
                        new KeyValue<>("kafka", 3L),
                        new KeyValue<>("submit", 1L)
                );

         /*
        * Step 2: Publish some messages following https://kafka.apache.org/documentation/#gettingStarted
        * pass in Embedded bootServer to WordCountLambdaDemo for StreamsConfig
        */
        streamTempDir = TestUtils.tempDirectory().getPath();
        wordCountDemo = new WordCountLambdaDemo(SERVER.bootstrapServers(),
        streamTempDir, true);
        wordCountDemo.execute();
        //
        // Step 3: Verify the application's output data.

        TestUtils.produceValuesSynchronously(WordCountLambdaDemo.TOPIC_IN, inputValues,
                producerProp, SERVER.time);
        Thread.sleep(1000);
        List<KeyValue<String, Long>> actualOuput = TestUtils.waitUntilMinKeyValueRecordsReceived(consumerProp, WordCountLambdaDemo.TOPIC_OUT,
                expectedOutput_0_cache.size(), 10 * 1000);
        System.out.println(actualOuput.size());
        if (actualOuput.size() > 0) {
            for (KeyValue<String, Long> line: actualOuput) {
                System.out.println(line);
            }
        }
        //assertThat(actualOuput, equalTo(expectedOutput));

    }


}
