package org.freemind.kafka.streams.integration;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.freemind.kafka.streams.integration.utils.EmbeddedKafkaServer;
import org.freemind.kafka.streams.integration.utils.KafkaEmbedded;
import org.freemind.kafka.streams.integration.utils.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import static org.freemind.kafka.streams.integration.TestDataRepository.*;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * https://cwiki.apache.org/confluence/display/KAFKA/KIP-63%3A+Unify+store+and+downstream+caching+in+streams
 *
 *  Local stores usually have a cache to batch writes and reduce the load on their backend.
 *  However, no such cache exists for data sent downstream.
 *
 *  cache.max.bytes.buffering serves as an unified mechanism. The semantics of this parameter is that data is
 *  forwarded and flushed whenever the earliest of commit.interval.ms (note this parameter already exists
 *  and specifies the frequency with which a processor flushes its state) or cache pressure hits.
 *  These are global parameters in the sense that they apply to all processor nodes in the topology,
 *
 * Created by fandev on 4/2/17.
 */
@RunWith(Parameterized.class)
public class WordCountIncludedTest {

    private static final Logger log = LoggerFactory.getLogger(WordCountIncludedTest.class);
    private final static String TOPIC_IN_PREFIX = "wordcount-in-";
    private final static String TOPIC_OUT_PREFIX = "wordcount-out-";
    private final static String WORD_COUNT_APP_PREFIX = "wordcount-integration-";

    private static volatile int testId = 0;
    private String wordCountTopicIn;
    private String wordCountTopicOut;
    private Properties streamsConfiguration;
    private KafkaStreams kafkaStreams;

    @ClassRule
    public static final EmbeddedKafkaServer SERVER = new EmbeddedKafkaServer();

    @Before
    public void setup() throws InterruptedException {
        testId++;
        wordCountTopicIn = TOPIC_IN_PREFIX + testId;
        wordCountTopicOut = TOPIC_OUT_PREFIX + testId;
        SERVER.createTopic(wordCountTopicIn);
        SERVER.createTopic(wordCountTopicOut);

        streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, WORD_COUNT_APP_PREFIX+testId);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, SERVER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, cacheSizeBytes);
        //for testing purpose, the default is 30000
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);//enough time to de-duplicate
    }

    @After
    public void cleanup() throws IOException {
        if (kafkaStreams != null) {
            kafkaStreams.close();
        }
    }

    @Parameterized.Parameter
    public long cacheSizeBytes;

    //Single parameter, use Object[]
    @Parameterized.Parameters
    public static Object[] data() {
        return new Object[] {0, 10 * 1024 * 1024L};
    }


    @Test
    public void testWordCountIncluded() throws Exception {
        /**
         * STEP1: Start KafkaStreams with wordcount logic
         */
        KStreamBuilder builder = new KStreamBuilder();
        KStream<String, String> source = builder.stream(wordCountTopicIn);
        KTable<String, Long> counts = source
                .flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split(" ")))
                .groupBy((key, value) -> value)
                .count("Counts");

        counts.to(Serdes.String(), Serdes.Long(), wordCountTopicOut);

        kafkaStreams = new KafkaStreams(builder, streamsConfiguration);
        kafkaStreams.start();

        /**
         * STEP2: Prepare producer. publish messages to in topic
         */
        final Properties producerProp = TestUtils.producerConfig(SERVER.bootstrapServers(),
                StringSerializer.class, StringSerializer.class);

        TestUtils.produceValuesSynchronously(wordCountTopicIn, getTestWordCountInput(),
                producerProp, SERVER.time);

        /**
         * STEP3: Prepare consumer. consumer message from out topic and verify
         */
        final Properties consumerProp = TestUtils.consumerConfig(SERVER.bootstrapServers(),
                StringDeserializer.class, LongDeserializer.class);
        log.debug("cacheSizeBytes= {}", cacheSizeBytes);
        int expectedOutputSize = getExpectedTestWordCountOutput(cacheSizeBytes).size();
        List<KeyValue<String, Long>> actualOutput = TestUtils.waitUntilMinKeyValueRecordsReceived(consumerProp,
                wordCountTopicOut, expectedOutputSize, 10 * 1000);
        assertThat(actualOutput.size(), equalTo(expectedOutputSize));
        System.out.println("==================");
        if (actualOutput.size() > 0) {
            for (KeyValue<String, Long> line: actualOutput) {
                System.out.println(line);
            }
        }
        System.out.println();
    }

}
