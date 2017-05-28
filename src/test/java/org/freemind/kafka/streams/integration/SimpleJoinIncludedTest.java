package org.freemind.kafka.streams.integration;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.freemind.kafka.streams.integration.utils.EmbeddedKafkaServer;
import org.freemind.kafka.streams.integration.utils.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import static org.freemind.kafka.streams.integration.TestDataRepository.*;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 *  A simple integration test using
 *  user-click joined with user-region then re-key into region, group, reduce
 *  It is inspired by kafka-src KStreamKTableJoinIntegrationTest
 *
 *  The take aways lesson - publish user-region (lookup table) before publish user-click otherwise region would be
 *  "UNKNOWN" all the way
 *
 * @author sling/threecuptea on 4/2/17.
 */

@RunWith(Parameterized.class)
public class SimpleJoinIncludedTest {

    private static final Logger log = LoggerFactory.getLogger(SimpleJoinIncludedTest.class);

    private final static String TOPIC_USER_CLICK_PREFIX = "topic-user-click-";
    private final static String TOPIC_USER_REGION_PREFIX = "topic-user-region-";
    private final static String STORE_USER_REGION_PREFIX = "store-user-region-";
    private final static String TOPIC_OUT_PREFIX = "topic-out-";
    private final static String APP_PREFIX = "app-";

    private static volatile int testId = 0;
    private String topicUserClick;
    private String topicUserRegion;
    private String storeUserRegion;
    private String topicOut;
    private Properties streamsConfiguration;
    private KafkaStreams kafkaStreams;

    @ClassRule
    public static final EmbeddedKafkaServer SERVER = new EmbeddedKafkaServer();

    @Before
    public void setup() throws InterruptedException {
        testId++;
        topicUserClick = TOPIC_USER_CLICK_PREFIX + testId;
        topicUserRegion = TOPIC_USER_REGION_PREFIX + testId;
        storeUserRegion = STORE_USER_REGION_PREFIX + testId;
        topicOut = TOPIC_OUT_PREFIX + testId;
        SERVER.createTopic(topicUserClick);
        SERVER.createTopic(topicUserRegion);
        SERVER.createTopic(topicOut);

        streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_PREFIX+testId);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, SERVER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, cacheSizeBytes);
        //for testing purpose, the default is 30000
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 3000);//enough time to de-duplicate
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

    /**
     * Tuple for a region and its associated number of clicks.
     */
    private static final class RegionClicks {

        private final String region;
        private final long clicks;

        public RegionClicks(final String region, final long clicks) {
            if (region == null || region.isEmpty()) {
                throw new IllegalArgumentException("region must be set");
            }
            if (clicks < 0) {
                throw new IllegalArgumentException("clicks must not be negative");
            }
            this.region = region;
            this.clicks = clicks;
        }
    }


    @Test
    public void testRegionClick() throws Exception {
        /**
         * STEP1: Start KafkaStreams with region-click logic
         */
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();

        KStreamBuilder builder = new KStreamBuilder();
        KStream<String, Long> userClick = builder.stream(stringSerde, longSerde, topicUserClick);
        KTable<String, String> userRegionTable = builder.table(stringSerde, stringSerde, topicUserRegion, storeUserRegion);
        //Join need to make sure data co-partitioned
        //The join is key-based, i.e. with the join predicate leftRecord.key == rightRecord.key,
        //KStream, KStream join is always Window-based. Two input records are joined if and only if their timestamps
        // are “close” to each other as defined by the user-supplied JoinWindows. Even though this operation is windowed,
        // the joined stream will be of type KStream<K, ...> rather than KStream<Windowed<K>, ...>
        //Suggestions: rewrite https://github.com/confluentinc/examples/blob/3.2.x/kafka-streams/src/test/java/io/confluent/examples/streams/StreamToStreamJoinIntegrationTest.java
        //KTable-KTable joins are always non-windowed joins. They are designed to be consistent with their counterparts
        // in relational databases. The changelog streams of both KTables are materialized into local state
        // stores to represent the latest snapshot of their table duals.
        // The join result is a new KTable that represents the changelog stream of the join operation.
        //KStream and KTable Join, Only input records for the left side (stream) trigger the join.
        // Input records for the right side (table) update only the internal right-side join state.
        //GlobalTable does not require data co-partitioning.  They allow for joining against foreign keys; i.e.,
        //you can lookup data in the table not just by the keys of records in the stream, but also by data in the record values.
        //using KeyValueMapper. KeyValueMapper#apply(leftRecord.key, leftRecord.value) == rightRecord.key.
        //See GlobalKTableIntegrationTest in Kafka src
        KTable<String, Long> clickPerRegion = userClick
                .leftJoin(userRegionTable, (clicks, region) -> new RegionClicks(region == null ? "UNKNOWN" : region, clicks))
                .map((user, regionClick) -> new KeyValue<>(regionClick.region, regionClick.clicks))
                .groupByKey(stringSerde, longSerde)
                .reduce((v1, v2) -> v1 + v2, "ClicksPerRegionUnwindowed");

        clickPerRegion.to(stringSerde, longSerde, topicOut);

        kafkaStreams = new KafkaStreams(builder, streamsConfiguration);
        kafkaStreams.start();

        /**
         * STEP2: Prepare producer. publish messages to in topic
         */

        final Properties userRegionProp = TestUtils.producerConfig(SERVER.bootstrapServers(),
                StringSerializer.class, StringSerializer.class);
        TestUtils.produceKeyValuesSynchronously(topicUserRegion, getTestUserRegions(),
                userRegionProp, SERVER.time);
        Thread.sleep(25);
        final Properties userClickProp = TestUtils.producerConfig(SERVER.bootstrapServers(),
                StringSerializer.class, LongSerializer.class);
        TestUtils.produceKeyValuesSynchronously(topicUserClick, getTestUserClicks(),
                userClickProp, SERVER.time);

        /**
         * STEP3: Prepare consumer. consumer message from out topic and verify
         */
        final Properties consumerProp = TestUtils.consumerConfig(SERVER.bootstrapServers(),
                StringDeserializer.class, LongDeserializer.class);
        log.debug("cacheSizeBytes= {}", cacheSizeBytes);
        List<KeyValue<String, Long>> expectedOutput = getExpectedRegionClicks(cacheSizeBytes);
        List<KeyValue<String, Long>> actualOutput = TestUtils.waitUntilMinKeyValueRecordsReceived(consumerProp,
                topicOut, expectedOutput.size(), 10*1000);
        assertThat(actualOutput, equalTo(expectedOutput));
        System.out.println("==================");
        if (actualOutput.size() > 0) {
            for (KeyValue<String, Long> line: actualOutput) {
                System.out.println(line);
            }
        }
        System.out.println();
    }
}
