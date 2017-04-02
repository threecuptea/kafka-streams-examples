package org.freemind.kafka.streams.examples.wordcount;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.UUID;

/**
 * Inspired by org.apache.kafka.streams.examples.wordcount.WordCountDemo,
 * This is the first time I write java 8 lambda (done that in scala, python and Ruby) and gradle
 *
 *  bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partition 1 --topic streams-file-input
 *
 *  bin/kafka-console-producer.sh --broker-list localhost:9092 --topic streams-file-input < file-input.txt
 *
 *  build:
 *  gradle fatJar
 *  run:
 *  java -cp build/libs/kafka-streams-examples-all-1.0-SNAPSHOT.jar org.freemind.kafka.streams.examples.wordcount.WordCountLambdaDemo
 *
 *  bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic streams-wordcount-output --from-beginning \
 *  --formatter kafka.tools.DefaultMessageFormatter --property print.key=true --property print.value=true \
 *  --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
 *  --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
 *
 *  The results should be
 *  all	1
 *  lead	1
 *  to	1
 *  hello	1
 *  streams	2
 *  join	1
 *  kafka	3
 *  summit	1
 *
 * bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic streams-file-input
 * and other topic or change-log with delete.topic.enable=true
 *
 * Verify with Kafka server
 *
 * @author  sling(threecuptea) on 3/11/2017
 */
public class WordCountLambdaDemo {

    public static final String TOPIC_IN = "streams-file-input";
    public static final String TOPIC_OUT = "streams-wordcount-output";

    private String bootstrapServer;
    private String storePath;
    private boolean itgTest;

    private KafkaStreams kafkaStreams;

    public WordCountLambdaDemo() {
        this("localhost:9092", null, false);
    }

    public WordCountLambdaDemo(String bootstrapServer, String storePath, boolean itgTest) {
        this.bootstrapServer = bootstrapServer;
        this.storePath = storePath;
        this.itgTest = itgTest;
    }

    public static void main(String[] args) throws Exception {
        WordCountLambdaDemo wordCount = new WordCountLambdaDemo();
        wordCount.execute();
    }

    public void execute() throws Exception {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 2000);//enough time to de-duplicate
        if (storePath != null) {
            props.put(StreamsConfig.STATE_DIR_CONFIG, storePath);
        }
        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString()); //To ensure each test run independently

        KStreamBuilder builder = new KStreamBuilder();
        KStream<String, String> source = builder.stream(TOPIC_IN);
        KTable<String, Long> counts = source.flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split(" ")))
                .groupBy((key, value) -> value)
                .count("Counts");
        counts.to(Serdes.String(), Serdes.Long(), TOPIC_OUT);

        kafkaStreams = new KafkaStreams(builder, props);
        kafkaStreams.start();
        if (!itgTest) {
            Thread.sleep(3000);
            kafkaStreams.close();
        }
    }

    public void closeStream() {
        //integration test will close streams
        if (kafkaStreams != null) {
            kafkaStreams.close();
        }
    }

}
