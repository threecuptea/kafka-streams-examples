package org.freemind.kafka.streams.examples.stock;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.freemind.kafka.streams.examples.model.StockTransaction;
import org.freemind.kafka.streams.examples.model.StockTransactionCollector;
import org.freemind.kafka.streams.examples.model.StockWindow;
import org.freemind.kafka.streams.examples.serializer.JsonDeserializer;
import org.freemind.kafka.streams.examples.serializer.JsonSerializer;

import java.util.Date;
import java.util.Properties;

/**
 * Last update on 5/7/2017
 * This suffers the same problem as
 * https://stackoverflow.com/questions/44049877/kafka-stream-commit-makes-particular-window-to-get-pushed-multiple-times-to-a-to
 * There isn't a answer yet

 * inspired by https://github.com/bbejeck/kafka-streams StockTransactionWindowDemo
 */
public class StockTransactionWindowDemo {

    public static final String TOPIC_IN = "stocks-in";
    public static final String TOPIC_OUT = "stocks-out";
    public static final String TOPIC_SUMMARY = "stocks-summary";
    public static final String STORE_SUMMARY = "stocks-summary-store";


    public static final String APP_ID = "stocks-transaction-window";


    public static void main(String[] args) throws Exception {
        StockTransactionWindowDemo demo = new StockTransactionWindowDemo();
        demo.execute();
    }

    public StockTransactionWindowDemo() {
    }

    public void execute() {
        StreamsConfig config = new StreamsConfig(getProps());

        JsonSerializer<StockTransaction> trxSerializer = new JsonSerializer<>();
        JsonDeserializer<StockTransaction> trxDeserializer = new JsonDeserializer<>(StockTransaction.class);
        Serde<StockTransaction> trxSerde = Serdes.serdeFrom(trxSerializer, trxDeserializer);

        JsonSerializer<StockTransactionCollector> collectorSerializer = new JsonSerializer<>();
        JsonDeserializer<StockTransactionCollector> collectorDeserializer = new JsonDeserializer<>(StockTransactionCollector.class);
        Serde<StockTransactionCollector> collectorSerde = Serdes.serdeFrom(collectorSerializer, collectorDeserializer);

        JsonSerializer<StockWindow> stockWindowJsonSerializer = new JsonSerializer<>();
        JsonDeserializer<StockWindow> stockWindowJsonDeserializer = new JsonDeserializer<>(StockWindow.class);
        Serde<StockWindow> stockWindowSerde = Serdes.serdeFrom(stockWindowJsonSerializer, stockWindowJsonDeserializer);

        KStreamBuilder builder = new KStreamBuilder();
        KStream<String, StockTransaction> sourceStream = builder.stream(Serdes.String(), trxSerde, TOPIC_IN);

        sourceStream.map((k, v) -> new KeyValue<>(v.getSymbol(), v))
                .through(Serdes.String(), trxSerde, TOPIC_OUT)
                .groupByKey(Serdes.String(), trxSerde)
                .aggregate(StockTransactionCollector::new,
                        //(aggKey, newValue, aggValue)
                        (k, v, collector) -> collector.add(v),
                        TimeWindows.of(30000),
                        collectorSerde, STORE_SUMMARY)
                .toStream()
                .map((key, value) -> new KeyValue<>(new StockWindow(key.key(), new Date(key.window().start())), value))
                .to(stockWindowSerde, collectorSerde, TOPIC_SUMMARY);

        System.out.println("Starting StockStreams Example");
        KafkaStreams kafkaStreams = new KafkaStreams(builder, config);
        kafkaStreams.start();
        System.out.println("StockStreams Example started");
    }


    private Properties getProps() {
        java.util.Properties props = new java.util.Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        //props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        //props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 30000);
        //props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, Long.MAX_VALUE);
        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        //props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        props.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);

        return props;

    }

}
