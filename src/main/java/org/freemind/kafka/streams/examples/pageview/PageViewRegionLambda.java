package org.freemind.kafka.streams.examples.pageview;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.internals.TimeWindow;

import java.util.Properties;
import java.util.UUID;

/**
 * Inspired by org.apache.kafka.streams.examples.pageview.PageViewUntypedDemo
 * Created by fandev on 3/11/17.
 */
public class PageViewRegionLambda {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pageview-untyped");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, JsonTimestampExtractor.class);

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString()); //To ensure each test run independently

        KStreamBuilder builder = new KStreamBuilder();
        Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
        Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);

        KStream<String, JsonNode> views = builder.stream(Serdes.String(), jsonSerde, "streams-pageview-input");
        KTable<String, JsonNode> users = builder.table(Serdes.String(), jsonSerde, "streams-userprofile-input", "streams-userprofile-store");
        KTable<String, String> userRegions =  users.mapValues(value -> value.get("region").textValue());

        //return KTable<Windowed<String>, Long> after .count(TimeWindows.of(5 * 60 * 1000L).advanceBy(60 * 1000L), "PageViewsByRegionStore")
        KStream<JsonNode, JsonNode> regionCounts = views.join(userRegions, (view, region) -> {
            ObjectNode jNode = JsonNodeFactory.instance.objectNode()
                                .put("user", view.get("user").textValue())
                                .put("page", view.get("page").textValue())
                                .put("region", region == null ? "UNKNOWN" : region);
            return (JsonNode)jNode;
        })
        .map((user,viewRegion) -> new KeyValue<>(viewRegion.get("region").textValue(), viewRegion))
        .groupByKey(Serdes.String(), jsonSerde)
        .count(TimeWindows.of(5 * 60 * 1000).advanceBy( 60 * 1000), "PageViewsByRegionStore")
        .toStream()
        .map((key, value) -> {
            ObjectNode keyNode = JsonNodeFactory.instance.objectNode()
                    .put("window-start", key.window().start())
                    .put("region", key.key());
            ObjectNode valueNode = JsonNodeFactory.instance.objectNode()
                    .put("count", value);
            return new KeyValue<>((JsonNode)keyNode, (JsonNode)valueNode);
        });

        regionCounts.to(jsonSerde, jsonSerde, "streams-pageviewstats-untyped-output");

        KafkaStreams streams = new KafkaStreams(builder, props);
        streams.start();
        Thread.sleep(5000L);

        streams.close();
    }
}
