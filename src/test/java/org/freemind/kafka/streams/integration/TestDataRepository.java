package org.freemind.kafka.streams.integration;

import org.apache.kafka.streams.KeyValue;

import java.util.Arrays;
import java.util.List;

/**
 * Created by fandev on 4/2/17.
 */
public class TestDataRepository {

    public static List<String> getTestWordCountInput() {
        return Arrays.asList(
                "all streams lead to kafka",
                "hello kafka streams",
                "join kafka summit");
    }

    public static List<KeyValue<String, Long>> getExpectedTestWordCountOutput(final long cacheSizeBytes) {
        final List<KeyValue<String, Long>> expectedOutput = cacheSizeBytes == 0 ?
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
                )
                :
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
        return expectedOutput;
    }


}
