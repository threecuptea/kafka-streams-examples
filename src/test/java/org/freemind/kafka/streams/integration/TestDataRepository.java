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

    public static List<KeyValue<String, Long>> getTestUserClicks() {
        final List<KeyValue<String, Long>> ret = Arrays.asList(
                new KeyValue<>("alice", 13L),
                new KeyValue<>("bob", 4L),
                new KeyValue<>("chao", 25L),
                new KeyValue<>("bob", 19L),
                new KeyValue<>("dave", 56L),
                new KeyValue<>("eve", 78L),
                new KeyValue<>("alice", 40L),
                new KeyValue<>("fang", 99L));
        return ret;
    }

    public static List<KeyValue<String, String>> getTestUserRegions() {
        final List<KeyValue<String, String>> ret =  Arrays.asList(
                new KeyValue<>("alice", "asia"),   /* Alice lived in Asia originally... */
                new KeyValue<>("bob", "americas"),
                new KeyValue<>("chao", "asia"),
                new KeyValue<>("dave", "europe"),
                new KeyValue<>("alice", "europe"), /* ...but moved to Europe some time later. */
                new KeyValue<>("eve", "americas"),
                new KeyValue<>("fang", "asia"));
        return ret;
    }

    public static List<KeyValue<String, Long>> getExpectedRegionClicks(final long cacheSizeBytes) {
        final List<KeyValue<String, Long>> expectedOutput = cacheSizeBytes == 0 ?
                Arrays.asList(
                        new KeyValue<>("europe", 13L),
                        new KeyValue<>("americas", 4L),
                        new KeyValue<>("asia", 25L),
                        new KeyValue<>("americas", 23L),
                        new KeyValue<>("europe", 69L),
                        new KeyValue<>("americas", 101L),
                        new KeyValue<>("europe", 109L),
                        new KeyValue<>("asia", 124L)
                ) :
                Arrays.asList(
                        new KeyValue<>("americas", 101L),
                        new KeyValue<>("europe", 109L),
                        new KeyValue<>("asia", 124L)
                );
        return expectedOutput;
    }






}
