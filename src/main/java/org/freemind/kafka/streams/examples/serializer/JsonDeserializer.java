package org.freemind.kafka.streams.examples.serializer;

import com.google.gson.Gson;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * Created by fandev on 4/11/17.
 */
public class JsonDeserializer<T> implements Deserializer<T> {

    private final Gson gson = new Gson();
    private final Class<T> deserializedClass;

    public JsonDeserializer(Class<T> deserializedClass) {
        this.deserializedClass = deserializedClass;
    }


    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        return gson.fromJson(new String(data), deserializedClass);
    }

    @Override
    public void close() {

    }
}
