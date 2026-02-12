package com.softclub.serviceKafkaStreams;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import java.util.List;

public class JsonListSerde implements Serde<List<String>> {
    private final ObjectMapper objectMapper = new ObjectMapper();
    
    @Override
    public Serializer<List<String>> serializer() {
        return (topic, data) -> {
            try {
                return objectMapper.writeValueAsBytes(data);
            } catch (Exception e) {
                throw new SerializationException("Error serializing list", e);
            }
        };
    }
    
    @Override
    public Deserializer<List<String>> deserializer() {
        return (topic, data) -> {
            try {
                return objectMapper.readValue(data, new TypeReference<List<String>>() {});
            } catch (Exception e) {
                throw new SerializationException("Error deserializing list", e);
            }
        };
    }
}