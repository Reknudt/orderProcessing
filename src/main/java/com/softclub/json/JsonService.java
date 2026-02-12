package com.softclub.json;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.util.ArrayList;
import java.util.List;

@ApplicationScoped
public class JsonService {

    @Inject
    KafkaStreams streams;

    public List<String> getJsons() {
        ReadOnlyKeyValueStore<String, String> jsonStore = streams.store(StoreQueryParameters.fromNameAndType("validated-jsons-store", QueryableStoreTypes.keyValueStore()));
        List<String> allJsons = new ArrayList<>();
        try (KeyValueIterator<String, String> iterator = jsonStore.all()) {
            while (iterator.hasNext()) {
                allJsons.add(iterator.next().value);
            }
        }
        return allJsons;
    }

    public String getJsonByKey(String key) {
        ReadOnlyKeyValueStore<String, String> jsonStore = streams.store(StoreQueryParameters.fromNameAndType("validated-jsons-store", QueryableStoreTypes.keyValueStore()));
        return jsonStore.get(key);
    }
}
