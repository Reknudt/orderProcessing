package com.softclub.serviceKafkaStreams;

import com.softclub.model.Order;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.errors.StreamsNotStartedException;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.util.List;

@ApplicationScoped
public class OrderQueryService {
    
    @Inject
    KafkaStreams streams;

    private static final int MAX_RETRIES = 5;
    private static final long RETRY_DELAY_MS = 2000;

    // Метод для получения доступа к Store
    public ReadOnlyKeyValueStore<String, Order> getOrdersStore() {
//        if (streams.state() == KafkaStreams.State.RUNNING)
            return streams.store(StoreQueryParameters.fromNameAndType("orders-main-store", QueryableStoreTypes.keyValueStore()));
//        throw new IllegalStateException("Kafka Streams application is not ready.");     // todo add retry or smth
    }

    // доступ к store для индекса
    public ReadOnlyKeyValueStore<String, List<String>> getShopAddressIndex() {
        return streams.store(StoreQueryParameters.fromNameAndType("shop-address-index", QueryableStoreTypes.keyValueStore()));
    }

}