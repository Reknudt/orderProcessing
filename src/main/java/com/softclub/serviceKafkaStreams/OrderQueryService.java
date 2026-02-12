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

//    public ReadOnlyKeyValueStore<String, Order> getOrdersStore() {
//        return getStoreWithRetry("orders-main-store");
//    }
//
//    public ReadOnlyKeyValueStore<String, List<String>> getShopAddressIndex() {
//        return getStoreWithRetry("shop-address-index");
//    }
//
//    private <K, V> ReadOnlyKeyValueStore<K, V> getStoreWithRetry(String storeName) {
//        int attempts = 0;
//        Exception lastException = null;
//
//        while (attempts < MAX_RETRIES) {
//            try {
//                if (streams.state() != KafkaStreams.State.RUNNING) {
//                    System.out.println("!!! Streams state is not RUNNING");
//                    throw new StreamsNotStartedException("Streams not in RUNNING state");
//                }
//                return streams.store(StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.keyValueStore()));
//
//            } catch (StreamsNotStartedException | IllegalStateException e) {
//                lastException = e;
//                attempts++;
//
//                System.out.println("!!! Streams are not RUNNING, trying to start");
//                streams.start();
//
//                if (attempts < MAX_RETRIES) {
//                    System.out.println(" Retry " + attempts + "/" + MAX_RETRIES + " for store: " + storeName);
//                    try {
//                        Thread.sleep(RETRY_DELAY_MS);
//                    } catch (InterruptedException ie) {
//                        Thread.currentThread().interrupt();
//                        throw new IllegalStateException("Interrupted while waiting for store", ie);
//                    }
//                }
//            }
//        }
//        throw new IllegalStateException("Failed to get store '" + storeName + "' after " + MAX_RETRIES + " attempts", lastException);
//    }
}