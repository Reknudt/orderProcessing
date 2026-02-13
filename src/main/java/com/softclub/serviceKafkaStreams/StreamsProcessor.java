package com.softclub.serviceKafkaStreams;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.xml.bind.ValidationException;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;


@ApplicationScoped
public class StreamsProcessor {

    private static final String ORDERS_STORE = "orders-store-versioned";

//    @Produces     // 1 version, helps to find by id (key)
//    public Topology buildTopology() {
//        System.out.println("!!! topology start");
//        StreamsBuilder builder = new StreamsBuilder();
//
//        ObjectMapperSerde<Order> orderSerde = new ObjectMapperSerde<>(Order.class);
//
//        System.out.println("!!! Создаем State Store для запросов");
//
//        // 1. Создаем State Store для запросов
//        StoreBuilder<KeyValueStore<String, Order>> storeBuilder = Stores.keyValueStoreBuilder(
//                Stores.persistentKeyValueStore("orders-store"),
//                Serdes.String(),
//                orderSerde);
//
//        System.out.println("!!! Создаем KTable для хранения последнего состояния по ключу");
//
//        // 2. Создаем KTable для хранения последнего состояния по ключу
//        KTable<String, Order> ordersTable = builder.stream("orders", Consumed.with(Serdes.String(), orderSerde))
//                .peek((key, order) -> System.out.println("Storing order: " + order.getId()))
//                .toTable(Materialized.<String, Order, KeyValueStore<Bytes, byte[]>>as("orders-store")
//                        .withKeySerde(Serdes.String())
//                        .withValueSerde(orderSerde));
//
//        return builder.build();
//    }

//    @Produces     // 2 version with filter by field
//    public Topology buildTopology() {
//        System.out.println("!!! Building topology with shopAddress index");
//        StreamsBuilder builder = new StreamsBuilder();
//
//        ObjectMapperSerde<Order> orderSerde = new ObjectMapperSerde<>(Order.class);
//
//        // Простой сериализатор для списка строк
//        Serde<List<String>> listSerde = new JsonListSerde();
//
//        System.out.println("!!! 1. Основной KTable для заказов");
//
//        // 1. Основной KTable для заказов (orderId -> Order)
//        KTable<String, Order> ordersTable = builder
//                .stream("orders", Consumed.with(Serdes.String(), orderSerde))
//                .peek((key, order) -> System.out.println("Processing order ID: " + order.getId() + ", shop: " + order.getShopAddress()))
//                .toTable(Materialized.<String, Order, KeyValueStore<Bytes, byte[]>>as("orders-main-store")
//                        .withKeySerde(Serdes.String())
//                        .withValueSerde(orderSerde));
//
//        System.out.println("!!! 2. Создаем индекс по shopAddress");
//
//        // 2. Индекс по shopAddress (shopAddress -> List<orderId>)
//        builder.stream("orders", Consumed.with(Serdes.String(), orderSerde))
//                .filter((orderId, order) -> order.getShopAddress() != null)
//                .map((orderId, order) -> KeyValue.pair(order.getShopAddress(), orderId))
//                .groupByKey(Grouped.with(Serdes.String(), Serdes.String())) // Явно указываем сериализаторы
//                .aggregate(
//                        ArrayList::new, (shopAddress, orderId, orderIdList) -> {
//                            if (!orderIdList.contains(orderId))
//                                orderIdList.add(orderId);
//                            return orderIdList;
//                        },
//                        Materialized.<String, List<String>, KeyValueStore<Bytes, byte[]>>as("shop-address-index")
//                                .withKeySerde(Serdes.String())
//                                .withValueSerde(listSerde) // используем созданный сериализатор
//                );
//
//        return builder.build();
//    }

//    @Produces   // 3 version with String as json and with filter to another topic
//    public Topology buildTopology() {
//        System.out.println("!!! Building topology for json");
//        StreamsBuilder builder = new StreamsBuilder();
//        Serde<String> stringSerde = Serdes.String();
//
//        // 1. State Store для запросов
//        StoreBuilder<KeyValueStore<String, String>> storeBuilder = Stores.keyValueStoreBuilder(
//                Stores.persistentKeyValueStore("jsons-store"),
//                Serdes.String(),
//                stringSerde);
//
//        System.out.println("!!! 2. Создаем jsons store");
//
//        // 2. KTable для хранения последнего состояния по ключу
//        KTable<String, String> jsonTable = builder.stream("jsons", Consumed.with(Serdes.String(), stringSerde))
//                .toTable(Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("jsons-store")
//                        .withKeySerde(Serdes.String())
//                        .withValueSerde(stringSerde));
//
//        // 3 validate and send to another topic
//        builder.stream("jsons", Consumed.with(Serdes.String(), stringSerde))
//                .mapValues(json -> {
//                    try {
//                        if (!json.contains("customerId"))
//                            throw new ValidationException("Missing required field.");
//                        return json;
//                    } catch (ValidationException e) {
//                        System.out.println("Validation failed");
//                        return null;
//                    }
//                })
//                .filter(((key, value) -> value != null))
//                .peek((key, json) -> System.out.println("Valid json: " + key))
//                .to("validated-jsons", Produced.with(Serdes.String(), Serdes.String()));  // .to("validated-jsons");
//
//        return builder.build();
//    }

//    @Produces   // 4 version jsons filtrated to validated without saving in jsons
//    public Topology buildTopology() {
//        System.out.println("!!! Building topology for json");
//        StreamsBuilder builder = new StreamsBuilder();
//        Serde<String> stringSerde = Serdes.String();
//
//        // 1 validate and send to another topic
//        builder.stream("jsons", Consumed.with(Serdes.String(), stringSerde))
//                .mapValues(json -> {
//                    try {
//                        if (!json.contains("customerId"))
//                            throw new ValidationException("Missing required field.");
//                        return json;
//                    } catch (ValidationException e) {
//                        System.out.println("Validation failed");
//                        return null;
//                    }
//                })
//                .filter(((key, value) -> value != null))
//                .peek((key, json) -> System.out.println("Valid json: " + key))
//                .to("validated-jsons", Produced.with(Serdes.String(), Serdes.String()));  // .to("validated-jsons");
//
//
//        // 2. State Store для запросов
//        StoreBuilder<KeyValueStore<String, String>> storeBuilder = Stores.keyValueStoreBuilder(
//                Stores.persistentKeyValueStore("validate-jsons-store"),
//                Serdes.String(),
//                stringSerde);
//
//        System.out.println("!!! 2. Создаем jsons store");
//
//        // 3. KTable для хранения последнего состояния по ключу
//        KTable<String, String> jsonTable = builder.stream("validated-jsons", Consumed.with(Serdes.String(), stringSerde))
//                .toTable(Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("validated-jsons-store")
//                        .withKeySerde(Serdes.String())
//                        .withValueSerde(stringSerde));
//
//
//        return builder.build();
//    }

    @Produces   // 5 version jsons filtrated to validated or moved to dlq
    public Topology buildTopology() {
        System.out.println("!!! Building topology for json");
        StreamsBuilder builder = new StreamsBuilder();
        Serde<String> stringSerde = Serdes.String();

        // 1 get stream
        KStream<String, String> sourceStream = builder.stream("jsons", Consumed.with(Serdes.String(), stringSerde));

        sourceStream
                .filter((key, json) -> json != null && json.contains("customer"))
                .peek((key, json) -> System.out.println("Valid: " + key))
                .to("validated-jsons", Produced.with(Serdes.String(), stringSerde));

        sourceStream
                .filter((key, json) -> json == null || !json.contains("customer"))
                .peek((key, json) -> System.out.println("To DLQ: " + key))
                .to("dlq-jsons", Produced.with(Serdes.String(), stringSerde));

        System.out.println("!!! 2. Создаем validated-jsons store");

        // 2. KTable для хранения последнего состояния по ключу
        KTable<String, String> jsonTable = builder.stream("validated-jsons", Consumed.with(Serdes.String(), stringSerde))
                .toTable(Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("validated-jsons-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(stringSerde));


        return builder.build();
    }

}
