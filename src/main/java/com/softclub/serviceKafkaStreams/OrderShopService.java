package com.softclub.serviceKafkaStreams;

import com.softclub.model.Order;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@ApplicationScoped
public class OrderShopService {
    
    @Inject
    OrderQueryService interactiveQuery;
    
    // Найти все заказы по shopAddress
    public List<Order> findByShopAddress(String shopAddress) {
        // 1. Получаем индекс по shopAddress
        ReadOnlyKeyValueStore<String, List<String>> shopIndex = interactiveQuery.getShopAddressIndex();
        
        // 2. Получаем список orderId для данного shopAddress
        List<String> orderIds = shopIndex.get(shopAddress);
        
        if (orderIds == null || orderIds.isEmpty())
            return Collections.emptyList();
        
        // 3. Получаем основной store с заказами
        ReadOnlyKeyValueStore<String, Order> ordersStore = interactiveQuery.getOrdersStore();
        
        // 4. Получаем заказы по их ID
        List<Order> orders = new ArrayList<>();
        for (String orderId : orderIds) {
            Order order = ordersStore.get(orderId);
            if (order != null)
                orders.add(order);
        }
        
        return orders;
    }
    
    // Получить все заказы (без фильтрации)
    public List<Order> getAllOrders() {
        ReadOnlyKeyValueStore<String, Order> ordersStore = interactiveQuery.getOrdersStore();
        
        List<Order> allOrders = new ArrayList<>();
        try (KeyValueIterator<String, Order> iterator = ordersStore.all()) {
            while (iterator.hasNext()) {
                allOrders.add(iterator.next().value);
            }
        }
        return allOrders;
    }
    
    // Получить заказ по ID
    public Order getOrderById(String orderId) {
        System.out.println("Searching for order " + orderId + "...");
        ReadOnlyKeyValueStore<String, Order> ordersStore = interactiveQuery.getOrdersStore();
        return ordersStore.get(orderId);
    }
}