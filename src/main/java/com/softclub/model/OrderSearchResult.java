package com.softclub.model;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class OrderSearchResult {
        private boolean found;
        private Order order;
        private String orderId;
        private Long kafkaOffset;
        private Integer kafkaPartition;
        private Long kafkaTimestamp;
        private String error;
        
        // Статические конструкторы
        public static OrderSearchResult found(Order order, ConsumerRecord<String, String> record) {
            OrderSearchResult result = new OrderSearchResult();
            result.found = true;
            result.order = order;
            result.orderId = order.getId();
            result.kafkaOffset = record.offset();
            result.kafkaPartition = record.partition();
            result.kafkaTimestamp = record.timestamp();
            return result;
        }
        
        public static OrderSearchResult notFound(String orderId) {
            OrderSearchResult result = new OrderSearchResult();
            result.found = false;
            result.orderId = orderId;
            result.error = "Заказ не найден в Kafka";
            return result;
        }
        
        public static OrderSearchResult error(String orderId, String error) {
            OrderSearchResult result = new OrderSearchResult();
            result.found = false;
            result.orderId = orderId;
            result.error = "Ошибка поиска: " + error;
            return result;
        }
        
        public boolean isFound() { return found; }
        public Order getOrder() { return order; }
        public String getOrderId() { return orderId; }
        public Long getKafkaOffset() { return kafkaOffset; }
        public Integer getKafkaPartition() { return kafkaPartition; }
        public Long getKafkaTimestamp() { return kafkaTimestamp; }
        public String getError() { return error; }
    }