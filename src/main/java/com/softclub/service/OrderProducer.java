package com.softclub.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.softclub.model.Order;
import io.smallrye.reactive.messaging.kafka.Record;
import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.jboss.logging.Logger;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.time.Instant;

@ApplicationScoped
public class OrderProducer {
    
    private static final Logger LOG = Logger.getLogger(OrderProducer.class);

    @Inject
    ObjectMapper objectMapper;

    @Inject
    @Channel("orders-out")
    Emitter<String> ordersEmitter;
    
    @Inject
    @Channel("status-updates-out")
    Emitter<Record<String, String>> statusUpdatesEmitter;
    
    /**
     * Отправка нового заказа
     */
    public void sendNewOrder(Order order) throws JsonProcessingException {
        LOG.infof("Отправка нового заказа: %s", order.getId());

        String messageKey = order.getId();

        String orderJson = objectMapper.writeValueAsString(order);

        Message<String> message = Message.of(orderJson)
                .addMetadata(OutgoingKafkaRecordMetadata.<String>builder()
                        .withKey(messageKey)           // Ключ = ID заказа
                        .withTopic("orders")
                        .withTimestamp(Instant.now())
                        .build());

        ordersEmitter.send(message);
    }
    
    /**
     * Отправка обновления статуса заказа
     */
    public void sendStatusUpdate(Order order) throws JsonProcessingException {
        LOG.infof("Отправка обновления статуса для заказа %s: %s", order.getId(), order.getStatus());
        String orderJson = objectMapper.writeValueAsString(order);
        // Используем Record для отправки ключа и значения
        statusUpdatesEmitter.send(Record.of(order.getId(), orderJson))
            .whenComplete((success, error) -> {
                if (error != null) {
                    LOG.errorf("Ошибка отправки обновления статуса %s: %s", order.getId(), error.getMessage());
                } else {
                    LOG.infof("Обновление статуса для заказа %s успешно отправлено", order.getId());
                }
            });
    }
}