package com.softclub.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.softclub.model.Order;
import com.softclub.model.OrderStatus;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.jboss.logging.Logger;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class OrderConsumer {
    
    private static final Logger LOG = Logger.getLogger(OrderConsumer.class);

    @Inject
    ObjectMapper objectMapper;

    @Inject
    OrderProducer orderProducer;
    
    /**
     * Обработка новых заказов
     */
    @Incoming("orders-in")
    public void process(ConsumerRecord<String, String> record) throws JsonProcessingException {
        Order order = objectMapper.readValue(record.value(), Order.class); // Получаем ваш объект Order
        String messageKey = record.key(); // Ключ сообщения (может быть null)
        long offset = record.offset();
        int partition = record.partition();
        LOG.infof("Обработка заказа: ID=%s, ключ=%s, партиция=%d, смещение=%d%n", order.getId(), messageKey, partition, offset);

        order.setStatus(OrderStatus.COMPLETED);
        orderProducer.sendStatusUpdate(order);
        LOG.infof("Заказ %s обработан успешно", order.getId());
    }

    /**
     * Обработка обновлений статусов
     */
    @Incoming("status-updates-in")
    public void processStatusUpdate(String orderJson) throws JsonProcessingException {
        Order order = objectMapper.readValue(orderJson, Order.class);
        LOG.infof("Получено обновление статуса: Заказ=%s, Статус=%s", order.getId(), order.getStatus());
        switch (order.getStatus()) {
            case PROCESSING:
                LOG.infof("Заказ %s передан в обработку", order.getId());
                break;
            case COMPLETED:
                LOG.infof("Заказ %s выполнен. Сумма: %.2f", 
                    order.getId(), order.getAmount());
                break;
            case CANCELLED:
                LOG.warnf("Заказ %s отменен", order.getId());
                break;
        }
    }
}