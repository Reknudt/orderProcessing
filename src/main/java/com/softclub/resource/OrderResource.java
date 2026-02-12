package com.softclub.resource;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.softclub.model.Order;
import com.softclub.model.OrderSearchResult;
import com.softclub.model.OrderStatus;
import com.softclub.service.KafkaService;
import com.softclub.service.OrderProducer;
import com.softclub.serviceKafkaStreams.OrderShopService;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.jboss.logging.Logger;

import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

@Path("/orders")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class OrderResource {

    private static final Logger LOG = Logger.getLogger(OrderResource.class);

    @Inject
    OrderProducer orderProducer;
    @Inject
    KafkaService kafkaService;
    @Inject
    KafkaStreams streams;
    @Inject
    OrderShopService orderShopService;

    // In-memory хранилище для демонстрации
    private final ConcurrentHashMap<String, Order> orders = new ConcurrentHashMap<>();

    @POST
    public Response createOrder(Order order) throws JsonProcessingException {
        LOG.infof("Создание заказа для клиента: %s", order.getCustomerId());

        // Сохраняем заказ
        orders.put(order.getId(), order);
        // Отправляем в Kafka
        orderProducer.sendNewOrder(order);

        return Response.ok(order).build();
    }

    @GET
    @Path("/{id}/messages")
    public OrderSearchResult getOrderAsMessage(@PathParam("id") String id, @QueryParam("createdAfter") String createdAfter) {
        OrderSearchResult result;
        if (createdAfter != null) {
            try {
                Instant instant = Instant.parse(createdAfter);
                long timestamp = instant.toEpochMilli();

                result = kafkaService.findOrderByIdWithTimeHint(id, timestamp);
            } catch (Exception e) {
                return kafkaService.findMessage2(id);
            }
        } else {
            result = kafkaService.findMessage2(id);
        }

        if (result.isFound()) {
            return result;
        } else {
            throw new NotFoundException();
        }
    }

    @GET
    @Path("/{id}/v1")
    public Order getOrderFromStreams(@PathParam("id") String orderId) {
        ReadOnlyKeyValueStore<String, Order> store = streams.store(StoreQueryParameters.fromNameAndType("orders-store", QueryableStoreTypes.keyValueStore()));
        Order order = store.get(orderId);
        if (order == null)
            throw new NotFoundException("Order not found: " + orderId);
        return order;
    }

    @GET
    @Path("/{id}/v2")
    public Order getOrdersStream(@PathParam("id") String orderId) {
        Order order = orderShopService.getOrderById(orderId);
        if (order == null)
            throw new NotFoundException("Order not found: " + orderId);
        return order;
    }

    @GET
    public List<Order> getAllOrders(@QueryParam("address") String shopAddress) {
        if (shopAddress != null)
            return orderShopService.findByShopAddress(shopAddress);
        return orderShopService.getAllOrders();
    }

//    @GET
//    public List<Order> getAllOrders() {
//        return new ArrayList<>(orders.values());
//    }

    @PUT
    @Path("/{id}/status")
    public Response updateStatus(@PathParam("id") String id, @QueryParam("status") OrderStatus status) throws JsonProcessingException {
        Order order = orders.get(id);
        if (order == null) {
            return Response.status(Response.Status.NOT_FOUND).build();
        }
        order.setStatus(status);
        orderProducer.sendStatusUpdate(order);

        return Response.ok(order).build();
    }

    @GET
    @Path("/health")
    @Produces(MediaType.TEXT_PLAIN)
    public String health() {
        return "OK";
    }
}