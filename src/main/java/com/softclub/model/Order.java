package com.softclub.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.UUID;

public class Order {

    private String id;
    private String customerId;
    private String product;
    private String shopAddress;
    private int quantity;
    private double amount;
    private OrderStatus status;
    private LocalDateTime createdAt;

    public Order() {
    }

    @JsonCreator
    public Order(@JsonProperty("customerId") String customerId, @JsonProperty("product") String product) {
        this.customerId = customerId;
        this.product = product;
        this.id = generateId(customerId, product);
        this.createdAt = LocalDateTime.now();
        this.status = OrderStatus.CREATED;
    }

    private String generateId(String customerId, String product) {
        if (customerId == null || product == null)
            return UUID.randomUUID().toString();
        String key = customerId + ":" + product;
        return String.valueOf(UUID.nameUUIDFromBytes(key.getBytes(StandardCharsets.UTF_8)));
    }

    public boolean isValid() {
        return this.id != null;
    }

    public String getId() { return id; }

    public void setId(String id) { this.id = id; }

    public String getCustomerId() { return customerId; }

    public void setCustomerId(String customerId) { this.customerId = customerId; }

    public String getProduct() { return product; }

    public void setProduct(String product) { this.product = product; }

    public String getShopAddress() { return shopAddress; }

    public void setShopAddress(String shopAddress) { this.shopAddress = shopAddress; }

    public int getQuantity() { return quantity; }

    public void setQuantity(int quantity) { this.quantity = quantity; }

    public double getAmount() { return amount; }

    public void setAmount(double amount) { this.amount = amount; }

    public OrderStatus getStatus() { return status; }

    public void setStatus(OrderStatus status) { this.status = status; }

    public LocalDateTime getCreatedAt() { return createdAt; }

    public void setCreatedAt(LocalDateTime createdAt) { this.createdAt = createdAt; }
}
