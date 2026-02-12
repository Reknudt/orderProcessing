package com.softclub.model;

import java.time.LocalDateTime;

public record OrderMessageDto(String id, String customerId, String product, OrderStatus status, LocalDateTime createdAt,
                              Long kafkaOffset, Integer kafkaPartition, Long kafkaTimestamp) {}
