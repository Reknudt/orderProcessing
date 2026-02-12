package com.softclub.model;

public record TopicInfo(String name, int partitions, int replicationFactor, boolean internal) {}
