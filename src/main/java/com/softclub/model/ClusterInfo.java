package com.softclub.model;

import org.apache.kafka.common.Node;

public record ClusterInfo(String clusterId, Node controller, int nodes, String quorumVoters) {
    public ClusterInfo(String clusterId, Node controller, int nodes) {
        this(clusterId, controller, nodes, null);
    }
}
