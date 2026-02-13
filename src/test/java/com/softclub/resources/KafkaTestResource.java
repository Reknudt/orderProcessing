package com.softclub.resources;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import jakarta.enterprise.context.ApplicationScoped;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.clients.admin.AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG;

@ApplicationScoped
public class KafkaTestResource implements QuarkusTestResourceLifecycleManager {

    private final KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:4.2.0-rc3"));
    private final List<String> topics = List.of("jsons", "validated-jsons", "dlq-jsons");

    @Override
    public Map<String, String> start() {
        try {
        kafka.start();

        Map<String, String> config = new HashMap<>();
        config.put("kafka.bootstrap.servers", kafka.getBootstrapServers());
        config.put("quarkus.kafka-streams.bootstrap-servers", kafka.getBootstrapServers());
        System.out.println("Kafka Container started on port: " + kafka.getBootstrapServers());

        List<NewTopic> newTopics = topics.stream().map(topic -> new NewTopic(topic, 3, (short)1)).toList();
        try (AdminClient admin = AdminClient.create(Map.of(BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
            admin.createTopics(newTopics);
        }
        return config;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void stop() {
        kafka.close();
    }

}