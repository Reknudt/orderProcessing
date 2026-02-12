package com.softclub.resources;

import io.quarkus.test.common.DevServicesContext;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import jakarta.enterprise.context.ApplicationScoped;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.kafka.clients.admin.AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG;

@ApplicationScoped
public class KafkaTestResource implements QuarkusTestResourceLifecycleManager, DevServicesContext.ContextAware {

    private final KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("apache/kafka-native:3.8.0"));
    private Optional<String> containerNetworkId;
    private final List<String> topics = List.of("jsons", "validated-jsons", "dlq-jsons");

    @Override
    public Map<String, String> start() {
        try {
        containerNetworkId.ifPresent(kafka::withNetworkMode);

        kafka.start();

        Map<String, String> config = new HashMap<>();
        String bootstrapServers = getBootstrapServers();

        config.put("kafka.bootstrap.servers", bootstrapServers);
        config.put("quarkus.kafka-streams.bootstrap-servers", bootstrapServers);
//        config.put("mp.messaging.connector.smallrye-kafka.bootstrap.servers", bootstrapServers);
        config.put("quarkus.kafka.devservices.enabled", "false");
            // Уникальный application ID для Kafka Streams
//            config.put("quarkus.kafka-streams.application-id", "test-app-123"/* + System.currentTimeMillis()*/);
//            config.put("quarkus.kafka-streams.topics", "jsons,validated-jsons");
            // Настройки для топиков
//            config.put("mp.messaging.incoming.dlq-topic.topic", "dlq-jsons");
//            config.put("mp.messaging.outgoing.jsons-out.topic", "validated-jsons");
//            config.put("mp.messaging.incoming.validated-jsons.topic", "validated-jsons");

        System.out.println("Kafka Container started on port: " + kafka.getBootstrapServers());

        List<NewTopic> newTopics = topics.stream().map(topic -> new NewTopic(topic, 3, (short)1)).toList();
        try (AdminClient admin = AdminClient.create(Map.of(BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
            admin.createTopics(newTopics);
        }

//        return Map.of("KAFKA_BOOTSTRAP_SERVERS", getBootstrapServers());
        return config;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private String getBootstrapServers() {
        return kafka.getBootstrapServers();
    }

    @Override
    public void setIntegrationTestContext(DevServicesContext context) {
        containerNetworkId = context.containerNetworkId();
        System.out.println("Use containerNetwork " + containerNetworkId);
    }

    @Override
    public void stop() {
        kafka.close();
    }

}


//        kafka.withEnv("KAFKA_NODE_ID", "1");
//        kafka.withEnv("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:9093");
//        kafka.withEnv("KAFKA_LISTENERS", "PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093");
//        kafka.withEnv("KAFKA_PROCESS_ROLES", "broker,controller");
//        kafka.withEnv("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER");
//        kafka.withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT");
//        kafka.withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "PLAINTEXT");
//        kafka.withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1");
//        kafka.withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0");
//        kafka.withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1");
//        kafka.withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1");
//        kafka.withEnv("KAFKA_LOG_DIRS", "/tmp/kraft-combined-logs");
//
//        // Дополнительные настройки для KRaft
//        kafka.withEnv("CLUSTER_ID", "test-cluster-id-" + System.currentTimeMillis());