package com.softclub.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.softclub.model.ClusterInfo;
import com.softclub.model.Order;
import com.softclub.model.OrderSearchResult;
import com.softclub.model.TopicInfo;
import jakarta.inject.Inject;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@ApplicationScoped
public class KafkaService {

    private static final Logger LOG = Logger.getLogger(KafkaService.class);

    @ConfigProperty(name = "kafka.bootstrap.servers")
    String bootstrapServers;

    @Inject
    ObjectMapper objectMapper;

    private AdminClient adminClient;

    @PostConstruct
    void init() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
        adminClient = AdminClient.create(props);
        LOG.infof("AdminClient инициализирован для KRaft кластера: %s", bootstrapServers);
    }

    /**
     * Создание топика с настройками для KRaft
     */
    public void createTopic(String topicName, int partitions, short replicationFactor) {
        NewTopic newTopic = new NewTopic(topicName, partitions, replicationFactor);

        // Конфигурация для KRaft (можно настроить под требования)
        Map<String, String> configs = new HashMap<>();
        configs.put("cleanup.policy", "delete");
        configs.put("retention.ms", "604800000"); // 7 дней
        configs.put("min.insync.replicas", "1");
        newTopic.configs(configs);

        CreateTopicsResult result = adminClient.createTopics(Collections.singleton(newTopic));

        try {
            result.all().get();
            LOG.infof("Топик '%s' создан: partitions=%d, replication=%d",
                topicName, partitions, replicationFactor);
        } catch (InterruptedException | ExecutionException e) {
            LOG.errorf("Ошибка создания топика %s: %s", topicName, e.getMessage());
        }
    }

    /**
     * Получение информации о кластере (особенно важно для KRaft)
     */
    public ClusterInfo getClusterInfo() {
        try {
            DescribeClusterResult describeResult = adminClient.describeCluster();

            ClusterInfo info = new ClusterInfo(describeResult.clusterId().get(), describeResult.controller().get(), describeResult.nodes().get().size());

            // Для KRaft: проверяем quorum voters
            DescribeConfigsResult configResult = adminClient.describeConfigs(
                Collections.singleton(new ConfigResource(ConfigResource.Type.BROKER, "0"))
            );

            Config config = configResult.all().get().values().iterator().next();
            config.get("controller.quorum.voters");//.ifPresent(info::setQuorumVoters);

            return info;

        } catch (Exception e) {
            LOG.errorf("Ошибка получения информации о кластере: %s", e.getMessage());
            return null;
        }
    }

    /**
     * Мониторинг состояния топиков
     */
    public List<TopicInfo> listTopics() {
        try {
            ListTopicsResult topicsResult = adminClient.listTopics();
            Set<String> topicNames = topicsResult.names().get();

            DescribeTopicsResult describeResult = adminClient.describeTopics(topicNames);
            Map<String, TopicDescription> topics = describeResult.allTopicNames().get();

            return topics.entrySet().stream()
                .map(entry -> {
                    TopicDescription desc = entry.getValue();
                    return new TopicInfo(entry.getKey(), desc.partitions().size(), desc.partitions().getFirst().replicas().size(), desc.isInternal());
                })
                .collect(Collectors.toList());

        } catch (Exception e) {
            LOG.errorf("Ошибка получения списка топиков: %s", e.getMessage());
            return Collections.emptyList();
        }
    }

    public String findMessageByOffset(String topic, int partition, long offset) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
//        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "com.example.serialization.OrderDeserializer"); // Ваш десериализатор
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "debug-group-" + UUID.randomUUID());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            TopicPartition topicPartition = new TopicPartition(topic, partition);
            consumer.assign(List.of(topicPartition));
            consumer.seek(topicPartition, offset);

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                String orderJson = record.value();
                Order order = objectMapper.readValue(orderJson, Order.class);

                return String.format("Найдено: ключ=%s, заказId=%s, статус=%s", record.key(), order.getId(), order.getStatus());
            }
            return "Сообщение не найдено.";
        } catch (Exception e) {
            return "Ошибка: " + e.getMessage();
        }
    }

    public OrderSearchResult findMessage2(String orderId) {
        LOG.infof("Старт поиска сообщения");
        Properties props = createConsumerProperties();
        LOG.infof("Созданы свойства Consumer");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            LOG.infof("Создание нового Consumer");
//            consumer.subscribe(Collections.singletonList("orders"));
//            LOG.infof("Подписка оформлена");
            List<PartitionInfo> partitions = consumer.partitionsFor("orders");
            LOG.infof("Поиск заказа %s в %d партициях", orderId, partitions.size());

            if (partitions.isEmpty()) {
                LOG.warn("Топик 'orders' не существует или нет партиций");
                return OrderSearchResult.notFound(orderId);
            }

            // Стратегия поиска: проверяем все партиции
            for (PartitionInfo partitionInfo : partitions) {
                LOG.infof("Проверка в партиции");
                int partitionNum = partitionInfo.partition();
                TopicPartition partition = new TopicPartition("orders", partitionInfo.partition());

                LOG.infof("Проверка партиции %d", partitionNum);
                consumer.assign(Collections.singletonList(partition));


                // Получаем начало и конец лога для этой партиции
                Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(Collections.singleton(partition));
                Map<TopicPartition, Long> endOffsets = consumer.endOffsets(Collections.singleton(partition));

                LOG.infof("Получаем начало и конец");

                Long start = beginningOffsets.get(partition);
                Long end = endOffsets.get(partition);

                if (start == null || end == null) {
                    LOG.infof("Не удалось получить оффсеты для партиции %d", partitionNum);
                    continue;
                }

                if (start.equals(end)) {
                    LOG.infof("Партиция %d пустая (start == end = %d)", partitionNum, start);
                    continue;
                }
                LOG.debugf("Поиск в партиции %d: offsets [%d - %d]", partitionInfo.partition(), start, end);

                // Поиск в конкретной партиции
                OrderSearchResult result = searchInPartition(consumer, partition, orderId, start, end);
                if (result != null) {
                    LOG.infof("Заказ найден в партиции %d", partitionNum);
                    return result;
                }
                consumer.unsubscribe();
            }
            LOG.infof("Заказ %s не найден ни в одной партиции", orderId);
            return OrderSearchResult.notFound(orderId);
        } catch (Exception e) {
            LOG.errorf("Ошибка поиска заказа %s: %s", orderId, e.getMessage());
            return OrderSearchResult.error(orderId, e.getMessage());
        }
    }

    /**
     * Поиск в конкретной партиции
     */
    private OrderSearchResult searchInPartition(KafkaConsumer<String, String> consumer, TopicPartition partition, String targetKey, long startOffset, long endOffset) {
        consumer.assign(Collections.singletonList(partition));

        // Оптимизация: начинаем поиск не с самого начала, а с примерного места
        // Можно реализовать бинарный поиск по offset, если сообщения упорядочены по ключу          // todo бинарный поиск по ключу

        long currentOffset = startOffset;
        final long BATCH_SIZE = 100; // Читаем по 100 сообщений за раз

        while (currentOffset < endOffset) {
            consumer.seek(partition, currentOffset);
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
            if (records.isEmpty()) {
                break;
            }

            // Ищем нужный ключ в пачке
            for (ConsumerRecord<String, String> record : records) {
                if (targetKey.equals(record.key())) {
                    try {
                        Order order = objectMapper.readValue(record.value(), Order.class);
                        return OrderSearchResult.found(order, record);
                    } catch (Exception e) {
                        return OrderSearchResult.error(targetKey, "Ошибка парсинга JSON");
                    }
                }
            }

            // Переходим к следующей пачке
            currentOffset = records.records(partition).get(records.count() - 1).offset() + 1;
        }
        return null;
    }

    /**
     * Ускоренный поиск: если знаем примерное время создания заказа
     */
    public OrderSearchResult findOrderByIdWithTimeHint(String targetKey /*= orderId*/, long approximateTimestamp) {
        Properties props = createConsumerProperties();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            List<PartitionInfo> partitions = consumer.partitionsFor("orders");

            // Ищем во всех партициях, начиная с указанного времени
            for (PartitionInfo partitionInfo : partitions) {
                TopicPartition partition = new TopicPartition("orders", partitionInfo.partition());

                // Получаем offset для указанного времени
                Map<TopicPartition, Long> timestamps = new HashMap<>();
                timestamps.put(partition, approximateTimestamp);

                Map<TopicPartition, OffsetAndTimestamp> offsets = consumer.offsetsForTimes(timestamps);

                if (offsets.get(partition) != null) {
                    long offset = offsets.get(partition).offset();
                    consumer.assign(Collections.singletonList(partition));
                    consumer.seek(partition, Math.max(0, offset - 1000)); // Начинаем за 1000 сообщений до

                    // Поиск вперёд от этой точки
                    ConsumerRecords<String, String> records;
                    int checked = 0;
                    do {
                        records = consumer.poll(Duration.ofMillis(200));
                        for (ConsumerRecord<String, String> record : records) {
                            if (targetKey.equals(record.key())) {
                                Order order = objectMapper.readValue(record.value(), Order.class);
                                return OrderSearchResult.found(order, record);
                            }
                            checked++;
                            if (checked > 5000) break; // Ограничиваем поиск
                        }
                    } while (!records.isEmpty() && checked <= 5000);
                }
            }

            return OrderSearchResult.notFound(targetKey);

        } catch (Exception e) {
            return OrderSearchResult.error(targetKey, e.getMessage());
        }
    }

    public Properties createConsumerProperties() {
        Properties props = new Properties();
//        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "search-service-" + UUID.randomUUID().toString().substring(0, 8));
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "1");
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "100");
        return props;
    }

}