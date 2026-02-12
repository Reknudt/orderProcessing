package com.softclub.resource;

import com.softclub.model.TopicInfo;
import com.softclub.model.ClusterInfo;
import com.softclub.service.KafkaService;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@Path("/kafka")
@Produces(MediaType.APPLICATION_JSON)
public class KafkaResource {

    @Inject
    KafkaService kafkaService;
    
    @GET
    @Path("/cluster")
    public Response getClusterInfo() {
        ClusterInfo info = kafkaService.getClusterInfo();
        if (info == null) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE).build();
        }
        return Response.ok(info).build();
    }
    
    @GET
    @Path("/topics")
    public List<TopicInfo> listTopics() {
        return kafkaService.listTopics();
    }

    @GET
    @Path("/messages")
    public String findMessage(@QueryParam("topic") String topic, @QueryParam("partition") int partition, @QueryParam("offset") long offset) {
        return kafkaService.findMessageByOffset(topic, partition, offset);
    }

    @POST
    @Path("/topics/{name}")
    public Response createTopic(@PathParam("name") String name, @QueryParam("partitions") @DefaultValue("3") int partitions, @QueryParam("replication") @DefaultValue("1") short replication) {
        if (name == null || name.trim().isEmpty()) {
            return Response.status(Response.Status.BAD_REQUEST)
                .entity("Имя топика обязательно").build();
        }
        kafkaService.createTopic(name, partitions, replication);
        return Response.ok().entity("Топик " + name + " создан").build();
    }

    //---------

    @GET
    @Path("/debug/topic-info")
    public String getTopicInfo() {
        Properties props = kafkaService.createConsumerProperties();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            // 1. Проверить существование топика
            List<PartitionInfo> partitions = consumer.partitionsFor("orders");
            if (partitions == null || partitions.isEmpty()) {
                return "Топик 'orders' не существует!";
            }

            StringBuilder result = new StringBuilder();
            result.append("Топик 'orders': ").append(partitions.size()).append(" партиций\n");

            // 2. Проверить каждую партицию
            for (PartitionInfo partition : partitions) {
                TopicPartition tp = new TopicPartition("orders", partition.partition());
                consumer.assign(Collections.singletonList(tp));

                Map<TopicPartition, Long> begin = consumer.beginningOffsets(Collections.singleton(tp));
                Map<TopicPartition, Long> end = consumer.endOffsets(Collections.singleton(tp));

                Long start = begin.get(tp);
                Long endVal = end.get(tp);

                result.append(String.format("  Партиция %d: start=%s, end=%s, сообщений=%s\n",
                        partition.partition(),
                        start,
                        endVal,
                        (start != null && endVal != null) ? (endVal - start) : "N/A"));

                consumer.unsubscribe();
            }

            // 3. Проверить список всех топиков
            Map<String, List<PartitionInfo>> allTopics = consumer.listTopics();
            result.append("\nВсе топики в кластере: ").append(allTopics.keySet());

            return result.toString();

        } catch (Exception e) {
            return "Ошибка: " + e.getMessage();
        }
    }

    @GET
    @Path("/debug/peek-messages")
    public String peekMessages(@QueryParam("limit") @DefaultValue("5") int limit) {
        Properties props = kafkaService.createConsumerProperties();
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList("orders"));

            StringBuilder result = new StringBuilder();
            result.append("Последние ").append(limit).append(" сообщений:\n");

            int count = 0;
            ConsumerRecords<String, String> records;

            do {
                records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    result.append(String.format("  [P%d:O%d] Key='%s', Value(length)=%d\n",
                            record.partition(),
                            record.offset(),
                            record.key(),
                            record.value().length()));
                    count++;
                    if (count >= limit) break;
                }
            } while (count < limit && !records.isEmpty());

            if (count == 0) {
                result.append("  (нет сообщений)");
            }

            return result.toString();
        } catch (Exception e) {
            return "Ошибка: " + e.getMessage();
        }
    }
}