package com.softclub.resources;

import com.softclub.json.JsonProducer;
import com.softclub.json.JsonService;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.reactive.messaging.kafka.companion.KafkaCompanion;
import jakarta.inject.Inject;
import org.apache.kafka.streams.KafkaStreams;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
public class JsonProducerTest {

    @Inject
    JsonProducer jsonProducer;
    @Inject
    JsonService jsonService;
    @Inject
    KafkaStreams kafkaStreams;

    @BeforeEach
    public void initKafkaStreams() throws InterruptedException {
        while(!isKafkaStreamRunning(kafkaStreams)) {
            Thread.sleep(500); // waiting for kafka streams run
        }
    }

    public static boolean isKafkaStreamRunning(KafkaStreams streams) {
        KafkaStreams.State currentState = streams.state();
        return currentState == KafkaStreams.State.RUNNING;// || currentState == KafkaStreams.State.REBALANCING;
    }


    @Test
    void shouldSendJsonAndReturnKey() throws InterruptedException {
        String value = "{\"customerId\": \"service-test\", \"status\": \"active\"}";
        String key = jsonProducer.sendValidatedJsonScheme(value);
        assertNotNull(key);
        Thread.sleep(500);
        String validatedValue = jsonService.getJsonByKey(key);
        assertNotNull(validatedValue);
        System.out.println("! Validated value: " + validatedValue + " , raw json value was: " + value);
        List<String> allValues = jsonService.getJsons();
        assertFalse(allValues.isEmpty());
        System.out.println("! All values: " + allValues);
    }

    @Test
    void shouldSendNotValidJson() throws InterruptedException {
        String value = "{\"user\": \"123\", \"status\": \"not-valid\"}";
        String key = jsonProducer.sendValidatedJsonScheme(value);
        assertNotNull(key);
        Thread.sleep(500);
        String jsn = jsonService.getJsonByKey(key);
        assertNull(jsn);
//        System.out.println("json: " + jsn);
//        assertThrows(NullPointerException.class, () -> jsonService.getJsonByKey(key));
        List<String> allValues = jsonService.getJsons();
        System.out.println("! All jsons: " + allValues);
        assertTrue(allValues.isEmpty() || allValues.stream().noneMatch(json -> json.contains("not-valid")));
    }
}