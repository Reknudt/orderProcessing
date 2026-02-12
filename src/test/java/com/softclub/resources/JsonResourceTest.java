package com.softclub.resources;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import io.restassured.response.Response;
import io.smallrye.reactive.messaging.kafka.companion.KafkaCompanion;
import jakarta.inject.Inject;
import org.apache.kafka.streams.KafkaStreams;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;


@QuarkusTest
@QuarkusTestResource(KafkaTestResource.class)
public class JsonResourceTest {

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
    public void testSendJson() {
        String jsonPayload = "{\"customerId\": \"123\", \"data\": \"test\"}";

        given()
                .contentType(ContentType.JSON)
                .body(jsonPayload)
                .when().post("/jsons")
                .then()
                .statusCode(200)
                .body(notNullValue());
    }

    @Test
    public void testGetJsonById() throws InterruptedException {
        String jsonPayload = "{\"customerId\": \"1\", \"data\": \"test GET by ID\"}";

        Response response = given()
                .contentType(ContentType.JSON)
                .body(jsonPayload)
                .when().post("/jsons");
        assertEquals(200, response.getStatusCode());
        String id = response.getBody().asString();
        assertNotNull(id);

        Thread.sleep(500);

        given()
                .when().get("/jsons/{id}", id)
                .then()
                .statusCode(200);
    }

    @Test
    public void testGetAllJsons() {
        given()
                .when().get("/jsons")
                .then()
                .statusCode(200)
                .contentType(ContentType.JSON);
    }

    @Test
    public void testGetJsonByIdNotFound() {
        given()
                .when().get("/jsons/{id}", "non-existent-key")
                .then()
                .statusCode(404);
    }
}
