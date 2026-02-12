package com.softclub.json;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;

import java.util.UUID;

@ApplicationScoped
public class JsonProducer {

    @Inject
    @Channel("jsons-out")
    Emitter<ProducerRecord<String, String>> emitter;

    @Channel("validated-jsons-out")
    Emitter<ProducerRecord<String, String>> validatedJsonEmitter;

    public String sendRawJsonScheme(String jsonPayload) {
        try {
//            LocalDateTime now = LocalDateTime.now();
            String now = UUID.randomUUID().toString();
            String key = now;

            emitter.send(new ProducerRecord<>("validated-jsons", key, jsonPayload));
            System.out.println("Sent raw JSON with key: " + key);
            return key;
        } catch (Exception e) {
            throw new RuntimeException("Failed to send raw JSON", e);
        }
    }

    public String sendValidatedJsonScheme(String jsonPayload) {
        try {
//            LocalDateTime now = LocalDateTime.now();
            String now = UUID.randomUUID().toString();
            String key = now;

            validatedJsonEmitter.send(new ProducerRecord<>("jsons", key, jsonPayload));
            System.out.println("Sent raw JSON with key: " + key);
            return key;
        } catch (Exception e) {
            throw new RuntimeException("Failed to send raw JSON", e);
        }
    }
}
