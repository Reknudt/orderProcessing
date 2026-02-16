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

//    @Channel("jsons-in-jsons")
//    Emitter<ProducerRecord<String, String>> jsonEmitter;

    public String sendRawJsonScheme(String jsonPayload) {
        try {
//            LocalDateTime now = LocalDateTime.now();
            String key = UUID.randomUUID().toString();  // key should be random of timestamp + props

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
            String key = UUID.randomUUID().toString(); // key should be random of timestamp + props

            validatedJsonEmitter.send(new ProducerRecord<>("jsons", key, jsonPayload));
            System.out.println("Sent raw JSON with key: " + key);
            return key;
        } catch (Exception e) {
            throw new RuntimeException("Failed to send raw JSON", e);
        }
    }
}
