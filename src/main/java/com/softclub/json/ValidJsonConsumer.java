package com.softclub.json;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.json.Json;
import jakarta.json.JsonObject;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.jboss.logging.Logger;

import java.io.StringReader;

@ApplicationScoped
public class ValidJsonConsumer {

    private static final Logger LOG = Logger.getLogger(JsonProducer.class);

    /**
     * Обработка валидных json'ов
     */
    @Incoming("validated-jsons")
    public void process(String json) {
        try {
            JsonObject jsonObject = Json.createReader(new StringReader(json)).readObject();
            String customerId = jsonObject.getString("customerId");
            LOG.infof("Получен json = %s, от customer = %s", json, customerId);
        } catch (Exception e) {
            LOG.infof("Ошибка обработки json ", e);
        }
    }

    @Incoming("dlq-jsons")
    public void dlqProcess(String json) {
        try {
            JsonObject jsonObject = Json.createReader(new StringReader(json)).readObject();
            String customerId = jsonObject.getString("customerId");
            LOG.infof("json = %s, от customer = %s отправлен в DLQ", json, customerId);
        } catch (Exception e) {
            LOG.infof("DLQ, Ошибка обработки json ", e);
        }
    }
}