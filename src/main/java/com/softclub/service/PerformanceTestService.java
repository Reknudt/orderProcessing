package com.softclub.service;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.OnOverflow;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@ApplicationScoped
public class PerformanceTestService {

    @Inject
    @Channel("perf-test")
    @OnOverflow(value = OnOverflow.Strategy.BUFFER, bufferSize = 2048)
    Emitter<String> emitter;

    public void runLatencyTest(int messageCount) {
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (int i = 0; i < messageCount; i++) {
            String message = "Test-" + i + "-" + UUID.randomUUID();
            CompletableFuture<Void> future = new CompletableFuture<>();
            futures.add(future);

            emitter.send(Message.of(message)
                    .withAck(() -> {
                        future.complete(null);
                        return CompletableFuture.completedFuture(null);
                    })
                    .withNack(error -> {
                        future.completeExceptionally(error);
                        return CompletableFuture.completedFuture(null);
                    })
            );
        }
        // Ждем завершения всех отправок
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .orTimeout(30, TimeUnit.SECONDS)
                .join();
    }
}
