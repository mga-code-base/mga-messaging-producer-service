package com.mygym.app.service;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import com.mygym.app.models.BroadcastMessageDto;

@Service
public class KafkaProducerService {

    public static final String EVENT_ID_HEADER = "mga-event-id";
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducerService.class);

    private final KafkaTemplate<String, BroadcastMessageDto> kafkaTemplate;

    public KafkaProducerService(KafkaTemplate<String, BroadcastMessageDto> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public String broadcast(BroadcastMessageDto messagePayload) {
        return broadcast(messagePayload, null);
    }

    public String broadcast(BroadcastMessageDto messagePayload, String idempotencyKey) {
        String eventId = idempotencyKey == null || idempotencyKey.isBlank() ? UUID.randomUUID().toString() : idempotencyKey;
        ProducerRecord<String, BroadcastMessageDto> record = new ProducerRecord<>(kafkaTemplate.getDefaultTopic(),
                messagePayload.senderId(), messagePayload);
        record.headers().add(EVENT_ID_HEADER, eventId.getBytes(StandardCharsets.UTF_8));
        LOGGER.info("Publishing broadcast {} from sender {}", eventId, messagePayload.senderId());

        CompletableFuture<SendResult<String, BroadcastMessageDto>> future = kafkaTemplate.send(record);
        future.whenComplete((result, exception) -> {
            if (exception == null) {
                LOGGER.info("Broadcast {} stored in partition {} at offset {}", eventId,
                        result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
            } else {
                LOGGER.error("Broadcast {} was rejected by Kafka", eventId, exception);
            }
        });
        return eventId;
    }
}
