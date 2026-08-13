package com.mygym.app.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import com.mygym.app.models.BroadcastMessageDto;

import java.util.concurrent.CompletableFuture;

/**
 * Service encapsulating asynchronous Kafka transmission logic and callback handling.
 */
@Service
public class KafkaProducerService {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducerService.class);
    private final KafkaTemplate<String, BroadcastMessageDto> kafkaTemplate;

    public KafkaProducerService(KafkaTemplate<String, BroadcastMessageDto> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Publishes a broadcast payload asynchronously to the configured default Kafka topic.
     */
    public void broadcast(BroadcastMessageDto messagePayload) {
        LOGGER.info("Preparing transport payload for sender: {}", messagePayload.getSenderId());

        CompletableFuture<SendResult<String, BroadcastMessageDto>> future = 
                kafkaTemplate.sendDefault(messagePayload.getSenderId(), messagePayload);

        future.whenComplete((result, exception) -> {
            if (exception == null) {
                LOGGER.info("Successfully pushed payload to Kafka. Partition assigned: {}", 
                        result.getRecordMetadata().partition());
            } else {
                LOGGER.error("Fatal: Kafka cluster rejected payload dispatch due to: ", exception);
            }
        });
    }
}