package com.mygym.app.controller;

import static com.mygym.app.util.MessageProducerConstants.BROADCAST_PUBLISHING_ENDPOINT;
import static com.mygym.app.util.MessageProducerConstants.BROADCAST_REST_CONTROLLER;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.mygym.app.models.BroadcastMessageDto;
import com.mygym.app.service.KafkaProducerService;

/**
 * REST Controller for ingesting broadcast message requests and delegating transmission to Kafka.
 */
@RestController
@RequestMapping(BROADCAST_REST_CONTROLLER)
@CrossOrigin(origins = "*")
public class BroadcastController {

    private final KafkaProducerService kafkaProducerService;

    public BroadcastController(KafkaProducerService kafkaProducerService) {
        this.kafkaProducerService = kafkaProducerService;
    }

    /**
     * Ingests broadcast payloads and dispatches them asynchronously via KafkaProducerService.
     */
    @PostMapping(BROADCAST_PUBLISHING_ENDPOINT)
    public ResponseEntity<String> sendMessageToKafka(@RequestBody BroadcastMessageDto messageDto) {
        kafkaProducerService.broadcast(messageDto);
        return ResponseEntity.ok("Message successfully queued for transmission!");
    }
}