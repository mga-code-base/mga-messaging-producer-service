package com.mygym.app.controller;

import static com.mygym.app.util.MessageProducerConstants.BROADCAST_PUBLISHING_ENDPOINT;
import static com.mygym.app.util.MessageProducerConstants.BROADCAST_REST_CONTROLLER;

import jakarta.validation.Valid;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.mygym.app.models.BroadcastMessageDto;
import com.mygym.app.service.KafkaProducerService;

@RestController
@RequestMapping(BROADCAST_REST_CONTROLLER)
public class BroadcastController {

    public static final String IDEMPOTENCY_KEY = "Idempotency-Key";

    private final KafkaProducerService kafkaProducerService;

    public BroadcastController(KafkaProducerService kafkaProducerService) {
        this.kafkaProducerService = kafkaProducerService;
    }

    @PostMapping(BROADCAST_PUBLISHING_ENDPOINT)
    public ResponseEntity<String> sendMessageToKafka(@Valid @RequestBody BroadcastMessageDto messageDto,
            @RequestHeader(name = IDEMPOTENCY_KEY, required = false) String idempotencyKey) {
        String eventId = kafkaProducerService.broadcast(messageDto, idempotencyKey);
        return ResponseEntity.accepted().header("X-Event-Id", eventId).body("Message successfully queued for transmission!");
    }
}
