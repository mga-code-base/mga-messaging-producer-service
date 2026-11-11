package com.mygym.app.controller;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import com.mygym.app.models.BroadcastMessageDto;
import com.mygym.app.service.KafkaProducerService;

@ExtendWith(MockitoExtension.class)
class BroadcastControllerTest {

    @Mock
    private KafkaProducerService kafkaProducerService;

    @InjectMocks
    private BroadcastController broadcastController;

    @Test
    @DisplayName("Should queue broadcast message and return 202 Accepted")
    void testSendMessageToKafka_Success() {
        BroadcastMessageDto messageDto = new BroadcastMessageDto("ADMIN-101", "Gym closed for maintenance today.", "maintenance.png");

        ResponseEntity<String> response = broadcastController.sendMessageToKafka(messageDto, "key-1");

        assertEquals(HttpStatus.ACCEPTED, response.getStatusCode());
        assertEquals("Message successfully queued for transmission!", response.getBody());
        verify(kafkaProducerService, times(1)).broadcast(messageDto, "key-1");
    }
}