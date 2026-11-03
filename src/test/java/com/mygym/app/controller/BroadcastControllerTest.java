package com.mygym.app.controller;

import com.mygym.app.models.BroadcastMessageDto;
import com.mygym.app.service.KafkaProducerService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class BroadcastControllerTest {

    @Mock
    private KafkaProducerService kafkaProducerService;

    @InjectMocks
    private BroadcastController broadcastController;

    @Test
    @DisplayName("Should queue broadcast message and return 200 OK")
    void testSendMessageToKafka_Success() {
        BroadcastMessageDto messageDto = new BroadcastMessageDto("ADMIN-101", "Gym closed for maintenance today.", "maintenance.png");

        ResponseEntity<String> response = broadcastController.sendMessageToKafka(messageDto);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals("Message successfully queued for transmission!", response.getBody());
        verify(kafkaProducerService, times(1)).broadcast(messageDto);
    }
}