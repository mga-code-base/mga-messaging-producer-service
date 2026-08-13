package com.mygym.app.service;

import com.mygym.app.models.BroadcastMessageDto;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class KafkaProducerServiceTest {

    @Mock
    private KafkaTemplate<String, BroadcastMessageDto> kafkaTemplate;

    @InjectMocks
    private KafkaProducerService kafkaProducerService;

    @Test
    @DisplayName("Should delegate message broadcast to kafkaTemplate sendDefault")
    void testBroadcast_Success() {
        BroadcastMessageDto dto = new BroadcastMessageDto();
        dto.setSenderId("ADMIN-99");
        dto.setContent("Flash sale on memberships!");

        CompletableFuture<SendResult<String, BroadcastMessageDto>> future = new CompletableFuture<>();
        when(kafkaTemplate.sendDefault(eq("ADMIN-99"), eq(dto))).thenReturn(future);

        kafkaProducerService.broadcast(dto);

        verify(kafkaTemplate, times(1)).sendDefault("ADMIN-99", dto);
    }
}