package com.mygym.app.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import com.mygym.app.models.BroadcastMessageDto;

@ExtendWith(MockitoExtension.class)
class KafkaProducerServiceTest {

    @Mock
    private KafkaTemplate<String, BroadcastMessageDto> kafkaTemplate;

    @InjectMocks
    private KafkaProducerService kafkaProducerService;

    @Test
    @DisplayName("Should key by sender and carry the idempotency key as the event id header")
    void testBroadcast_UsesIdempotencyKeyAsEventId() {
        BroadcastMessageDto dto = new BroadcastMessageDto("ADMIN-99", "Flash sale on memberships!", null);
        when(kafkaTemplate.getDefaultTopic()).thenReturn("broadcast-messages");
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(new CompletableFuture<SendResult<String, BroadcastMessageDto>>());

        String eventId = kafkaProducerService.broadcast(dto, "key-42");

        ArgumentCaptor<ProducerRecord<String, BroadcastMessageDto>> sent = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(sent.capture());
        assertEquals("key-42", eventId);
        assertEquals("broadcast-messages", sent.getValue().topic());
        assertEquals("ADMIN-99", sent.getValue().key());
        assertEquals("key-42", new String(sent.getValue().headers().lastHeader(KafkaProducerService.EVENT_ID_HEADER).value(),
                StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("Should generate an event id when no idempotency key is supplied")
    void testBroadcast_GeneratesEventId() {
        when(kafkaTemplate.getDefaultTopic()).thenReturn("broadcast-messages");
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(new CompletableFuture<SendResult<String, BroadcastMessageDto>>());

        String eventId = kafkaProducerService.broadcast(new BroadcastMessageDto("ADMIN-1", "Hello", null));

        assertNotNull(eventId);
    }
}
