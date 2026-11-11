package com.mygym.app.models;

import java.io.Serializable;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

public record BroadcastMessageDto(
        @NotBlank @Size(max = 64) String senderId,
        @NotBlank @Size(max = 2000) String content,
        @Size(max = 2048) String image) implements Serializable {
}
