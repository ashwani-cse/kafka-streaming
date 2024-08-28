package com.kafka.streams.app.domain;

import java.time.LocalDateTime;

public record Greeting(String message,
                       LocalDateTime localDateTime) {
}
