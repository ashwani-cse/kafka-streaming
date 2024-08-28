package com.kafka.streams.app;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@EnableKafkaStreams
@SpringBootApplication
public class KafkaStreamsAppApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaStreamsAppApplication.class, args);
	}

}
