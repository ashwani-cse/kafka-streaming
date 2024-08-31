package com.streams.management.order;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@EnableKafkaStreams
@SpringBootApplication
public class OrderManagementStreamsApplication {

	public static void main(String[] args) {
		SpringApplication.run(OrderManagementStreamsApplication.class, args);
	}

}
