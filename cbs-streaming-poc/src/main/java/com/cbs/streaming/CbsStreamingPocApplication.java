package com.cbs.streaming;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@EnableKafkaStreams
@SpringBootApplication
public class CbsStreamingPocApplication {

	public static void main(String[] args) {
		SpringApplication.run(CbsStreamingPocApplication.class, args);
	}

}
