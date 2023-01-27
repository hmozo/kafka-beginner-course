package com.doctorkernel.kafkabasics;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@Slf4j
@AllArgsConstructor
public class KafkaBasicsApplication implements CommandLineRunner {

	private KafkaProducerDemo producer;
	private KafkaConsumerDemo consumer;

	public static void main(String[] args) {
		SpringApplication.run(KafkaBasicsApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		producer.init();
		consumer.init();

	}
}
