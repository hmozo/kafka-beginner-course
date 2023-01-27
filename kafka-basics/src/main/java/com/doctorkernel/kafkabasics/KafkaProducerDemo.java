package com.doctorkernel.kafkabasics;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.stereotype.Component;

import java.util.Properties;

@Component
@Slf4j
public class KafkaProducerDemo {

    public void init(){
        Properties properties= new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try(var producer= new org.apache.kafka.clients.producer.KafkaProducer<String, String>(properties)){
            ProducerRecord<String, String> producerRecord= new ProducerRecord<>("uruk-topic", "hello world");

            log.info("Sending message..");
            producer.send(producerRecord);
        }
    }
}
