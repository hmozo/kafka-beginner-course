package com.doctorkernel.kafkabasics;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

@Component
@Slf4j
public class KafkaConsumerDemo {

    private String groupId= "uruk-group";
    private String topic= "uruk-topic";

    private volatile boolean keepConsuming= true;

    public void init(){
        Properties properties= new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());

        try(KafkaConsumer<String, String> consumer= new KafkaConsumer<>(properties)){
            consumer.subscribe(Collections.singletonList(topic));

            while(keepConsuming){
                ConsumerRecords<String, String> records= consumer.poll(Duration.ofMillis(250));

                for(ConsumerRecord<String, String> record:records){
                    log.info("Key: {}, Value: {}, Partition: {}, Offset: {}", record.key(), record.value(), record.partition(), record.offset());
                }
            }
        }

        KafkaConsumerDemo kafkaConsumerDemo= new KafkaConsumerDemo();
        Runtime.getRuntime().removeShutdownHook(new Thread(kafkaConsumerDemo::shutdown));

    }

    private void shutdown(){
        keepConsuming= false;
    }
}
