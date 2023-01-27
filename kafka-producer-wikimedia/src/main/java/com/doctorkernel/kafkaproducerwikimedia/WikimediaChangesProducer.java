package com.doctorkernel.kafkaproducerwikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
public class WikimediaChangesProducer implements CommandLineRunner {

	public static void main(String[] args) {
		SpringApplication.run(WikimediaChangesProducer.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		String bootstrapServer= "127.0.0.1:9092";

		Properties properties= new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
		properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 *1024));
		properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

		KafkaProducer<String, String> producer= new KafkaProducer<String, String>(properties);
		String topic = "wikimedia.recentchanges";

		EventHandler eventHandler= new WikimediaChangeHandler(producer, topic);
		String url= "https://stream.wikimedia.org/v2/stream/recentchange";
		EventSource eventSource=new EventSource.Builder(eventHandler, URI.create(url)).build();
		eventSource.start();

		TimeUnit.MINUTES.sleep(1);

	}
}
