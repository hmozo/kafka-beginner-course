package com.doctorkernel.kafkaconsumeropensearch;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.time.Duration;
import java.util.Collections;

@AllArgsConstructor
@Slf4j
@SpringBootApplication
public class OpenSearchConsumer implements CommandLineRunner {

	private final RestHighLevelClient restHighLevelClient;
	private final KafkaConsumer kafkaConsumer;

	public static void main(String[] args) {
		SpringApplication.run(OpenSearchConsumer.class, args);
	}


	@Override
	public void run(String... args) throws Exception {

		try(restHighLevelClient) {
			Boolean indexExists= restHighLevelClient.indices().exists(new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT);

			if (!indexExists) {
				CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
				restHighLevelClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
				log.info("wikimedia index created");
			}else{
				log.info("wikimedia index already exists");
			}

			kafkaConsumer.subscribe(Collections.singleton("wikimedia.recentchanges"));

			while(true){
				ConsumerRecords<String, String> records= kafkaConsumer.poll(Duration.ofMillis(3000));
				int recordCount= records.count();
				log.info("Received " + recordCount + " record(s)");

				for(ConsumerRecord<String, String> record:records){
					try {
						IndexRequest indexRequest = new IndexRequest("wikimedia")
								.source(record.value(), XContentType.JSON);
						IndexResponse response = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
						log.info("Inserted 1 document into OpenSearch. " + response.getId());
					}catch (Exception ex){
						log.error(ex.getMessage());
					}
				}
			}
		}
	}
}
