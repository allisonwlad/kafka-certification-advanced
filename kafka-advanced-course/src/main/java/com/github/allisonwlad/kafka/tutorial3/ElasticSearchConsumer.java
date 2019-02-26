package com.github.allisonwlad.kafka.tutorial3;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonParser;


public class ElasticSearchConsumer {
	
	public static RestHighLevelClient createClient(){
		String hostname = "elastic-allison-test-5339594341.us-east-1.bonsaisearch.net";
		String user = "ipn9t1jcox";
		String pwd = "e182f6w82t";
		
		final CredentialsProvider credentials = new BasicCredentialsProvider();
		credentials.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(user, pwd));
		
		RestClientBuilder builder = RestClient.builder(new HttpHost(hostname, 443, "https"))
				.setHttpClientConfigCallback(new HttpClientConfigCallback() {
					
					@Override
					public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
						// TODO Auto-generated method stub
						return httpClientBuilder.setDefaultCredentialsProvider(credentials);
					}
				});
		RestHighLevelClient client = new RestHighLevelClient(builder);
		return client;
	}
	public static KafkaConsumer<String, String> createConsumer(){
		// consumer configs
		String topic = "twetts";
		Properties properties = new Properties();
				
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "Elastic-demo");
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
				
		// create the consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
		consumer.subscribe(Arrays.asList(topic));
		
		return consumer;
	}
	
	public static void main(String[] args) throws IOException {
		Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
		RestHighLevelClient client = createClient();
		KafkaConsumer<String, String> consumer = createConsumer();
		
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
			
			Integer recordCounts = records.count();
			logger.info("recebendo "+ recordCounts +" registros");
			BulkRequest bulkRequest = new BulkRequest();
			for(ConsumerRecord<String, String> record : records){
				//estrategia de geração de ID usando o kafka
				//String id = record.topic() +"_"+record.partition()+"_"+record.offset();
				// estrategia usando o id_str da mensagem vinda do twitter
				String id = extractIdFromTweet(record.value());
				//insert data into elastic
				IndexRequest indexRequest = new IndexRequest("twitter", "tweets", id)
						.source(record.value(), XContentType.JSON);
				
				bulkRequest.add(indexRequest);//faz uma inserção bulk
				
				//IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT); processamento linha a linha
				
			}
			if(recordCounts>0){
				BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
				logger.info("Commit registros");
				consumer.commitSync();
				logger.info("Commit finalizado");
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		//client.close();
	}
	private static JsonParser jsonParser = new JsonParser();
	private static String extractIdFromTweet(String value) {
		// TODO Auto-generated method stub
		return jsonParser.parse(value).getAsJsonObject().get("id_str").getAsString();
	}
}
