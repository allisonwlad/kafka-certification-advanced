package com.github.allisonwlad.kafka.tutorial3;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import com.google.gson.JsonParser;

public class KafkaStreamsFilter {

	private static JsonParser jsonParser = new JsonParser();
	private static Integer extractUserFollowers(String value) {
		// TODO Auto-generated method stub
		try {
			return jsonParser.parse(value)
				.getAsJsonObject()
				.get("user")
				.getAsJsonObject()
				.get("followes_count")
				.getAsInt();
		}catch(NullPointerException e){
			return 0;
		}
	}
	public static void main(String[] args) {
		//create properties
		Properties properties = new Properties();
		properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "demo-kafka-streams");
		properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
		properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
		
		//create topology
		StreamsBuilder streamsBuilder = new StreamsBuilder();
		
		//input topic
		KStream<String,String> inputTopic = streamsBuilder.stream("tweets");
		KStream<String,String> filterStream = inputTopic.filter(
				(k, jsonTweet) -> extractUserFollowers(jsonTweet)>1000
						//filter tweets has au ser over 1000 followers
		);
		filterStream.to("important_tweets");
		//build topology
		KafkaStreams kafkaStrems = new KafkaStreams(streamsBuilder.build(), properties);
		//start streaming
		kafkaStrems.start();
	}
}
