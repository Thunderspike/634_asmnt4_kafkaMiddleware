package thunder.SurveyOnKafka;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.TreeMap;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import com.google.gson.Gson;

public class SurveyConsumer implements Runnable {
	
	private static Map<String, String> env = System.getenv();
	
	KafkaConsumer<String, String> consumer;
	private String topicName;
	private static Map<String, Survey> surveyRecords = Collections.synchronizedMap( 
			new HashMap<String, Survey>()
	);
	
	public List<Survey> getSurveys() {
		return new ArrayList<>(surveyRecords.values());
	}
	
	public Survey getSurvey(int id) {
		return surveyRecords.get(Integer.toString(id));
	}
	
	public int generateNewId() {
		return Integer.parseInt(Collections.max(surveyRecords.keySet())) + 1;
	}
	
	static Gson gson = new Gson();
	
	public SurveyConsumer() {
		this.topicName = "survey-data-topic";
		Properties props = new Properties();
		props.put("bootstrap.servers", env.get("MY_CLUSTER_KAFKA_BOOTSTRAP_PORT_9092_TCP_ADDR")+":9092");
		props.put("group.id", "survey-consumer");
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		consumer = new KafkaConsumer(props);

		List<TopicPartition> partitions = Arrays.asList(new TopicPartition(topicName, 0),
				new TopicPartition(topicName, 1), new TopicPartition(topicName, 2));

		consumer.assign(partitions);
		consumer.seekToBeginning(partitions);
	}

	@Override
	public void run() {
		try {
			while (true) {
				try {
					ConsumerRecords<String, String> records = consumer.poll(100);
					for (ConsumerRecord<String, String> record : records) {
						if (record.key() != null && record.timestamp() > 0) {
							System.out.println("offset: " + record.offset());
							System.out.println("timestamp: " + record.timestamp());
							System.out.println("key: " + record.key());
							System.out.println("value: " + record.value());

							Survey survey = gson.fromJson(record.value(), Survey.class);

							// TreeMap to have only the latest versions of the surveys
							surveyRecords.put(record.key(), survey);
						}
					}
				} catch (Exception e) {
					consumer.close();
				}
			}
		} catch (IllegalStateException | NoSuchElementException e) {
			// System.in has been closed
			System.out.println("Error; exiting");
			consumer.close();
		} finally {
			consumer.close();
		}
	}
}
