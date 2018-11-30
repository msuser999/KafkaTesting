package kafkaApp;

import org.apache.kafka.clients.consumer.*;
import java.time.Duration;
import java.util.Properties;

import javax.swing.JTextArea;

public class ConsumerThread implements Runnable {

	private KafkaConsumer<String, String> consumer;
	private String currentTopic;
	private javax.swing.JTextArea console;

	public ConsumerThread(String serverName, int messagePort, String currentTopic, JTextArea console) {
		Properties props = new Properties();
		props.put("bootstrap.servers", serverName + ":" + messagePort);
		props.put("group.id", "user-" + System.currentTimeMillis());
		props.put("auto.offset.reset", "earliest"); // get all messages
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		this.consumer = new KafkaConsumer<>(props);
		this.currentTopic = currentTopic;
		this.console = console;
	}

	@Override
	public void run() {
		consumer.subscribe(java.util.Collections.singletonList(currentTopic));
		final int giveUp = 100;
		int noRecordsCount = 0;
		while (true) {
			final ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
			if (consumerRecords.count() == 0) {
				noRecordsCount++;
				if (noRecordsCount > giveUp) {
					break;
				} else {
					continue;
				}
			}
			consumerRecords.forEach(record -> {
				console.append(record.value() + "\n");
			});
			consumer.commitAsync(); //??
		}
		consumer.close();
	}

}
