package java_KafkaTesting;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import java.time.Duration;
import java.util.Properties;

import java.util.Scanner;

public class Main {

	public static void main(String[] args) {
		ConsumerTest();
	}

	static void ConsumerTest() {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", "user-" + System.currentTimeMillis());
		//props.put("enable.auto.commit", "true");
		//props.put("auto.commit.interval.ms", "500");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		// ENTER TOPIC NAME HERE
		String topicName = "mytopic";

		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(java.util.Collections.singletonList(topicName));

		final int giveUp = 100;
		int noRecordsCount = 0;
		while (true) {
			final ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
			if (consumerRecords.count() == 0) {
				noRecordsCount++;
				if (noRecordsCount > giveUp)
					break;
				else
					continue;
			}
			consumerRecords.forEach(record -> {
				System.out.printf("Consumer Record:(%d, %s, %d, %d)\n", record.key(), record.value(), record.partition(), record.offset());
			});
			consumer.commitAsync();
		}

		consumer.close();
		System.out.println("DONE");
	}

	static void ProducerTest() {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		//props.put("group.id", "user-" + System.currentTimeMillis());
		//props.put("enable.auto.commit", "true");
		//props.put("auto.commit.interval.ms", "500");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		String topicName = "mytopic";

		Producer<String, String> producer = new KafkaProducer<>(props);

		Scanner sc = new Scanner(System.in);
		while (true) {
			String inp = sc.nextLine();
			if (inp.isEmpty()) {
				break;
			} else {
				producer.send(new ProducerRecord<String, String>(topicName, inp + " @ " + java.time.Instant.now()));
			}
		}
		sc.close();
		producer.close();
		System.out.println("DONE");
	}

}
