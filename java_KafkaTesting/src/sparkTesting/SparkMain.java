package sparkTesting;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.util.*;

public class SparkMain {

	public static void main(String[] args) throws StreamingQueryException {
		System.out.println("start");
		sparkStreamingTest();
		System.out.println("end");
	}

	// SPARK STREAMING 
	/*	https://spark.apache.org/docs/latest/streaming-programming-guide.html
	* 	https://spark.apache.org/docs/2.4.0/streaming-kafka-0-10-integration.html		*/
	public static void sparkStreamingTest() {
		// Configuration for Kafka consumer
		String srcTopic = "mytopic";
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", "localhost:9092");
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", "stream1");
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("enable.auto.commit", false);
		Collection<String> topics = Arrays.asList(srcTopic);

		/*	https://spark.apache.org/docs/latest/api/java/org/apache/spark/SparkConf.html
		 * 	.setMaster(String master) - The master URL to connect to, such as "local" to run locally with one thread, 
		 * 		"local[4]" to run locally with 4 cores, or "spark://master:7077" to run on a Spark standalone cluster. 		*/
		SparkConf conf = new SparkConf().setAppName("sparkEx2").setMaster("local[2]"); // "local" or "local[*]" or local[2] ?

		/*	https://spark.apache.org/docs/latest/api/java/index.html?org/apache/spark/streaming/api/java/JavaStreamingContext.html
		 	A Java-friendly version of StreamingContext which is the main entry point for Spark Streaming functionality. It provides methods to create JavaDStream and JavaPairDStream from input sources	*/
		JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(4000));
		ssc.sparkContext().setLogLevel("ERROR"); // Redirect INFO messages to logs instead of spamming the console

		/* 	Create DStream
		 	https://spark.apache.org/docs/latest/api/java/index.html?org/apache/spark/streaming/api/java/JavaDStream.html 		*/
		JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent(), ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));
		JavaDStream<String> lines = stream.map(r -> r.value().toString()); // ?? difference between DStream and InputDStream ??
		lines.print();

		// Producer configuration 
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		// Make an SQL query on incoming messages and send results to Kafka
		/*	https://spark.apache.org/docs/latest/api/java/index.html?org/apache/spark/api/java/JavaRDD.html		
		 * 	https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/SparkSession.html
		 * 	https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/Dataset.html		*/
		/*
		String outputTopic = "mytopic2";
		KafkaProducer<String, String> prod = new KafkaProducer<>(props);
		JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator()); // Split each line into words
		words.foreachRDD(rdd -> {
			// Get the singleton instance of SparkSession
			SparkSession spark = SparkSession.builder().config(rdd.context().getConf()).getOrCreate(); // sparkContext() (in tutorial) == context() ???
		
			// Create a Dataset
			Dataset<String> ds = spark.createDataset(JavaRDD.toRDD(rdd), Encoders.STRING());
			ds.createOrReplaceTempView("words"); // table name ?
		
			// Make a query on  Dataset
			Dataset<Row> ds2 = spark.sql("SELECT * FROM words");
			ds2.show(); // prints the dataset
		
			// Send dataset to Kafka
			List<String> ds3 = ds2.as(Encoders.STRING()).collectAsList();
			ds3.forEach(e -> prod.send(new ProducerRecord<String, String>(outputTopic, e + " @ " + java.time.Instant.now())));
		});*/

		// JSON stream testing
		lines.foreachRDD(rdd -> {
			SparkSession spark = SparkSession.builder().config(rdd.context().getConf()).getOrCreate();
			Dataset<String> ds = spark.createDataset(JavaRDD.toRDD(rdd), Encoders.STRING());
			Dataset<Row> ds2 = spark.read().json(ds);
			ds2.show();
		});

		ssc.start();
		try {
			ssc.awaitTermination();
			//prod.close();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}

/* OLD -- writing from DStream
lines.foreachRDD(rdd -> rdd.foreach(s -> {
	KafkaProducer<String, String> prod = new KafkaProducer<>(props);
	prod.send(new ProducerRecord<String, String>("mytopic2", s + " @ " + java.time.Instant.now()));
	prod.close();
}));*/