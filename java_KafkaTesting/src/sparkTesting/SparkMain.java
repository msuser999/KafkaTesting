package sparkTesting;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.util.*;

// code cleanup, new class to avoid issues (replace SparkApp with this if works)
public class SparkMain {

	public static void main(String[] args) throws StreamingQueryException {
		System.out.println("start");
		webSparkTest();
		System.out.println("end");
	}

	static HashMap<String, List<String>> queries = new HashMap<>();

	public static void addQuery(String tableName, String query) {
		if (queries.containsKey(tableName) && !queries.get(tableName).contains(query)) {
			List<String> qs = new ArrayList<>();
			queries.get(tableName).forEach(n -> qs.add(n));
			qs.add(query);
			queries.put(tableName, qs);
		} else {
			queries.put(tableName, Arrays.asList(query));
		}
	}

	public static void webSparkTest() {
		String srcTopic = "mytopic";
		Collection<String> topics = Arrays.asList(srcTopic);
		Map<String, Object> kafkaParams = getLocalKafkaParams();
		SparkConf conf = new SparkConf().setAppName("sparkEx2").setMaster("local[2]");
		JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(4000));
		ssc.sparkContext().setLogLevel("ERROR");
		JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent(), ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));

		JavaDStream<String> lines = stream.map(r -> r.value().toString());
		lines.print();

		JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator());
		words.foreachRDD(rdd -> {
			SparkSession spark = SparkSession.builder().config(rdd.context().getConf()).getOrCreate();
			Dataset<String> ds = spark.createDataset(JavaRDD.toRDD(rdd), Encoders.STRING());
			String tableName = "words";
			ds.createOrReplaceTempView(tableName);
			Dataset<Row> ds2 = spark.sql("SELECT * FROM " + tableName);
			ds2.show();
		});

		WebInterface wb;
		try {
			wb = new WebInterface();
			wb.run();
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		ssc.start();
		try {
			ssc.awaitTermination();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	// SPARK STREAMING
	/*	https://spark.apache.org/docs/latest/streaming-programming-guide.html
	* 	https://spark.apache.org/docs/2.4.0/streaming-kafka-0-10-integration.html		*/
	public static void sparkStreamingTest() {
		// Configuration for Kafka consumer
		String path = "/Users/roopekausiala"; //!!!CHANGE THE PATH to the location of client.keystore.jks and client.truststore.jks

		//If there is no existing topic for the datatable/connector, the Debezium connector makes new topic when you make
		//changes to the table in Postgres (if you have the connector running)
		String srcTopic = "test.public.testi";
		Map<String, Object> kafkaParams = getVMKafkaParams(path);
		Collection<String> topics = Arrays.asList(srcTopic);

		/*	https://spark.apache.org/docs/latest/api/java/org/apache/spark/SparkConf.html
		 * 	.setMaster(String master) - The master URL to connect to, such as "local" to run locally with one thread,
		 * 		"local[4]" to run locally with 4 cores, or "spark://master:7077" to run on a Spark standalone cluster. 		*/
		SparkConf conf = new SparkConf().setAppName("sparkEx2").setMaster("local[2]"); // "local" or "local[*]" or local[2] ?

		/*	https://spark.apache.org/docs/latest/api/java/index.html?org/apache/spark/streaming/api/java/JavaStreamingContext.html
		 	A Java-friendly version of StreamingContext which is the main entry point for Spark Streaming functionality. It provides methods to create JavaDStream and JavaPairDStream from input sources	*/
		JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(4000));
		ssc.sparkContext().setLogLevel("ERROR"); // Redirect INFO messages to logs instead of spamming the console
		//SparkSession session = SparkSession.builder().config(conf).getOrCreate();

		//HashMap<String, String> map = new HashMap<>();
		//map.put("url", "jdbc:postgresql://lgn/postgres?user=vagrant&password=1234");
		//map.put("dbtable", "users");
		//jdbcD = session.sqlContext().load("jdbc", map);

		//reading from postgres, probably useless
		/*Dataset<Row> jdbcDF = session.read()
				  .format("jdbc")
				  .option("url", "jdbc:postgresql://localhost/postgres")
				  .option("dbtable", "schema.tablename")
				  .option("user", "postgres")
				  //.option("password", "password")
				  .load();*/

		//SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sccc);

		//JavaSparkContext ssc = new JavaSparkContext(conf);

		/* 	Print received message
		 	https://spark.apache.org/docs/latest/api/java/index.html?org/apache/spark/streaming/api/java/JavaDStream.html 		*/
		JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent(), ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));
		JavaDStream<String> lines = stream.map(r -> r.value().toString()); // ?? difference between DStream and InputDStream ??
		lines.print();

		lines.foreachRDD(rdd -> {
			SparkSession spark = SparkSession.builder().config(rdd.context().getConf()).getOrCreate();
			Dataset<String> ds = spark.createDataset(JavaRDD.toRDD(rdd), Encoders.STRING());
			if (!ds.isEmpty()) {
				String tableName = spark.read().json(ds).select("payload.source.table").collectAsList().get(0).get(0).toString();
				System.out.println("table name: " + tableName);
				List<String> qs = queries.get(tableName);
				if (qs != null) {
					qs.forEach(q -> {
						Dataset<Row> ds2 = spark.read().json(ds).drop("schema").select("payload.after.*");
						ds2.createOrReplaceTempView(tableName);
						Dataset<Row> ds3 = spark.sql(q);
						ds3.show();
					});
				}
			} else {
				Dataset<Row> ds3 = spark.read().json(ds);
				ds3.show();
			}
		});

		// TEST same as above but with Structured Streaming
		SparkSession sparkStr = SparkSession.builder().appName("StructuredStreaming").master("local[*]").getOrCreate();
		String pathStr = "/Users/markorepairs :--D";
		String topic = "test.public.testi";
		HashMap<String, String> kafkaParamsStr = new HashMap<>();
		kafkaParams.put("kafka.bootstrap.servers", "canadmin:CS-C2130#yeswecan@cankafka.northeurope.cloudapp.azure.com:9092");
		kafkaParams.put("security.protocol", "SSL");
		kafkaParams.put("ssl.truststore.location", path + "/client.truststore.jks");
		kafkaParams.put("ssl.truststore.password", "j!z,KS)u]g656aHY]t5P");
		kafkaParams.put("ssl.keystore.location", path + "/client.keystore.jks");
		kafkaParams.put("ssl.keystore.password", "j!z,KS)u]g656aHY]t5P");
		kafkaParams.put("ssl.key.password", "j!z,KS)u]g656aHY]t5P");
		kafkaParams.put("ssl.endpoint.identification.algorithm", "");

		Dataset<Row> dsStr = readKafka(sparkStr, topic, kafkaParamsStr);
		if (!dsStr.isEmpty()) {
			String tableName = dsStr.select("payload.source.table").collectAsList().get(0).get(0).toString();
			System.out.println("table name: " + tableName);
			List<String> qs = queries.get(tableName);
			if (qs != null) {
				qs.forEach(q -> {
					Dataset<Row> ds2 = sparkStr.read().json(dsStr.as(Encoders.STRING())).drop("schema").select("payload.after.*");
					ds2.createOrReplaceTempView(tableName);
					Dataset<Row> ds3 = sparkStr.sql(q);
					ds3.show();
				});
			}
		} else {
			Dataset<Row> ds3 = sparkStr.read().json(dsStr.as(Encoders.STRING()));
			ds3.show();
		}

		//WebInterface.main();
		ssc.start();

		try {
			ssc.awaitTermination();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	//Send JSON from Kafka and create Dataset table from the data
	/* 	JSON Lines or newline-delimited JSOon (ndjson)	
	 * 		http://jsonlines.org/examples/	
	 * 		https://github.com/ndjson/ndjson-spec
	 * 	https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/DataFrameReader.html 	*/
	public static void jsonStreamTest(JavaDStream<String> lines) {
		lines.foreachRDD(rdd -> {
			SparkSession spark = SparkSession.builder().config(rdd.context().getConf()).getOrCreate();
			Dataset<String> ds = spark.createDataset(JavaRDD.toRDD(rdd), Encoders.STRING()); //
			Dataset<Row> ds2 = spark.read().json(ds); // .read() -> DataframeReader -> .json() -> Dataset<Row>
			ds2.show();
		});
	}

	// Make an SQL query on incoming messages and send results to Kafka
	/*	https://spark.apache.org/docs/latest/api/java/index.html?org/apache/spark/api/java/JavaRDD.html		
	 * 	https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/SparkSession.html
	 * 	https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/Dataset.html		*/
	public static void splitLinesTest(JavaDStream<String> lines, KafkaProducer<String, String> prod) {
		String outputTopic = "mytopic2";
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
		});
	}

	// Kafka params for local use 
	public static Map<String, Object> getLocalKafkaParams() {
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", "localhost:9092");
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", "stream1");
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("enable.auto.commit", false);
		return kafkaParams;
	}

	// Kafka params for Azure VM use
	public static Map<String, Object> getVMKafkaParams(String path) {
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", "canadmin:CS-C2130#yeswecan@cankafka.northeurope.cloudapp.azure.com:9092");
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", "stream1");
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("enable.auto.commit", false);
		kafkaParams.put("security.protocol", "SSL");
		kafkaParams.put("ssl.truststore.location", path + "/client.truststore.jks");
		kafkaParams.put("ssl.truststore.password", "j!z,KS)u]g656aHY]t5P");
		kafkaParams.put("ssl.keystore.location", path + "/client.keystore.jks");
		kafkaParams.put("ssl.keystore.password", "j!z,KS)u]g656aHY]t5P");
		kafkaParams.put("ssl.key.password", "j!z,KS)u]g656aHY]t5P");
		kafkaParams.put("ssl.endpoint.identification.algorithm", "");
		return kafkaParams;
	}

	// Create a Kafka producer
	public static KafkaProducer<String, String> createProducer() {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		return new KafkaProducer<>(props);
	}

	// Structured Streaming testing
	public static void structuredStreamingTest() {
		SparkSession spark = SparkSession.builder().appName("StructuredStreaming").master("local[*]").getOrCreate();
		String path = "/Users/markorepairs :--D";
		String topic = "test.public.testi";
		HashMap<String, String> kafkaParams = new HashMap<>();
		kafkaParams.put("kafka.bootstrap.servers", "canadmin:CS-C2130#yeswecan@cankafka.northeurope.cloudapp.azure.com:9092");
		kafkaParams.put("security.protocol", "SSL");
		kafkaParams.put("ssl.truststore.location", path + "/client.truststore.jks");
		kafkaParams.put("ssl.truststore.password", "j!z,KS)u]g656aHY]t5P");
		kafkaParams.put("ssl.keystore.location", path + "/client.keystore.jks");
		kafkaParams.put("ssl.keystore.password", "j!z,KS)u]g656aHY]t5P");
		kafkaParams.put("ssl.key.password", "j!z,KS)u]g656aHY]t5P");
		kafkaParams.put("ssl.endpoint.identification.algorithm", "");

		Dataset<Row> df = readKafka(spark, topic, kafkaParams);
		debugStructuredStreaming(df);
		Dataset<Row> ds = df;
		for (String q : queries.get(topic)) {
			ds = ds.selectExpr(q);
		}
		writeKafka(ds, kafkaParams);
	}

	public static Dataset<Row> readKafka(SparkSession spark, String topic, HashMap<String, String> kafkaParams) {
		return spark.readStream().format("kafka").options(kafkaParams).option("subscribe", topic).load();
	}

	public static StreamingQuery writeKafka(Dataset<Row> ds, HashMap<String, String> kafkaParams) {
		return ds.writeStream().format("kafka").options(kafkaParams).start();
	}

	public static void debugStructuredStreaming(Dataset<Row> ds) {
		ds.writeStream().queryName("For debugging purposes").format("console").start();
	}
}
