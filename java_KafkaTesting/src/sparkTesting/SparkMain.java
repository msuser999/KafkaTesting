package sparkTesting;

// // // structured streaming //
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.api.java.function.FlatMapFunction;
// // //
// // // streaming //
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.clients.producer.*;
// // // 
import java.util.*;

public class SparkMain {

	public static void main(String[] args) throws StreamingQueryException {
		System.setProperty("hadoop.home.dir", "C:/Hadoop/"); // location of /bin/winutils.exe (Hadoop on windows...)
		System.out.println("start");
		sparkTest2();
		System.out.println("end");
	}

	// SPARK STRUCTURED STREAMING
	public static void sparkTest1() {
		// https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/SparkSession.html
		SparkSession spark = SparkSession.builder().appName("Ex1").master("local").getOrCreate();

		// Read from socket (create Dataframe)
		/*	.readStream() => https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/streaming/DataStreamReader.html
		 * 	.format(String src) - Specify the data format
		 * 	.option(String key, String/long/double/boolean value) - Adds an input option for the underlying data source
		 * 	.load() - Loads input data stream in as a DataFrame, for data streams that don't require a path 	
		 *  	=> https://spark.apache.org/docs/2.4.0/api/java/index.html?org/apache/spark/sql/Dataset.html		*/
		Dataset<Row> lines = spark.readStream().format("socket").option("host", "localhost").option("port", 9999).load();

		// Split the lines into words
		/* .as(Encoder<U> encoder) -  Returns a new Dataset where each record has been mapped on to the specified type
		 * .flatmap(FlatMapFunction<T, U> f, Encoder<U> encoder) -  (Java-specific) Returns a new Dataset by first applying a function to all elements of this Dataset, and then flattening the results		*/
		Dataset<String> words = lines.as(Encoders.STRING()).flatMap((FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(), Encoders.STRING());
		Dataset<Row> wordCounts = words.groupBy("value").count(); // running word count

		// Start running the query that prints the running counts to the console
		/*	.writeStream() => http://spark.apache.org/docs/2.4.0/api/java/index.html?org/apache/spark/sql/streaming/DataStreamWriter.html
		 * 	.outputMode(String outputMode) - Specifies how data of a streaming DataFrame/Dataset is written to a streaming sink
		 * 	.format(String src) - Specifies the underlying output data source
		 * 	.start() - Starts the execution of the streaming query, which will continually output results to the given path as new data arrives		*/
		StreamingQuery query = wordCounts.writeStream().outputMode("complete").format("console").start();

		try {
			query.awaitTermination();
		} catch (StreamingQueryException e) {
			e.printStackTrace();
		}
	}

	// SPARK STREAMING 
	public static void sparkTest2() {
		// Configuration for Kafka consumer
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", "localhost:9092");
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", "stream1");
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("enable.auto.commit", false);
		Collection<String> topics = Arrays.asList("mytopic");

		/*	https://spark.apache.org/docs/latest/api/java/org/apache/spark/SparkConf.html
		 * 	.setAppName(String name) - Set a name for your application.
		 * 	.setMaster(String master) - The master URL to connect to, such as "local" to run locally with one thread, 
		 * 		"local[4]" to run locally with 4 cores, or "spark://master:7077" to run on a Spark standalone cluster. 		*/
		SparkConf conf = new SparkConf().setAppName("sparkEx2").setMaster("local"); // "local" or "local[*]" ?

		/*	https://spark.apache.org/docs/latest/api/java/index.html?org/apache/spark/streaming/api/java/JavaStreamingContext.html
		 	A Java-friendly version of StreamingContext which is the main entry point for Spark Streaming functionality. It provides methods to create JavaDStream and JavaPairDStream from input sources	*/
		JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(1000));

		JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent(), ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));

		JavaDStream<String> lines = stream.map(r -> r.value().toString()); // ?? difference between DStream and InputDStream ??
		lines.print();

		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		KafkaProducer<String, String> prod = new KafkaProducer<>(props);

		lines.foreachRDD(r -> {
			prod.send(new ProducerRecord<String, String>("mytopic", r + " @ " + java.time.Instant.now()));	// not correct, r is not hte ntext line !!
		});

		ssc.start();
		try {
			ssc.awaitTermination();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}


/*	NOTES
 * 
 */

/*// OLD OLD OLD 
static void kafkaEx1() {
	SparkSession spark = SparkSession.builder().appName("KafkaExample1").getOrCreate();
	// Reading from Kafka
	Dataset<Row> df = spark.readStream().format("kafka").option("kafka.bootstrap.servers", "host1:port1,host2:port2").option("subscribe", "topic1").load(); // put "topic1,topic2" to sub. to multiple topics
	df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)"); // sql query, conversion...
	// other things: sub. to pattern, specify offsets (explicit and earliest, latest)...
	// Writing to Kafka
	StreamingQuery ds = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").writeStream().format("kafka").option("kafka.bootstrap.servers", "host1:port1,host2:port2").option("topic", "topic1").start();
}
static void sparkEx1() throws StreamingQueryException {
	SparkSession spark = SparkSession.builder().appName("Example1").master("local[*]").getOrCreate();
	// Create DataFrame representing the stream of input lines from connection to localhost:9999
	Dataset<Row> lines = spark.readStream().format("socket").option("host", "localhost").option("port", 9999).load();
	// Split the lines into words
	Dataset<String> words = lines.as(Encoders.STRING()).flatMap((FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(), Encoders.STRING());
	// Generate running word count
	Dataset<Row> wordCounts = words.groupBy("value").count();
	// Start running the query that prints the running counts to the console
	StreamingQuery query = wordCounts.writeStream().outputMode("complete").format("console").start();
	query.awaitTermination();
}
static void sparkEx2() {
	SparkSession spark = SparkSession.builder().appName("Example2").master("local[*]").getOrCreate();
	Dataset<Row> socketDF = spark.readStream().format("socket").option("host", "localhost").option("port", 9999).load();
	StructType userSchema = new StructType().add("name", "string").add("age", "integer"); // Specify the schema of the csv files
	Dataset<Row> csvDF = spark.readStream().option("sep", ";").schema(userSchema).csv("java_kafkaTesting/SDStable2"); // Equivalent to format("csv").load("/path/to/directory")
}
*/

