package sparkTesting;

import org.apache.spark.sql.*;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.Iterator;

public class SparkMain {

	public static void main(String[] args) {
		System.out.println("aaa");
	}


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
		SparkSession spark = SparkSession.builder().appName("Example1").getOrCreate();
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
		SparkSession spark = SparkSession.builder().appName("Example2").getOrCreate();
		Dataset<Row> socketDF = spark.readStream().format("socket").option("host", "localhost").option("port", 9999).load();
		Boolean is = socketDF.isStreaming();
		socketDF.printSchema();
		// Read all the csv files written atomically in a directory
		StructType userSchema = new StructType().add("name", "string").add("age", "integer"); // Specify the schema of the csv files
		Dataset<Row> csvDF = spark.readStream().option("sep", ";").schema(userSchema).csv("/path/to/directory"); // Equivalent to format("csv").load("/path/to/directory")
	}

	static void sparkEx3() {
		SparkSession spark = SparkSession.builder().appName("Example2").getOrCreate();
		Dataset<Row> peopleDFCsv = spark.read().format("csv").option("sep", ";").option("inferSchema", "true").option("header", "true").load("SDStable2.csv");
	}
}


/*	NOTES
 * 
 * 	
 * 
 * 
 * 
 * 
 * */
