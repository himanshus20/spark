package com.citi.example;

import java.io.StringReader;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import au.com.bytecode.opencsv.CSVReader;
import scala.Tuple2;

@SuppressWarnings({ "rawtypes", "resource", "unchecked", "serial" })
public class CitiJavaSpark {

	private final static String TOPIC = "CitiTest";
	private final static Logger logger = Logger.getLogger(CitiJavaSpark.class);
	private final static String SPARK_MASTER = "spark://168.72.195.249:7077";
	private final static String UNIX_INPUT_FILE = "/mnt/dataspace/iem/hs31777/names.csv";
	private final static String UNIX_OUTPUT_DIRECTORY = "/mnt/dataspace/iem/hs31777/Output/";

	public static void main(String[] args) {
		sparkFileReadDemo();
		sparkRddWriteToKafka();
		sparkKafkaConsumer();

	}

	private static void sparkFileReadDemo() {

		SparkConf sconf = new SparkConf();
		sconf.setAppName("Citi Java Spark Application");
		sconf.setMaster(SPARK_MASTER);
		JavaSparkContext javaSparkContext = new JavaSparkContext(sconf);

		// String csvInput = "C:\\Users\\hs31777\\Desktop\\Spark\\names.csv";
		// String outputFile = "C:\\Users\\hs31777\\Desktop\\Spark\\Output\\";

		JavaPairRDD<String, String> csvData = javaSparkContext.wholeTextFiles(UNIX_INPUT_FILE);

		JavaRDD<String[]> keyedRDD = csvData.flatMap(new ParseLine());

		JavaRDD<String[]> result = keyedRDD.filter(new Function<String[], Boolean>() {
			public Boolean call(String[] input) {
				return input[0].contains("Er");
			}
		});

		result.saveAsTextFile(UNIX_OUTPUT_DIRECTORY);
		javaSparkContext.close();
	}

	private static void sparkRddWriteToKafka() {
		SparkConf sconf = new SparkConf();
		sconf.setAppName("Citi Java Spark Kafka Producer_" + System.currentTimeMillis());
		sconf.setMaster(SPARK_MASTER);
		JavaSparkContext javaSparkContext = new JavaSparkContext(sconf);

		Properties kafkaProps = getKafkaProducerProps();

		KafkaProducer producer = new KafkaProducer<String, String>(kafkaProps);

		// String csvInput = "C:\\Users\\hs31777\\Desktop\\Spark\\names.csv";

		logger.info("Reading CSV input from :" + UNIX_INPUT_FILE);
		JavaPairRDD<String, String> csvData = javaSparkContext.wholeTextFiles(UNIX_INPUT_FILE);

		logger.info("Reading RDD from FLATMAP input from CSV of Count:" + csvData.count());
		JavaRDD<String[]> keyedRDD1 = csvData.flatMap(new ParseLine());

		JavaRDD<String[]> result = keyedRDD1.filter(new Function<String[], Boolean>() {
			public Boolean call(String[] input) {
				return true;
			}
		});

		logger.info("Taking DATA FROM RESULT FOR COUNT:" + result.count());
		List<String[]> dataCollected = result.take(10);
		logger.info("Collected Total Data: " + dataCollected.size());

		for (String[] strings : dataCollected) {
			for (String string : strings) {
				logger.info("Producing Message to Kafka :" + string + ", : Producer" + producer.toString());

				// Keys are used to determine the partition within a log to
				// which a message get's appended to. While the value is the
				// actual payload of the message
				ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, "PrecisionProducts_" + string,
						"India_" + string);
				producer.send(record);
			}
		}
		javaSparkContext.close();
		producer.flush();
		producer.close();
	}

	private static void sparkKafkaConsumer() {
		SparkConf sconf = new SparkConf();
		sconf.setAppName("Citi Java Kafka Consumer_" + System.currentTimeMillis());
		sconf.setMaster(SPARK_MASTER);

		Set<String> topics = new HashSet<String>();
		topics.add(TOPIC);
		topics.add("test");
		final Properties props = new Properties();
		setKafkaConsumerProps(props);

		JavaStreamingContext jsc = new JavaStreamingContext(sconf, Durations.seconds(30));
		/*
		 * JavaPairInputDStream<String[], String[]> stream =
		 * KafkaUtils.createDirectStream(jsc, String[].class, String[].class,
		 * DefaultDecoder.class, DefaultDecoder.class, new HashMap<>(), topics);
		 */

		callConsumer(props);

	}

	private static void callConsumer(final Properties props) {
		final Consumer<Long, String> consumer = new KafkaConsumer<>(props);

		// Subscribe to the topic.
		consumer.subscribe(Collections.singletonList(TOPIC));

		final int giveUp = 8; // Give up after 8 tries
		int noRecordsCount = 0;

		while (true) {
			final ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000);

			if (consumerRecords.count() == 0) {
				noRecordsCount++;
				if (noRecordsCount > giveUp)
					break;
				else
					continue;
			}

			logger.info("Total records in consumer are:" + consumerRecords.count());
			consumerRecords.forEach(record -> {
				logger.info("Consumer Record:" + record.key() + " " + record.value() + " " + record.partition() + " "
						+ record.offset() + "\n");
			});

			consumer.commitAsync();
		}
		consumer.close();
	}

	private static void setKafkaConsumerProps(final Properties consumerProperties) {
		consumerProperties.put("zookeeper.hosts", "168.72.195.249");
		consumerProperties.put("zookeeper.port", "2181");

		consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "168.72.195.249:2181");
		consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "CitiKafkaConsumer");
		consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

		// Create the consumer using props.
		consumerProperties.put("consumer.forcefromstart", "true");
		consumerProperties.put("max.poll.records", "100");
		consumerProperties.put("consumer.fillfreqms", "1000");
		consumerProperties.put("consumer.backpressure.enabled", "true");
	}

	private static Properties getKafkaProducerProps() {
		Properties kafkaProperties = new Properties();
		kafkaProperties.put("bootstrap.servers", "168.72.195.249:9092");
		kafkaProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		kafkaProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		return kafkaProperties;
	}

	public static class ParseLine implements FlatMapFunction<Tuple2<String, String>, String[]> {

		@Override
		public Iterator<String[]> call(Tuple2<String, String> t) throws Exception {
			CSVReader reader = new CSVReader(new StringReader(t._2()));
			return reader.readAll().iterator();
		}
	}
}
