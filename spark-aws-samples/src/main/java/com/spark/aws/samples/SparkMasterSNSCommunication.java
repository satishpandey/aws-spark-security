package com.spark.aws.samples;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import com.spark.aws.listener.ApplicationListenerSNS;

import scala.Tuple2;

/**
 * 
 * @author Satish Pandey
 *
 */
public class SparkMasterSNSCommunication {

	private final static Logger logger = LogManager.getLogger(SparkMasterSNSCommunication.class);

	@SuppressWarnings({ "serial", "resource", "rawtypes" })
	public static void main(String[] args) throws Exception {
		logger.info("Spark application starts ...");
		String inputFile = args[0];
		String outputFile = args[1];
		String localTest = args[2];
		// Create a Java Spark Context.
		SparkConf conf = new SparkConf().setAppName("wordCount");
		if (Boolean.parseBoolean(localTest) == true) {
			conf = conf.setMaster("local[2]").set("spark.executor.memory", "1g");
		}
		SparkContext context = new SparkContext(conf);
		context.addSparkListener(new ApplicationListenerSNS());
		JavaSparkContext sc = new JavaSparkContext(context);
		// Load our input data.
		JavaRDD<String> input = sc.textFile(inputFile);
		// Split up into words.
		JavaRDD<String> words = input.flatMap(new FlatMapFunction<String, String>() {
			public Iterator<String> call(String x) {
				return Arrays.asList(x.split(" ")).iterator();
			}
		});
		// Transform into word and count.
		JavaPairRDD<String, Integer> counts = words.mapToPair(new PairFunction<String, String, Integer>() {
			@SuppressWarnings("unchecked")
			public Tuple2<String, Integer> call(String x) {
				return new Tuple2(x, 1);
			}
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer x, Integer y) {
				return x + y;
			}
		});
		// Save the word count back out to a text file, causing evaluation.
		counts.saveAsTextFile(outputFile);
		logger.info("Spark application ends ...");
	}

}