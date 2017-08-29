package com.spark.aws.samples;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.util.EC2MetadataUtils;
import com.spark.aws.security.TemporaryCredentialsServiceLoader;
import com.spark.aws.security.utils.AWSServiceConfig;

import scala.Tuple2;

/**
 * 
 * @author Satish Pandey
 *
 */
public class SparkWorkerSQSCommunication {

	private final static Logger logger = LogManager.getLogger(SparkWorkerSQSCommunication.class);

	@SuppressWarnings({ "serial", "resource", "rawtypes" })
	public static void main(String[] args) throws Exception {
		String inputFile = args[0];
		String outputFile = args[1];
		// Create a Java Spark Context.
		SparkConf conf = new SparkConf().setAppName("wordCount");
		JavaSparkContext sc = new JavaSparkContext(conf);
		// Load our input data.
		JavaRDD<String> newRDD = sc.textFile(inputFile);
		JavaRDD<String> input = newRDD.mapPartitions(new FlatMapFunction<Iterator<String>, String>() {
			@Override
			public Iterator<String> call(Iterator<String> input) throws Exception {
				logger.debug("Loading temporary credentials...");
				TemporaryCredentialsServiceLoader serviceLoader = new TemporaryCredentialsServiceLoader();
				AWSSecurityTokenService sts = serviceLoader.loadSTSClientFromInstanceRole();
				AWSServiceConfig awsServiceConfig = AWSServiceConfig.getInstance();
				String sqsServiceRoleArn = serviceLoader.prepareRoleArn(awsServiceConfig.getProperty("sqs.role.name"));
				AmazonSQS sqs = serviceLoader.loadAWSService(sts, sqsServiceRoleArn, AmazonSQS.class);
				String queueMesg = "I am from worker machine : " + EC2MetadataUtils.getInstanceInfo().getInstanceId();
				logger.info("Sending data to SQS queue : " + queueMesg);
				sqs.sendMessage("SPARK-TEST-QUEUE", queueMesg);
				return input;
			}
		});
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
	}

}