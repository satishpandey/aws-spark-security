package com.spark.aws.samples;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.spark.aws.security.TemporaryCredentialsServiceLoader;
import com.spark.aws.security.utils.AWSServiceConfig;

/**
 * 
 * @author Satish Pandey
 *
 */
public class SparkMasterS3Communication {

	private final static Logger logger = LogManager.getLogger(SparkMasterS3Communication.class);

	/*
	 * Run spark job in remote cluster mode
	 * 
	 * ~/spark-2.2.0-bin-hadoop2.7/bin/spark-submit --master
	 * spark://ip-172-31-41-118.us-east-2.compute.internal:7077 --class
	 * com.spark.summit.samples.WordCountS3
	 * ~/spark-summit-samples-0.0.1-SNAPSHOT.jar s3a://spark.data.com/index.html
	 * ~/output
	 */

	@SuppressWarnings({ "serial", "resource", "rawtypes" })
	public static void main(String[] args) throws Exception {
		String inputFile = args[0];
		String outputFile = args[1];
		String roleName = AWSServiceConfig.getInstance().getProperty("s3.role.name");
		TemporaryCredentialsServiceLoader serviceLoader = new TemporaryCredentialsServiceLoader();
		AWSSecurityTokenService sts = serviceLoader.loadSTSClientFromInstanceRole();
		String s3ServiceRoleArn = serviceLoader.prepareRoleArn(roleName);
		String roleSessionName = roleName + "Session";
		STSAssumeRoleSessionCredentialsProvider credentialsProvider = serviceLoader.loadRoleBasedCredentials(sts,
				s3ServiceRoleArn, roleSessionName);
		// Create a Java Spark Context.
		SparkConf conf = new SparkConf().setAppName("SparkMasterS3Communication");
		JavaSparkContext sc = new JavaSparkContext(conf);
		logger.info("Using temporary credencials ...");
		Configuration configuration = sc.hadoopConfiguration();
		configuration.set("fs.s3a.access.key", credentialsProvider.getCredentials().getAWSAccessKeyId());
		configuration.set("fs.s3a.secret.key", credentialsProvider.getCredentials().getAWSSecretKey());
		configuration.set("fs.s3a.session.token", credentialsProvider.getCredentials().getSessionToken());
		configuration
				.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider");
		// Load input data.
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
	}
}