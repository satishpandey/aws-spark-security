package com.spark.aws.samples;

import java.util.Arrays;
import java.util.Date;
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

import scala.Tuple2;

import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.util.EC2MetadataUtils;
import com.amazonaws.util.json.Jackson;
import com.spark.aws.security.TemporaryCredentialsServiceLoader;
import com.spark.aws.security.utils.AWSServiceConfig;

/**
 * 
 * @author Satish Pandey
 *
 */
public class SparkWorkerSQSCommunication {

	private final static Logger logger = LogManager.getLogger(SparkWorkerSQSCommunication.class);
	private final static String accountId = EC2MetadataUtils.getInstanceInfo().getAccountId();
	// RoleArn template
	private final static String roleArnTemplate = String.format("arn:aws:iam::%s:role/%s", accountId, "%s");

	@SuppressWarnings({ "serial", "rawtypes" })
	public static void main(String[] args) throws Exception {
		String inputFile = args[0];
		String outputFile = args[1];
		SparkConf conf = new SparkConf().setAppName("wordCount");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> newRDD = sc.textFile(inputFile);
		JavaRDD<String> input = newRDD.mapPartitions(new FlatMapFunction<Iterator<String>, String>() {
			@Override
			public Iterator<String> call(Iterator<String> input) throws Exception {
				logger.debug("Loading temporary credentials...");
				String roleName = "S3TempAccessRole";
				String roleSessionName = roleName + "Session";
				String roleArn = String.format(roleArnTemplate, roleName);
				InstanceProfileCredentialsProvider credentialsProvider = new InstanceProfileCredentialsProvider(true);
				AWSSecurityTokenService sts = AWSSecurityTokenServiceClientBuilder.standard()
						.withCredentials(credentialsProvider).build();
				STSAssumeRoleSessionCredentialsProvider provider = new STSAssumeRoleSessionCredentialsProvider.Builder(
						roleArn, roleSessionName).withStsClient(sts).build();
				AmazonSQS sqs = AmazonSQSClientBuilder.standard().withCredentials(provider).build();
				ExecutorInfo executorInfo = new ExecutorInfo(EC2MetadataUtils.getInstanceId(), EC2MetadataUtils
						.getPrivateIpAddress(), EC2MetadataUtils.getLocalHostName(), EC2MetadataUtils.getMacAddress(),
						System.getenv().get("SPARK_EXECUTOR_DIRS"), System.getProperty("user.dir"), System.getenv()
								.get("SPARK_LOG_URL_STDOUT"), System.getenv().get("SPARK_LOG_URL_STDERR"));
				String sqsMessage = Jackson.toJsonString(executorInfo);
				logger.info("Sending data to SQS queue : " + sqsMessage);
				sqs.sendMessage("SPARK-TEST-QUEUE", sqsMessage);
				return input;
			}
		});
		JavaRDD<String> words = input.flatMap(new FlatMapFunction<String, String>() {
			public Iterator<String> call(String x) {
				return Arrays.asList(x.split(" ")).iterator();
			}
		});
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
		counts.saveAsTextFile(outputFile);
		sc.close();
		System.exit(0);
	}

	static class ExecutorInfo {
		private Date createdDateTime;
		private String instanceId;
		private String privateIp;
		private String localAddress;
		private String macAddress;
		private String executorDir;
		private String userDir;
		private String logUrlStdout;
		private String logUrlStderr;

		public ExecutorInfo() {
			createdDateTime = new Date();
		}

		public ExecutorInfo(String instanceId, String privateIp, String localAddress, String macAddress,
				String executorDir, String userDir, String logUrlStdout, String logUrlStderr) {
			this();
			this.instanceId = instanceId;
			this.privateIp = privateIp;
			this.localAddress = localAddress;
			this.macAddress = macAddress;
			this.executorDir = executorDir;
			this.userDir = userDir;
			this.logUrlStdout = logUrlStdout;
			this.logUrlStderr = logUrlStderr;
		}

		public String getInstanceId() {
			return instanceId;
		}

		public void setInstanceId(String instanceId) {
			this.instanceId = instanceId;
		}

		public String getPrivateIp() {
			return privateIp;
		}

		public void setPrivateIp(String privateIp) {
			this.privateIp = privateIp;
		}

		public String getLocalAddress() {
			return localAddress;
		}

		public void setLocalAddress(String localAddress) {
			this.localAddress = localAddress;
		}

		public String getMacAddress() {
			return macAddress;
		}

		public void setMacAddress(String macAddress) {
			this.macAddress = macAddress;
		}

		public String getExecutorDir() {
			return executorDir;
		}

		public void setExecutorDir(String executorDir) {
			this.executorDir = executorDir;
		}

		public String getUserDir() {
			return userDir;
		}

		public void setUserDir(String userDir) {
			this.userDir = userDir;
		}

		public String getLogUrlStdout() {
			return logUrlStdout;
		}

		public void setLogUrlStdout(String logUrlStdout) {
			this.logUrlStdout = logUrlStdout;
		}

		public String getLogUrlStderr() {
			return logUrlStderr;
		}

		public void setLogUrlStderr(String logUrlStderr) {
			this.logUrlStderr = logUrlStderr;
		}

		public Date getCreatedDateTime() {
			return createdDateTime;
		}

	}
}