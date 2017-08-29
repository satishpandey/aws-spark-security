package com.spark.aws.security.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.CreateTableResult;
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteTableResult;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.DeleteBucketRequest;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.model.CreateTopicRequest;
import com.amazonaws.services.sns.model.CreateTopicResult;
import com.amazonaws.services.sns.model.DeleteTopicRequest;
import com.amazonaws.services.sns.model.DeleteTopicResult;
import com.amazonaws.services.sns.model.ListTopicsResult;
import com.amazonaws.services.sns.model.Topic;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.DeleteQueueResult;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.amazonaws.services.sqs.model.ListQueuesResult;
import com.amazonaws.services.sqs.model.QueueDeletedRecentlyException;
import com.amazonaws.util.EC2MetadataUtils;
import com.amazonaws.util.StringUtils;
import com.spark.aws.security.TemporaryCredentialsServiceLoader;
import com.spark.aws.security.utils.AWSServiceConfig;

/**
 * Assume Role Credentials Test
 * 
 * @author Satish Pandey
 */
public class AssumeRoleCredentialsTest {

	private final static Logger logger = LogManager.getLogger(AssumeRoleCredentialsTest.class);
	private final static Region region = Region.getRegion(Regions.fromName(EC2MetadataUtils.getInstanceInfo()
			.getRegion()));

	public final static AWSServiceConfig awsServiceConfig = AWSServiceConfig.getInstance();

	/**
	 * Main method
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			int repeatTestWithFreshCredentials = 1;
			if (args != null && args.length > 0) {
				try {
					int count = Integer.parseInt(args[0]);
					if (count > 1) {
						repeatTestWithFreshCredentials = count;
					} else {
						logger.warn(String.format("Invalid repeat count: %d.. Using default count 1.", count));
					}
				} catch (NumberFormatException exception) {
					logger.warn(exception, exception);
				}
			}

			// Loading required AWS services for testing
			TemporaryCredentialsServiceLoader serviceLoader = new TemporaryCredentialsServiceLoader();
			AWSSecurityTokenService sts = serviceLoader.loadSTSClientFromInstanceRole();
			String s3ServiceRoleArn = serviceLoader.prepareRoleArn(awsServiceConfig.getProperty("s3.role.name"));
			String snsServiceRoleArn = serviceLoader.prepareRoleArn(awsServiceConfig.getProperty("sns.role.name"));
			String sqsServiceRoleArn = serviceLoader.prepareRoleArn(awsServiceConfig.getProperty("sqs.role.name"));
			String dynamodbServiceRoleArn = serviceLoader.prepareRoleArn(awsServiceConfig
					.getProperty("dynamodb.role.name"));
			AmazonS3 s3 = serviceLoader.loadAWSService(sts, s3ServiceRoleArn, AmazonS3.class);
			AmazonSNS sns = serviceLoader.loadAWSService(sts, snsServiceRoleArn, AmazonSNS.class);
			AmazonSQS sqs = serviceLoader.loadAWSService(sts, sqsServiceRoleArn, AmazonSQS.class);
			AmazonDynamoDB dynamodb = serviceLoader.loadAWSService(sts, dynamodbServiceRoleArn, AmazonDynamoDB.class);

			// Test AWS services
			AssumeRoleCredentialsTest test = new AssumeRoleCredentialsTest();
			for (int index = 0; index < repeatTestWithFreshCredentials; index++) {
				if (index != 0) {
					long waitTime = 1000l * TemporaryCredentialsServiceLoader.stsRoleSessionDurationSeconds;
					try {
						logger.debug(String.format("Going to sleep for %d miliseconds", waitTime));
						Thread.sleep(waitTime);
					} catch (Exception e) {
						logger.error(e, e);
					}
				}
				test.testS3Operations(s3, awsServiceConfig.getProperty("s3.bucket.name"));
				test.testSNSOperations(sns, awsServiceConfig.getProperty("sns.topic.name"));
				test.testSQSOperations(sqs, awsServiceConfig.getProperty("sqs.queue.name"));
				test.testDynamoDBOperations(dynamodb, awsServiceConfig.getProperty("dynamodb.table.name"));
			}
		} catch (Exception exception) {
			logger.error(exception, exception);
			System.exit(-1);
		} catch (AssertionError error) {
			logger.error(error, error);
			System.exit(-2);
		}
		System.exit(0);
	}

	private void testS3Operations(AmazonS3 s3, String bucketName) {
		boolean exist = s3.doesBucketExist(bucketName);
		if (exist) {
			logger.info("Bucket already exist .. cleaning it");
			DeleteBucketRequest deleteBucketRequest = new DeleteBucketRequest(bucketName);
			s3.deleteBucket(deleteBucketRequest);
		}
		assertFalse("Bucket already available in S3 store", s3.doesBucketExist(bucketName));
		CreateBucketRequest createBucketRequest = new CreateBucketRequest(bucketName, region.getName());
		Bucket bucket = s3.createBucket(createBucketRequest);
		assertSame("Bucket name doesn't match!", bucketName, bucket.getName());
		logger.info("New Bucket created : " + bucket);
		assertTrue("Created bucket not found in response", s3.doesBucketExist(bucketName));
		String bucketLocation = s3.getBucketLocation(bucketName);
		logger.info(String.format("Bucket location : %s", bucketLocation));
		assertFalse("Invalid Bucket location", StringUtils.isNullOrEmpty(bucketLocation));
		DeleteBucketRequest deleteBucketRequest = new DeleteBucketRequest(bucketName);
		s3.deleteBucket(deleteBucketRequest);
		assertFalse("Bucket not deleted from S3 store", s3.doesBucketExist(bucketName));
		logger.info("Bucket deleted successfully");
	}

	private void testSNSOperations(AmazonSNS sns, String snsTopic) {
		CreateTopicRequest createTopicRequest = new CreateTopicRequest(snsTopic);
		CreateTopicResult createTopicResult = sns.createTopic(createTopicRequest);
		logger.info("Create Topic result: " + createTopicResult);
		ListTopicsResult listTopicResult = sns.listTopics();
		assertTrue("No topics found in list", !listTopicResult.getTopics().isEmpty());
		logger.info("ListTopic Result: " + listTopicResult);
		boolean topicExist = false;
		for (Topic topic : listTopicResult.getTopics()) {
			logger.trace(String.format("Topic: %s", topic));
			if (topic.getTopicArn().equals(createTopicResult.getTopicArn())) {
				topicExist = true;
				break;
			}
		}
		assertTrue("Topic not exist.. create operation failed", topicExist);
		DeleteTopicRequest deleteTopicRequest = new DeleteTopicRequest(createTopicResult.getTopicArn());
		DeleteTopicResult deleteTopicResult = sns.deleteTopic(deleteTopicRequest);
		logger.info("DeleteTopicResult : " + deleteTopicResult);
	}

	private void testSQSOperations(AmazonSQS sqs, String queueName) {
		CreateQueueRequest createQueueRequest = new CreateQueueRequest(queueName);
		CreateQueueResult createQueueResult = null;
		try {
			createQueueResult = sqs.createQueue(createQueueRequest);
		} catch (QueueDeletedRecentlyException exception) {
			sleep(1000 * 60l);
			createQueueResult = sqs.createQueue(createQueueRequest);
		}
		logger.info("Create queue result: " + createQueueResult);
		ListQueuesResult queuesResult = sqs.listQueues();
		List<String> queueUrls = queuesResult.getQueueUrls();
		assertTrue("No queue url found in list", !queueUrls.isEmpty());
		logger.info(String.format("Queues : %s", queueUrls));
		GetQueueUrlResult queueUrlResult = sqs.getQueueUrl(queueName);
		assertEquals("Queue Url not matched!", createQueueResult.getQueueUrl(), queueUrlResult.getQueueUrl());
		DeleteQueueRequest deleteQueueRequest = new DeleteQueueRequest(createQueueResult.getQueueUrl());
		DeleteQueueResult deleteQueueResult = sqs.deleteQueue(deleteQueueRequest);
		logger.info("DeleteQueueResult : " + deleteQueueResult);
	}

	private void testDynamoDBOperations(AmazonDynamoDB dynamodb, String tableName) {
		try {
			DescribeTableResult result = dynamodb.describeTable(tableName);
			TableDescription table = result.getTable();
			logger.debug(String.format("%s Table already exist", table));
			DeleteTableRequest deleteTableRequest = new DeleteTableRequest(tableName);
			DeleteTableResult deleteTableResult = dynamodb.deleteTable(deleteTableRequest);
			logger.info("DeleteTableResult : " + deleteTableResult);
			sleep(1000 * 60l);
		} catch (ResourceNotFoundException exception) {
			logger.trace(exception);
		}
		ListTablesResult listTables = dynamodb.listTables();
		logger.debug("DynamoDB tables: " + listTables);
		for (String table : listTables.getTableNames()) {
			logger.debug(String.format("Table: %s", table));
			assertNotSame("Table alrady exist", tableName, table);
		}
		List<KeySchemaElement> elements = new ArrayList<KeySchemaElement>();
		String column1 = awsServiceConfig.getProperty("dynamodb.table.column1");
		elements.add(new KeySchemaElement(column1, KeyType.HASH));
		ArrayList<AttributeDefinition> attributeDefinitions = new ArrayList<AttributeDefinition>();
		attributeDefinitions.add(new AttributeDefinition().withAttributeName(column1).withAttributeType("S"));
		CreateTableRequest createTableRequest = new CreateTableRequest()
				.withTableName(tableName)
				.withKeySchema(elements)
				.withProvisionedThroughput(
						new ProvisionedThroughput().withReadCapacityUnits(10l).withWriteCapacityUnits(5l))
				.withAttributeDefinitions(attributeDefinitions);
		CreateTableResult result = dynamodb.createTable(createTableRequest);
		logger.info(String.format("CreateTableResult: %s", result));
		assertEquals("Table name doesn't match!", tableName, result.getTableDescription().getTableName());
		sleep(1000 * 60l);
		DeleteTableRequest deleteTableRequest = new DeleteTableRequest(tableName);
		DeleteTableResult deleteTableResult = dynamodb.deleteTable(deleteTableRequest);
		assertEquals("Table not deleted", tableName, deleteTableResult.getTableDescription().getTableName());
		logger.info("DeleteTableResult : " + deleteTableResult);
	}

	private void sleep(long miliseconds) {
		try {
			logger.warn(String.format("Going to sleep for %d miliseconds", miliseconds));
			Thread.sleep(miliseconds);
		} catch (Exception e) {
			logger.error(e, e);
		}
	}
}
