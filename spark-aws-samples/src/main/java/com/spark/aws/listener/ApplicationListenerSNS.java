package com.spark.aws.listener;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.scheduler.ApplicationEventListener;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;
import org.apache.spark.scheduler.SparkListenerApplicationStart;

import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.sns.AmazonSNS;
import com.spark.aws.security.TemporaryCredentialsServiceLoader;
import com.spark.aws.security.utils.AWSServiceConfig;

public class ApplicationListenerSNS extends ApplicationEventListener {

	private final static Logger logger = LogManager.getLogger(ApplicationListenerSNS.class);

	AmazonSNS sns = null;

	String topicName = "SPARK-TOPIC";

	public ApplicationListenerSNS() {
		logger.debug("Loading temporary credentials...");
		TemporaryCredentialsServiceLoader serviceLoader = new TemporaryCredentialsServiceLoader();
		AWSSecurityTokenService sts = serviceLoader.loadSTSClientFromInstanceRole();
		AWSServiceConfig awsServiceConfig = AWSServiceConfig.getInstance();
		String snsServiceRoleArn = serviceLoader.prepareRoleArn(awsServiceConfig.getProperty("sns.role.name"));
		logger.trace("SnsServiceRoleARN : " + snsServiceRoleArn);
		sns = serviceLoader.loadAWSService(sts, snsServiceRoleArn, AmazonSNS.class);
	}

	@Override
	public void onApplicationEnd(SparkListenerApplicationEnd applicationEnd) {
		String message = "Application end event captured : time - " + applicationEnd.time();
		logger.trace(message);
		sns.publish("arn:aws:sns:us-east-2:" + TemporaryCredentialsServiceLoader.accountId + ":" + topicName, message);
	}

	@Override
	public void onApplicationStart(SparkListenerApplicationStart applicationStart) {
		String message = "Application start event captured : time - " + applicationStart.time();
		logger.trace(message);
		sns.publish("arn:aws:sns:us-east-2:" + TemporaryCredentialsServiceLoader.accountId + ":" + topicName, message);
	}

}
