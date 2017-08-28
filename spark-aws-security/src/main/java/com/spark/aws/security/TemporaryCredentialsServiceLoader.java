package com.spark.aws.security;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Method;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.util.EC2MetadataUtils;
import com.amazonaws.util.StringUtils;

/**
 * Assume Role Credentials
 * 
 * @author Satish Pandey
 */
public class TemporaryCredentialsServiceLoader {

	private final static Logger logger = LogManager.getLogger(TemporaryCredentialsServiceLoader.class);

	public final static String accountId = EC2MetadataUtils.getInstanceInfo().getAccountId();

	// RoleArn template
	private final static String roleArnTemplate = String.format("arn:aws:iam::%s:role/%s", accountId, "%s");

	// Assume Role session expiry
	public final static int stsRoleSessionDurationSeconds = 900;

	/**
	 * Load STS Client using Instance MetaData Temporary Credentials
	 * 
	 * @return
	 */
	public AWSSecurityTokenService loadSTSClientFromInstanceRole() {
		logger.trace("Loading Temporary Credentials from Instance MetaData....");
		InstanceProfileCredentialsProvider credentialsProvider = new InstanceProfileCredentialsProvider(true);
		assertNotNull("No credentials loaded from metadata", credentialsProvider.getCredentials());
		logger.debug(String.format("Returning Temporary credencials : %s",
				displayCredentials(credentialsProvider.getCredentials())));
		AWSSecurityTokenService sts = AWSSecurityTokenServiceClientBuilder.standard()
				.withCredentials(credentialsProvider).build();
		assertNotNull("STS service not loaded", sts);
		logger.info(String.format("Returning STS service %s", sts));
		return sts;
	}

	/**
	 * Load AWS Service using STS Assume Role with default role session name
	 * generated using aws service class and instance id.
	 * 
	 * @param sts
	 * @param roleArn
	 * @param service
	 * @return
	 */
	public <T> T loadAWSService(AWSSecurityTokenService sts, String roleArn, Class<T> service) {
		String roleSessionName = service.getSimpleName() + "-" + EC2MetadataUtils.getInstanceId();
		logger.trace(String.format("Created RoleSessionName : %s", roleSessionName));
		return loadAWSService(sts, roleArn, roleSessionName, service);
	}

	/**
	 * Load AWS Service using STS Assume Role
	 * 
	 * @param sts
	 * @param roleArn
	 * @param roleSessionName
	 * @param service
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public <T> T loadAWSService(AWSSecurityTokenService sts, String roleArn, String roleSessionName, Class<T> service) {
		String serviceName = service.getName();
		logger.trace(String.format("Loading Service with RoleArn(%s), RoleSessionName(%s), Service(%s)", roleArn,
				roleSessionName, service.getName()));
		T t = null;
		try {
			Class<?> clientBuilder = Class.forName(serviceName + "ClientBuilder", true,
					TemporaryCredentialsServiceLoader.class.getClassLoader());
			logger.trace(String.format("Loading ClientBuilder : %s", clientBuilder));
			Method standard = clientBuilder.getMethod("standard", new Class[] {});
			logger.trace(String.format("Method : %s", standard));
			Object obj = standard.invoke(null);
			logger.trace(String.format("Method invocation returned Object : %s", obj));
			Method withCredentials = clientBuilder.getMethod("withCredentials",
					new Class[] { AWSCredentialsProvider.class });
			logger.trace(String.format("Method : %s", withCredentials));
			STSAssumeRoleSessionCredentialsProvider provider = this.loadRoleBasedCredentials(sts, roleArn,
					roleSessionName);
			obj = withCredentials.invoke(obj, provider);
			logger.trace(String.format("Method invocation returned Object : %s", obj));
			Method build = clientBuilder.getMethod("build", new Class[] {});
			logger.trace(String.format("Method : %s", build));
			Object client = build.invoke(obj);
			logger.trace(String.format("Method invocation returned Object : %s", client));
			t = (T) client;
		} catch (ClassNotFoundException e) {
			logger.error(e, e);
		} catch (NoSuchMethodException e) {
			logger.error(e, e);
		} catch (Exception e) {
			logger.error(e, e);
		}
		assertNotNull("Invalid service .. NULL", t);
		logger.debug(String.format("Returning service : %s", t));
		return t;
	}

	/**
	 * Load STS Assume role credentials.
	 * 
	 * @param sts
	 * @param roleArn
	 * @param roleSessionName
	 * @return
	 */
	public STSAssumeRoleSessionCredentialsProvider loadRoleBasedCredentials(AWSSecurityTokenService sts,
			String roleArn, String roleSessionName) {
		assertTrue("Invalid arn : " + roleArn, roleArn.contains(accountId));
		assertFalse("Invalid RoleSessionName", StringUtils.isNullOrEmpty(roleSessionName));
		logger.trace(String.format("Loading Credentials with RoleArn(%s), RoleSessionName(%s)", roleArn,
				roleSessionName));
		STSAssumeRoleSessionCredentialsProvider provider = new STSAssumeRoleSessionCredentialsProvider.Builder(roleArn,
				roleSessionName).withStsClient(sts).withRoleSessionDurationSeconds(stsRoleSessionDurationSeconds)
				.build();
		logger.debug(String.format("AssumeRoleSessionCredentials : %s", displayCredentials(provider.getCredentials())));
		return provider;
	}

	/**
	 * Prepares Role ARN
	 * 
	 * @param roleName
	 * @return
	 */
	public String prepareRoleArn(String roleName) {
		String roleArn = String.format(roleArnTemplate, roleName);
		return roleArn;
	}

	/**
	 * Display and return AWS credentials as a String
	 * 
	 * @param credentials
	 * @return
	 */
	private static String displayCredentials(AWSCredentials credentials) {
		StringBuilder builder = new StringBuilder();
		assertFalse("Invalid AccessKey", StringUtils.isNullOrEmpty(credentials.getAWSAccessKeyId()));
		builder.append("{ Key: ").append(credentials.getAWSAccessKeyId());
		assertFalse("Invalid Secret", StringUtils.isNullOrEmpty(credentials.getAWSSecretKey()));
		builder.append(", Secret: ").append(credentials.getAWSSecretKey());
		if (credentials instanceof BasicSessionCredentials) {
			assertFalse("Invalid SessionToken",
					StringUtils.isNullOrEmpty(((BasicSessionCredentials) credentials).getSessionToken()));
			builder.append(", Token: ").append(((BasicSessionCredentials) credentials).getSessionToken());
		}
		builder.append("}");
		logger.trace(String.format("Returing credentials: %s", builder.toString()));
		return builder.toString();
	}

}
