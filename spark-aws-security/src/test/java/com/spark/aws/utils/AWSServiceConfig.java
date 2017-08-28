package com.spark.aws.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class AWSServiceConfig {

	private final static Logger logger = LogManager.getLogger(AWSServiceConfig.class);

	String propertiesFile = "aws-services-config.properties";

	Properties properties = new Properties();

	private static AWSServiceConfig instance;

	public static AWSServiceConfig getInstance() {
		if (instance == null) {
			synchronized (AWSServiceConfig.class) {
				if (instance == null) {
					instance = new AWSServiceConfig();
				}
			}
		}
		return instance;
	}

	private AWSServiceConfig() {
		InputStream inputStream = getClass().getClassLoader().getResourceAsStream(propertiesFile);
		if (inputStream != null) {
			try {
				properties.load(inputStream);
			} catch (IOException e) {
				logger.error(e, e);
			}
		}
	}

	public String getProperty(String key) {
		return properties.getProperty(key);
	}

}
