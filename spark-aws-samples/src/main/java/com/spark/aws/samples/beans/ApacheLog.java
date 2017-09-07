package com.spark.aws.samples.beans;

import java.util.Date;

public class ApacheLog {

	private String remoteHost;
	private String remoteIdentity;
	private String localIdentity;
	private Date requestTime;
	private String method;
	private String endpoint;
	private String protocol;
	private Integer responseCode;
	private Long contentSize;

	public ApacheLog(String remoteHost, String remoteIdentity, String localIdentity, Date requestTime, String method,
			String endpoint, String protocol, Integer responseCode, Long contentSize) {
		super();
		this.remoteHost = remoteHost;
		this.remoteIdentity = remoteIdentity;
		this.localIdentity = localIdentity;
		this.requestTime = requestTime;
		this.method = method;
		this.endpoint = endpoint;
		this.protocol = protocol;
		this.responseCode = responseCode;
		this.contentSize = contentSize;
	}

	public String getRemoteHost() {
		return remoteHost;
	}

	public void setRemoteHost(String remoteHost) {
		this.remoteHost = remoteHost;
	}

	public String getRemoteIdentity() {
		return remoteIdentity;
	}

	public void setRemoteIdentity(String remoteIdentity) {
		this.remoteIdentity = remoteIdentity;
	}

	public String getLocalIdentity() {
		return localIdentity;
	}

	public void setLocalIdentity(String localIdentity) {
		this.localIdentity = localIdentity;
	}

	public Date getRequestTime() {
		return requestTime;
	}

	public void setRequestTime(Date requestTime) {
		this.requestTime = requestTime;
	}

	public String getMethod() {
		return method;
	}

	public void setMethod(String method) {
		this.method = method;
	}

	public String getEndpoint() {
		return endpoint;
	}

	public void setEndpoint(String endpoint) {
		this.endpoint = endpoint;
	}

	public String getProtocol() {
		return protocol;
	}

	public void setProtocol(String protocol) {
		this.protocol = protocol;
	}

	public Integer getResponseCode() {
		return responseCode;
	}

	public void setResponseCode(Integer responseCode) {
		this.responseCode = responseCode;
	}

	public Long getContentSize() {
		return contentSize;
	}

	public void setContentSize(Long contentSize) {
		this.contentSize = contentSize;
	}
}
