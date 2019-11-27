package com.burberry.datachecker.model;

public class ErrorDetail {
	private String message;
	private String customerId;
	private String sourceFileName;
	private String targetFileName;

	public String getTargetFileName() {
		return targetFileName;
	}

	public void setTargetFileName(String targetFileName) {
		this.targetFileName = targetFileName;
	}

	public ErrorDetail(String message, String customerId, String sourceFileName, String targetFileName) {
		super();
		this.message = message;
		this.customerId = customerId;
		this.sourceFileName = sourceFileName;
		this.targetFileName = targetFileName;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public String getCustomerId() {
		return customerId;
	}

	public void setCustomerId(String customerId) {
		this.customerId = customerId;
	}

	public String getFileName() {
		return sourceFileName;
	}

	public void setFileName(String fileName) {
		this.sourceFileName = fileName;
	}

}
