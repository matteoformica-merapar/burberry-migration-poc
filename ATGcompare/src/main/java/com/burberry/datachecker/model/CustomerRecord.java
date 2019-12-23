package com.burberry.datachecker.model;

public class CustomerRecord {

	private String customerId;
	private String hash;
	private String filename;

	public CustomerRecord(String customerId, String hash, String filename) {
		super();
		this.customerId = customerId;
		this.hash = hash;
		this.filename = filename;
	}
	
	public String getCustomerId() {
		return customerId;
	}

	public void setCustomerId(String customerId) {
		this.customerId = customerId;
	}

	public String getFilename() {
		return filename;
	}

	public void setFilename(String filename) {
		this.filename = filename;
	}

	public String getHash() {
		return hash;
	}

	public void setHash(String hash) {
		this.hash = hash;
	}

}
