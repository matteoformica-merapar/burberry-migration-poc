package com.burberry.datachecker.model;

public class DataCopyRequest {

	@Override
	public String toString() {
		return "DataCopyRequest [bucketName=" + bucketName + ", processFolderName=" + processFolderName + ", prefix="
				+ prefix + ", delay=" + delay + "]";
	}

	private String bucketName;
	private String processFolderName;
	private String prefix;
	private String delay;

	public String getBucketName() {
		return bucketName;
	}

	public void setBucketName(String bucketName) {
		this.bucketName = bucketName;
	}

	public String getProcessFolderName() {
		return processFolderName;
	}

	public void setProcessFolderName(String processFolderName) {
		this.processFolderName = processFolderName;
	}

	public String getPrefix() {
		return prefix;
	}

	public void setPrefix(String prefix) {
		this.prefix = prefix;
	}

	public String getDelay() {
		return delay;
	}

	public void setDelay(String delay) {
		this.delay = delay;
	}

}
