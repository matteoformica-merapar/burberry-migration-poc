package com.burberry.datachecker.model;

public class CompareRequest {

	private String bucket;
	private String sourceTable;
	private String targetTable;
	private Boolean writeReport;

	public String getBucket() {
		return bucket;
	}

	public void setBucket(String bucket) {
		this.bucket = bucket;
	}

	public String getSourceTable() {
		return sourceTable;
	}

	public void setSourceTable(String sourceTable) {
		this.sourceTable = sourceTable;
	}

	public String getTargetTable() {
		return targetTable;
	}

	public void setTargetTable(String targetTable) {
		this.targetTable = targetTable;
	}

	public Boolean getWriteReport() {
		return writeReport;
	}

	public void setWriteReport(Boolean writeReport) {
		this.writeReport = writeReport;
	}

	@Override
	public String toString() {
		return "CompareRequest [bucket=" + bucket + ", sourceTable=" + sourceTable + ", targetTable=" + targetTable
				+ ", writeReport=" + writeReport + "]";
	}

}
