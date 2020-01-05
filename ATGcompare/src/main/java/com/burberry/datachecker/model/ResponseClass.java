package com.burberry.datachecker.model;

import java.util.List;

public class ResponseClass {
	
	private int atgRecordFound;
	private int atgRecordFoundMatching;
	private int atgRecordNotFound;
	private int atgRecordFoundNotMatching;
	private List<ErrorDetail> messages;

	public ResponseClass(int atgRecordFound, int atgRecordFoundMatching, int atgRecordNotFound,
			int atgRecordFoundNotMatching, List<ErrorDetail> messages) {
		super();
		this.atgRecordFound = atgRecordFound;
		this.atgRecordFoundMatching = atgRecordFoundMatching;
		this.atgRecordNotFound = atgRecordNotFound;
		this.atgRecordFoundNotMatching = atgRecordFoundNotMatching;
		this.messages = messages;
	}

	public int getAtgRecordFound() {
		return atgRecordFound;
	}

	public void setAtgRecordFound(int atgRecordFound) {
		this.atgRecordFound = atgRecordFound;
	}

	public int getAtgRecordFoundMatching() {
		return atgRecordFoundMatching;
	}

	public void setAtgRecordFoundMatching(int atgRecordFoundMatching) {
		this.atgRecordFoundMatching = atgRecordFoundMatching;
	}

	public int getAtgRecordNotFound() {
		return atgRecordNotFound;
	}

	public void setAtgRecordNotFound(int atgRecordNotFound) {
		this.atgRecordNotFound = atgRecordNotFound;
	}

	public int getAtgRecordFoundNotMatching() {
		return atgRecordFoundNotMatching;
	}

	public void setAtgRecordFoundNotMatching(int atgRecordFoundNotMatching) {
		this.atgRecordFoundNotMatching = atgRecordFoundNotMatching;
	}

	public List<ErrorDetail> getMessages() {
		return messages;
	}

	public void setMessages(List<ErrorDetail> messages) {
		this.messages = messages;
	}

	@Override
	public String toString() {
		return "ResponseClass [atgRecordFound=" + atgRecordFound + ", atgRecordFoundMatching=" + atgRecordFoundMatching
				+ ", atgRecordNotFound=" + atgRecordNotFound + ", atgRecordFoundNotMatching="
				+ atgRecordFoundNotMatching + "]";
	}
}