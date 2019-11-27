package com.burberry.datachecker;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.http.entity.ContentType;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;

public class DataCompareHandler implements RequestHandler<Object, String> {

	AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();
	AmazonS3 s3client = AmazonS3ClientBuilder.standard().build();

	int recordNotFound = 0;
	int recordFound = 0;
	
	StringBuilder sb = new StringBuilder("");
	Formatter fmt = new Formatter(sb);
		
	String bucketName = System.getenv("BUCKET_NAME");

	@Override
	public String handleRequest(Object input, Context context) {
		
		if(!s3client.doesBucketExistV2(bucketName)) {
			System.out.format("Bucket name is not available: %s", bucketName);
		    System.exit(1);
		}
				
		scanATGtable();
		
		fmt.format("Record found: %s \n", recordFound);
		fmt.format("Record not found: %s \n", recordNotFound);
		System.out.format("Record found: %s \n", recordFound);
		System.out.format("Record not found: %s \n", recordNotFound);
		
		return "Game over!";
	}

	private void scanATGtable() {
		
		Map<String,AttributeValue> lastKey = null;

		ScanRequest scanRequest = new ScanRequest()
				.withTableName(System.getenv("SOURCE_TABLE"))
				.withExclusiveStartKey(lastKey)
				.withProjectionExpression("customerId, firstName, lastName");
		
		do {
			ScanResult result = client.scan(scanRequest);
			for (Map<String, AttributeValue> item : result.getItems()) {
				queryCDCImport(item.get("customerId").getS());
			}
			
			//System.out.format("LAST EVALUATED KEY: %s \n", result.getLastEvaluatedKey());
			lastKey = result.getLastEvaluatedKey();
			scanRequest.setExclusiveStartKey(lastKey);
			
		} while (lastKey != null);
		
		InputStream targetStream = new ByteArrayInputStream(sb.toString().getBytes());
		
		if(System.getenv("WRITE_REPORT").equals("true")) {
			 try {
				 ObjectMetadata meta = new ObjectMetadata();
		         meta.setContentLength(sb.toString().length());
		         meta.setContentType(ContentType.TEXT_PLAIN.toString());
				 s3client.putObject(bucketName, new SimpleDateFormat("'COMPARE_RESULTS_'yyyyMMddHHmm'.txt'").format(new Date()), targetStream, meta);
	         }
	         catch(AmazonServiceException e)
	         {
	             System.err.println(e.getErrorMessage());
	             System.exit(1);
	         }
		}else {
			System.out.format("WRITE_REPORT set to false: skipping report generation");
		}
		
	}

	private void queryCDCImport(String customerId) {

		HashMap<String, AttributeValue> key_to_get = new HashMap<String, AttributeValue>();

		key_to_get.put("customerId", new AttributeValue(customerId));

		GetItemRequest request = null;
		request = new GetItemRequest()
				.withKey(key_to_get)
				.withTableName(System.getenv("TARGET_TABLE"))
				.withProjectionExpression("customerId");
		
		try {
			Map<String, AttributeValue> returned_item = client.getItem(request).getItem();
			if (returned_item != null) {
				Set<String> keys = returned_item.keySet();
				for (String key : keys) {
					//System.out.format("%s: %s\n", key, returned_item.get(key).toString());
					if(System.getenv("WRITE_REPORT").equals("true")) {
						fmt.format("%s: %s\n", key, returned_item.get(key).toString());
					}
				}
				recordFound++;
			} else {
				if(System.getenv("WRITE_REPORT").equals("true")) {
					fmt.format("No item found with the key %s\n", customerId);
				}
				//System.out.format("No item found with the key %s!\n", customerId);
				recordNotFound++;
			}
		} catch (AmazonServiceException e) {
			System.err.println(e.getErrorMessage());
			recordNotFound++;
		}

	}
	
	

}
