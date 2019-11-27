package com.burberry.datachecker;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.http.entity.ContentType;
import org.json.JSONObject;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.burberry.datachecker.model.ErrorDetail;
import com.burberry.datachecker.model.ResponseClass;

public class DataCompareHandlerParallel implements RequestHandler<Object, ResponseClass> {

	static AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();
	AmazonS3 s3client = AmazonS3ClientBuilder.standard().build();
	static DynamoDB dynamoDB = new DynamoDB(client);
	static int recordNotFound = 0;
	static int recordFound = 0;
	static int recordFoundNotMatching = 0;
	static int recordFoundMatching = 0;

	// number of items each scan request should return
	static int scanItemLimit = Integer.parseInt(System.getenv("SCAN_LIMIT"));

	// number of logical segments for parallel scan
	static int parallelScanThreads = Integer.parseInt(System.getenv("PARALLEL_THREADS"));;
	String bucketName = System.getenv("BUCKET_NAME");
	static List<ErrorDetail> errorDetails;
	
	@Override
	public ResponseClass handleRequest(Object input, Context context) {
		recordNotFound = 0;
		recordFound = 0;
		recordFoundMatching = 0;
		recordFoundNotMatching = 0;
		errorDetails = new ArrayList<ErrorDetail>();
		
		if (!s3client.doesBucketExistV2(bucketName)) {
			System.out.format("Bucket name is not available: %s", bucketName);
			System.exit(1);
		}
		
		parallelScan(System.getenv("SOURCE_TABLE"), scanItemLimit, parallelScanThreads);
		
		System.out.format("Record found: %s \n", recordFound);
		System.out.format("Record not found: %s \n", recordNotFound);
		System.out.format("Record found matching: %s \n", recordFoundMatching);
		System.out.format("Record found not matching : %s \n", recordFoundNotMatching);
		
		ResponseClass rsp = new ResponseClass(recordFound,recordFoundMatching,recordNotFound,recordFoundNotMatching, errorDetails);
		JSONObject s3Report = new JSONObject(rsp);
		dumpReportToS3(s3Report.toString(2));		
		//remove list of error messages from Lambda output
		rsp.setMessages(null);
		return rsp;
	}

	private void dumpReportToS3(String report) {
		if (System.getenv("WRITE_REPORT").equals("true")) {
			InputStream targetStream = new ByteArrayInputStream(report.toString().getBytes());
			try {
				ObjectMetadata meta = new ObjectMetadata();
				meta.setContentLength(report.toString().length());
				meta.setContentType(ContentType.TEXT_PLAIN.toString());
				s3client.putObject(bucketName,
						new SimpleDateFormat("'COMPARE_RESULTS_'yyyyMMddHHmm'.txt'").format(new Date()), targetStream,
						meta);
			} catch (AmazonServiceException e) {
				System.err.println(e.getErrorMessage());
				System.exit(1);
			}
		} else {
			System.out.format("WRITE_REPORT set to false: skipping report generation");
		}
	}

	private static void queryCDCImport(String customerId, String attributesHash, String fileName) {
		HashMap<String, AttributeValue> key_to_get = new HashMap<String, AttributeValue>();
		key_to_get.put("customerId", new AttributeValue(customerId));
		
		GetItemRequest request = null;
		request = new GetItemRequest()
				.withKey(key_to_get)
				.withTableName(System.getenv("TARGET_TABLE"))
				.withProjectionExpression("customerId, attributesHash, fileName");
		
		try {
			Map<String, AttributeValue> returned_item = client.getItem(request).getItem();
			if (returned_item != null) {
				if (System.getenv("WRITE_REPORT").equals("true")) {
					if (!returned_item.get("attributesHash").getS().equals(attributesHash)) {
						recordFoundNotMatching++;
						ErrorDetail err = new ErrorDetail( "No item found with the given customerId in CDC dataset.", 
								returned_item.get("customerId").getS(), 
								fileName, 
								returned_item.get("fileName").getS());
						errorDetails.add(err);
					}else {
						recordFoundMatching++;
					}
				}
				recordFound++;
			} else {
				if (System.getenv("WRITE_REPORT").equals("true")) {
					ErrorDetail err = new ErrorDetail( "Records not matching for given customer", 
							customerId, 
							fileName, 
							null);
					errorDetails.add(err);
				}
				recordNotFound++;
			}
		} catch (AmazonServiceException e) {
			e.printStackTrace();
			System.err.println(e.getErrorMessage());
			recordNotFound++;
		}
	}

	private static void parallelScan(String tableName, int itemLimit, int numberOfThreads) {
		ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);
		// Divide DynamoDB table into logical segments
		// Create one task for scanning each segment
		// Each thread will be scanning one segment
		int totalSegments = numberOfThreads;
		for (int segment = 0; segment < totalSegments; segment++) {
			// Runnable task that will only scan one segment
			ScanSegmentTask task = new ScanSegmentTask(tableName, itemLimit, totalSegments, segment);
			// Execute the task
			executor.execute(task);
		}
		shutDownExecutorService(executor);
	}

	// Runnable task for scanning a single segment of a DynamoDB table
	private static class ScanSegmentTask implements Runnable {

		// DynamoDB table to scan
		private String tableName;

		// number of items each scan request should return
		private int itemLimit;

		// Total number of segments
		// Equals to total number of threads scanning the table in parallel
		private int totalSegments;

		// Segment that will be scanned with by this task
		private int segment;

		public ScanSegmentTask(String tableName, int itemLimit, int totalSegments, int segment) {
			this.tableName = tableName;
			this.itemLimit = itemLimit;
			this.totalSegments = totalSegments;
			this.segment = segment;
		}

		@Override
		public void run() {
			int totalScannedItemCount = 0;

			Map<String, AttributeValue> lastKey = null;

			ScanRequest scanRequest = new ScanRequest()
					.withTableName(System.getenv("SOURCE_TABLE"))
					.withLimit(itemLimit)
					.withTotalSegments(totalSegments)
					.withSegment(segment)
					.withExclusiveStartKey(lastKey)
					.withProjectionExpression("customerId, attributesHash, fileName");

			do {
				ScanResult result = client.scan(scanRequest);
				totalScannedItemCount++;
				for (Map<String, AttributeValue> item : result.getItems()) {					
					if(item.get("customerId")!=null && item.get("attributesHash")!=null && item.get("fileName")!=null) {
						queryCDCImport(item.get("customerId").getS(), item.get("attributesHash").getS(), item.get("fileName").getS());
					}else {
						if(item.get("attributesHash") == null) {
							System.err.format("Hash not present for record: %s", item.get("customerId"));	
						}
						if(item.get("fileName") == null) {
							System.err.format("Filename not present for record: %s", item.get("customerId"));	
						}
					}
				}

				lastKey = result.getLastEvaluatedKey();
				scanRequest.setExclusiveStartKey(lastKey);

			} while (lastKey != null);

		}
	}

	private static void shutDownExecutorService(ExecutorService executor) {
		executor.shutdown();
		try {
			if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
				executor.shutdownNow();
			}
		} catch (InterruptedException e) {
			executor.shutdownNow();
			// Preserve interrupt status
			Thread.currentThread().interrupt();
		}
	}

}