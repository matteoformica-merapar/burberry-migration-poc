package com.burberry.datachecker;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.http.entity.ContentType;
import org.json.JSONObject;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.BatchGetItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.TableKeysAndAttributes;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.burberry.datachecker.model.CustomerRecord;
import com.burberry.datachecker.model.ErrorDetail;
import com.burberry.datachecker.model.ResponseClass;

public class DataCompareHandlerSequentialBatched implements RequestHandler<Object, ResponseClass> {

	static AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();
	AmazonS3 s3client = AmazonS3ClientBuilder.standard().build();
	static DynamoDB dynamoDB = new DynamoDB(client);

	static int recordNotFound = 0;
	static int recordFound = 0;
	static int recordFoundNotMatching = 0;
	static int recordFoundMatching = 0;
	String bucketName = System.getenv("BUCKET_NAME");
	static List<ErrorDetail> errorDetails;

	static final int BATCH_SIZE = 100;

	StringBuilder sb = new StringBuilder("");
	Formatter fmt = new Formatter(sb);

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

		scanATGtable();

		fmt.format("Record found: %s \n", recordFound);
		fmt.format("Record not found: %s \n", recordNotFound);

		System.out.format("Record found: %s \n", recordFound);
		System.out.format("Record not found: %s \n", recordNotFound);
		System.out.format("Record found matching: %s \n", recordFoundMatching);
		System.out.format("Record found not matching : %s \n", recordFoundNotMatching);

		ResponseClass rsp = new ResponseClass(recordFound, recordFoundMatching, recordNotFound, recordFoundNotMatching,
				errorDetails);
		JSONObject s3Report = new JSONObject(rsp);
		dumpReportToS3(s3Report.toString(2));
		// remove list of error messages from Lambda output
		rsp.setMessages(null);
		return rsp;
	}

	private void scanATGtable() {

		Map<String, AttributeValue> lastKey = null;

		ScanRequest scanRequest = new ScanRequest()
				.withTableName(System.getenv("SOURCE_TABLE"))
				.withExclusiveStartKey(lastKey)
				.withProjectionExpression("customerId, attributesHash, fileName");

		List<CustomerRecord> scannedCustomers = new ArrayList<>();

		do {
			ScanResult result = client.scan(scanRequest);
			for (Map<String, AttributeValue> item : result.getItems()) {

				if (item.get("customerId") != null && item.get("attributesHash") != null && item.get("fileName") != null) {
					
					if(scannedCustomers.size() == BATCH_SIZE -1) {
						queryTargetBatch(scannedCustomers);
						scannedCustomers = new ArrayList<CustomerRecord>();						
					} else {
						scannedCustomers.add(new CustomerRecord(
								item.get("customerId").getS(),
								item.get("attributesHash").getS(), 
								item.get("fileName").getS()));
					} 
				} else {
					if (item.get("attributesHash") == null) {
						System.err.format("Hash not present for record: %s", item.get("customerId"));
					}
					if (item.get("fileName") == null) {
						System.err.format("Filename not present for record: %s", item.get("customerId"));
					}
				}
			}

			lastKey = result.getLastEvaluatedKey();
			scanRequest.setExclusiveStartKey(lastKey);
		} while (lastKey != null);

	}

	private static void queryTargetBatch(List<CustomerRecord> scannedCustomers) {

		List<String> ids = scannedCustomers.stream().map(x -> x.getCustomerId()).collect(Collectors.toList());

		TableKeysAndAttributes targetTableKeysAndAttributes = new TableKeysAndAttributes(System.getenv("TARGET_TABLE"));
		targetTableKeysAndAttributes.addHashOnlyPrimaryKeys("customerId", ids)
				.withProjectionExpression("customerId, attributesHash, fileName");

		BatchGetItemOutcome outcome = dynamoDB.batchGetItem(targetTableKeysAndAttributes);
		List<Item> items = outcome.getTableItems().get(System.getenv("TARGET_TABLE"));

		Map<String, CustomerRecord> queryResultMap = new HashMap<String, CustomerRecord>();
		for (Item i : items) {
			queryResultMap.put(i.getString("customerId"),
					new CustomerRecord(i.getString("customerId"), i.getString("hash"), i.getString("fileName")));
		}

		for (CustomerRecord mainRecord : scannedCustomers) {
			CustomerRecord targetItem = queryResultMap.get(mainRecord.getCustomerId());

			if (targetItem != null) {
				if (System.getenv("WRITE_REPORT").equals("true")) {
					if (!targetItem.getHash().equals(mainRecord.getHash())) {
						recordFoundNotMatching++;
						ErrorDetail err = new ErrorDetail("No item found with the given customerId in CDC dataset.",
								targetItem.getCustomerId(), mainRecord.getFilename(), targetItem.getFilename());
						errorDetails.add(err);
					} else {
						recordFoundMatching++;
					}
				}
				recordFound++;
			} else {
				if (System.getenv("WRITE_REPORT").equals("true")) {
					ErrorDetail err = new ErrorDetail("Records not matching for given customer",
							mainRecord.getCustomerId(), mainRecord.getFilename(), null);
					errorDetails.add(err);
				}
				recordNotFound++;
			}
		}
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

}
