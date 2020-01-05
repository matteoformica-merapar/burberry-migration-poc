package com.burberry.datacopier;

import java.util.List;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.burberry.datachecker.model.DataCopyRequest;

public class DataCopyHandler implements RequestHandler<DataCopyRequest, String> {

	AmazonS3 s3client = AmazonS3ClientBuilder.standard().build();
	
	@Override
	public String handleRequest(DataCopyRequest input, Context context) {
		
		String bucketName = input.getBucketName();
		String processFolderName = input.getProcessFolderName();
		String prefix = input.getPrefix();
		String delay = input.getDelay();
		
		System.out.format("Input: %s", input);
		
		if (!s3client.doesBucketExistV2(bucketName)) {
			System.out.format("Bucket name is not available: %s \n", bucketName);
			System.exit(1);
		}

		ObjectListing listing = s3client.listObjects(bucketName, prefix);
		List<S3ObjectSummary> summaries = listing.getObjectSummaries();
		
		System.out.format("Files: %s", summaries.size());
		
		int copyCounter = 0;
		int errorCounter = 0;
		
		for(S3ObjectSummary summary : summaries) {
			try {
				// copy object with delay
				System.out.format("Copying: %s \n", summary.getKey());
				CopyObjectRequest copyObjRequest = new CopyObjectRequest(bucketName, summary.getKey(), bucketName, processFolderName+"/"+summary.getKey());
				s3client.copyObject(copyObjRequest);
				copyCounter++;
				Thread.sleep(Integer.parseInt(delay));
			}catch(Exception e) {
				System.err.format("Cannot copy: %s \n", summary.getKey());
				e.printStackTrace();
				errorCounter++;
			}
		}

		listing = s3client.listNextBatchOfObjects(listing);
		summaries.addAll(listing.getObjectSummaries());
		
		return "Files Copied : "+copyCounter+" - Errors: "+errorCounter;

	}

}
