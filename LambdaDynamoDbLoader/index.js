var async = require('async');

console.log('Loading function');
var crypto = require('crypto');

var AWS = require('aws-sdk');
const dynamodb = new AWS.DynamoDB({apiVersion: '2012-08-10'});

var S3 = new AWS.S3({
    maxRetries: 0,
    region: 'eu-west-1',
});

exports.handler = (event, context, callback) => {
    console.log('Received event:', JSON.stringify(event, null, 2));
	
	var insertSuccess = 0;
	var insertErrors = 0;
	var totalSuccess = 0;
	var totalErrors = 0;
	var processedFiles = [];
	
	const processFolderName = process.env.PROCESS_FOLDER_NAME+'/';
	const atgTable = process.env.ATG_TABLE;
	const cdcTable = process.env.CDC_TABLE;
	const atgFilePrefix = process.env.ATG_FILE_PREFIX;
	const cdcFilePrefix = process.env.CDC_FILE_PREFIX;

    var srcBucket = event.Records[0].s3.bucket.name;
    var srcKey = event.Records[0].s3.object.key;
	
	//to extract the filename, remove the process folder name and intermediate subfolders
	var fullFilePath = srcKey.substring(processFolderName.length);
	var pathArray = fullFilePath.split("/"); 
	var filename = pathArray[pathArray.length-1];
	
	const logFilePath = 'LOAD_LOG/' + 'LOG_' + filename + ".log";;
	
	console.log("Event " + event+ "\n");
    console.log("Params: srcBucket: " + srcBucket + " srcKey: " + srcKey + "\n");
	console.log("Processing File name: " + filename + "\n");
	console.log("Log file path: " + logFilePath + "\n");

    var tableName = "";
    
	if (filename.startsWith(atgFilePrefix)) {
      tableName = atgTable;
    } else if (filename.startsWith(cdcFilePrefix)) {
      tableName = cdcTable;
    } else {
      console.log("File name not recognized: " + filename + "\n");
    }
	
	if(tableName === atgTable || tableName === cdcTable){
		async.waterfall(
		[ 
			function download(next) {
				
				console.log("DOWNLOAD: " + srcKey);
				
				S3.getObject({
					Bucket: srcBucket,
					Key: srcKey,
				},next);
			},
			
			function load(data, next){
				var recordsArray = JSON.parse( data.Body.toString('utf-8'));
			
				console.log("Customers in file: "+recordsArray.length + " - file size: "+event.Records[0].s3.object.size/1024/1024 + " MB");
				
				var uploadPromises = [];
				var errors = [];
				
				recordsArray.map((record) => {

					var attrs = record.data.customerIdentifier + checkData(record.profile.firstName) + checkData(record.profile.lastName) + checkData(record.profile.title) + checkData(record.profile.email)+
								checkNumber(record.profile.phones) + checkData(record.data.contacts.contact[0].addressLine1) + checkData(record.data.contacts.contact[0].addressLine2) + 
								checkData(record.data.contacts.contact[0].addressLine3) + checkData(record.data.contacts.contact[0].city) + checkData(record.data.contacts.contact[0].postalCode) + 
								checkData(record.data.contacts.contact[0].country) + checkData(record.data.contacts.contact[0].state) + checkData(record.preferences.thirdPartyConsent.isConsentGranted) +
								checkData(record.preferences.thirdPartyConsent.lastConsentModified) + checkData(record.subscriptions.post.marketing.email.isSubscribed) + 
								checkData(record.subscriptions.post.marketing.email.lastUpdatedSubscriptionState) + checkData(record.subscriptions.phone.marketing.email.lastUpdatedSubscriptionState) + 
								checkData(record.subscriptions.phone.marketing.email.isSubscribed) + checkData(record.subscriptions.phone.marketing.email.lastUpdatedSubscriptionState) + 
								checkData(record.subscriptions.email.marketing.email.isSubscribed) + checkData(record.subscriptions.email.marketing.email.lastUpdatedSubscriptionState) + 
								checkData(record.subscriptions.messaging.marketing.email.isSubscribed) + checkData(record.subscriptions.messaging.marketing.email.lastUpdatedSubscriptionState) + 
								checkData(record.preferences.userProfilingConsent.isConsentGranted) + checkData(record.preferences.userProfilingConsent.lastConsentModified) + 
								checkData(record.data.sourceCreateDate);
			
					var hash = crypto.createHash('md5').update(attrs).digest('hex');
					
					var promise = new Promise ((resolve, reject) => {
						dynamodb.putItem({
							TableName: tableName,
							Item: {
								"customerId": {
								  S: record.data.customerIdentifier
								},
								"attributesHash": {
								  S: hash
								},
								"fileName": {
								  S: srcKey
								}
							  }
						},function(err, data) {
							if (err) {
								insertErrors++;
								if (record.data.customerIdentifier !== undefined) {
									errors.push({
									gcn: record.data.customerIdentifier,
									firstName: record.profile.firstName,
									lastName: record.profile.lastName,
									error: err
									});
								} else {
									errors.push({
									gcn: 'n/a',
									firstName: record.profile.firstName,
									lastName: record.profile.lastName,
									error: 'customerId not found in record'
									});
								}
								console.log(err, err.stack);
							} else {
								insertSuccess++;
							}
							resolve();
						});
					});
					
					uploadPromises.push(promise);				
				});	
				
				Promise.all(uploadPromises).then(function(values) {
					
					var logObject = {
						bucket: srcBucket,
						file: srcKey,
						timestamp: new Date(),
						customersInFile: recordsArray.length,
						fileSize: event.Records[0].s3.object.size / 1024 / 1024 + " MB",
						destinationTable: tableName,
						successCount: insertSuccess,
						errorCount: insertErrors,
						failedRecords: errors
					  };

					  console.log(logObject);
					  totalSuccess += insertSuccess;
					  totalErrors += insertErrors;
		  
					  processedFiles.push({
						fileName: srcKey,
						successCount: insertSuccess,
						errorCount: insertErrors
					  });
		  
					  if (insertErrors > 0) {
						console.error('Error while reading from ' + srcBucket + '/' + srcKey);
					  } else {
						console.log('Successfully migrated data from ' + srcBucket + '/' + srcKey);
					  }
		  
					  S3.putObject({
						Bucket: srcBucket,
						Key: logFilePath,
						Body: JSON.stringify(logObject, null, 4) //logFileBody
					  }, function(err) {
						if (err) {
						  throw err;
						}
						next();
					  });
					});			
			}
			
		], function(err, result){
			if (err) {
				console.log('Error '+err);
			} 
			var output = {
				"totalSuccess": totalSuccess,
				"totalErrors": totalErrors,
				"processedFiles": processedFiles
			  }
			  callback(null, output);
		});	
	}	
	
	function checkData(data){
        if(data !== null && data !== '' && data !== undefined) {
            return data;
        }else{
            return "";
        }
    }
    
    function checkNumber(data){
        if(data !== null && data !== '' && data !== undefined) {
            return data[0].number;
        }else{
            return "";
        }
    }
    
};