var async = require('async');

console.log('Loading function');
var crypto = require('crypto');

var AWS = require('aws-sdk');
const dynamodb = new AWS.DynamoDB({apiVersion: '2012-08-10'});

var S3 = new AWS.S3({
    maxRetries: 0,
    region: 'eu-west-1',
});

var insertSuccess = 0;
var insertErrors = 0;
var logContentKO = "";

exports.handler = (event, context, callback) => {
    console.log('Received event:', JSON.stringify(event, null, 2));

    var srcBucket = event.Records[0].s3.bucket.name;
    var srcKey = event.Records[0].s3.object.key;
    const d = new Date();
	const logFileName = 'CIAM_DB_LOAD'+d.getFullYear() + "" + (d.getMonth() + 1) + "" + d.getDate()+'.log';
	
    console.log("Params: srcBucket: " + srcBucket + " srcKey: " + srcKey + "\n");
    
    var tableName = "";
    
	if(srcKey.startsWith("ATG_Export")){
        tableName = "customers";
    }
    
	if(srcKey.startsWith("CDC_Import")){
        tableName = "cdcimport";
    }
   
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
							"hash" : {
								S: hash
							}
						}
					},function(err, data) {
						if (err) {
							insertErrors++;
							if(record.data.customerIdentifier !== undefined){
								errors.push({ gcn: record.data.customerIdentifier, firstName: record.profile.firstName, lastName: record.profile.lastName});
							}else{
								errors.push({ gcn: 'n/a', firstName: record.profile.firstName, lastName: record.profile.lastName});
							}
							
							//logContentKO += (record.data.customerIdentifier + ",");
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
				var s3Body = "*** Loading data: \nBucket: srcBucket: " + srcBucket + "\nFile: " + srcKey + "\n"
				s3Body+= "Timestamp: "+new Date()+"\n";
				s3Body+= "Customers in file: "+recordsArray.length + "\nFile size: "+event.Records[0].s3.object.size/1024/1024 + " MB\n";
				s3Body += "DynamoDB destination table: "+tableName+"\n";
				s3Body += 'Insert Result : successCount: ' + insertSuccess + ', errorCount: ' + insertErrors
				console.log("Insert Result -- successCount: " + insertSuccess + " errorCount: " + insertErrors);
				
				if(insertErrors > 0){
				    logContentKO += ('\nRecords which failed insertion:\n' + JSON.stringify(errors));
					console.error('Error while reading from ' + srcBucket + '/' + srcKey);
				}else{
					console.log('Successfully migrated data from ' + srcBucket + '/' + srcKey);
				}   
				
				var logFileBody = s3Body + logContentKO;
				
				S3.getObject({
					Bucket: srcBucket,
					Key: logFileName,
				}, function(err,data){
					
					if(err){
						if (err.statusCode === 404) {
							S3.putObject({
								Bucket: srcBucket,
								Key: logFileName,
								Body: logFileBody
							}, function (err) {
								if (err) { throw err; }
							});	
						}
					}else{
						S3.putObject({
							Bucket: srcBucket,
							Key: logFileName,
							Body: data.Body.toString('utf-8') + '\n'+ logFileBody
						  }, function (err) {
							if (err) { throw err; }
						  });
					}
				});
				
			});
			
        }
        
    ], function(err, result){
		if (err) {
			console.log('Error '+err);
		} 
		console.log(result);
	});
		
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