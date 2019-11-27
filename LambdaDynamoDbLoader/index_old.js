'use strict';
//var asyncr = require('async');

const doc = require('dynamodb-doc');

var crypto = require('crypto');
var fs = require('fs');

var AWS = require('aws-sdk');
const dynamodb = new AWS.DynamoDB({apiVersion: '2012-08-10'});

var S3 = new AWS.S3({
    maxRetries: 0,
    region: 'eu-west-1',
});



exports.handler = (event, context, callback) => {
    
    var insertSuccess = 0;
    var insertErrors = 0;
    
    console.log('Received event:', JSON.stringify(event, null, 2));
    console.log("Init complete, running.. \n")

    var srcBucket = event.Records[0].s3.bucket.name;
    var srcKey = event.Records[0].s3.object.key;
    
    console.log("Params: srcBucket: " + srcBucket + " srcKey: " + srcKey + "\n");
    
    var tableName = "";
    if(srcKey.startsWith("ATG_Export")){
        tableName = "customers"
    }
    if(srcKey.startsWith("CDC_Import")){
        tableName = "cdcimport"
    }
    
    S3.getObject({
        Bucket: srcBucket,
        Key: srcKey,
    }, function(err, data ) {
        if (err !== null) {
            console.log("ERROR: " + err);
            return callback(err, null);
        }
        
        //var recordsArray = [{ "data": { "customerIdentifier" : "1234" }}]
        var recordsArray = JSON.parse( data.Body.toString('utf-8'));
        
        console.log("Customers in file: "+recordsArray.length + " - file size: "+event.Records[0].s3.object.size/1024/1024 + " MB");
        
        for (var i = 0; i < recordsArray.length; i++) {
     
            var record = recordsArray[i];
            
            //console.log("table: " + tableName +" - record ID: " + record.data.customerIdentifier);
            var attrs = record.data.customerIdentifier + checkData(record.profile.firstName) + checkData(record.profile.lastName) + checkData(record.profile.title) + checkData(record.profile.email)+
                        checkData(record.profile.phones[0]) + checkData(record.data.contacts.contact[0].addressLine1) + checkData(record.data.contacts.contact[0].addressLine2) + 
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
            
            //console.log("hash: " + hash);
            
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
            }, function(err, data) {
                if (err) {
                    console.log("DDB ERROR: "+err, err.stack);
                    insertErrors++;
                    callback(null, {
                        statusCode: '500',
                        body: err
                    });
                } else {
                    insertSuccess++;
                    callback(null, {
                        statusCode: '200',
                        body: 'Hello ' + record.profile.lastName + '!'
                    });
                }
            });

         }
        
        return callback(null, "Insert Result -- successCount: " + insertSuccess + " errorCount: " + insertErrors);
    });
    
    function checkData(data){
        if(data !== null && data !== '' && data !== undefined) {
            return data;
        }else{
            return "";
        }
    }
    
    function dynamoResultCallback(err, data) {
        if (err) {
            insertErrors++;
            console.log("Insert Error: \n");
            console.log(err, err.stack); // an error occurred
        } else {
            insertSuccess++;
        }
    }
    
};