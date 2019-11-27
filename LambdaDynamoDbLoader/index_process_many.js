var async = require('async');
var crypto = require('crypto');
var AWS = require('aws-sdk');
//AWS.config.update({accessKeyId: 'mykey', secretAccessKey: 'mysecret', region: 'myregion'});

const dynamodb = new AWS.DynamoDB({
  apiVersion: '2012-08-10'
});

var S3 = new AWS.S3({
  maxRetries: 0,
  region: 'eu-west-1',
});

var totalSuccess = 0;
var totalErrors = 0;
var processedFiles = [];

exports.handler = (event, context, callback) => {
  console.log('Received event:', JSON.stringify(event, null, 2));

  const params = {
    Bucket: event.bucket,
    Delimiter: '/',
    Prefix: event.prefix
  };

  listObjects(params)
    .then(() => {
      console.log("Processing...");
    })
    .then(() => {
      console.log("RETURNING!");
      var output = {
        "totalSuccess": totalSuccess,
        "totalErrors": totalErrors,
        "processedFiles": processedFiles
      }
      callback(null, output);
    })
    .catch(error => {
      console.warn(error);
    });

  function listObjects(params) {
    return new Promise((resolve, reject) => {
      S3.listObjects(params, function(err, data) {
        if (err) throw new Error(err)
        var arr = [];
        data.Contents.forEach(function(item) {
          if (item.Key.endsWith("/") || (item.Key.endsWith('.json') === false)) {} else {
            arr.push(processFile(event, item));
          }
        });
        Promise.all(arr).then(() => {
          resolve();
        });
      });
    });
  }

  function processFile(event, srcItem) {
    var insertSuccess = 0;
    var insertErrors = 0;
    var tableName = '';
    var srcKey = srcItem.Key.substring(event.prefix.length);
    var srcBucket = event.bucket;

    if (srcKey.startsWith("ATG_Export")) {
      tableName = "customers";
    }

    if (srcKey.startsWith("CDC_Import")) {
      tableName = "cdcimport";
    }

    return new Promise((resolve, reject) => {
      async.waterfall([
        function(next) {
          console.log("DOWNLOAD: " + srcKey);
          S3.getObject({
            Bucket: srcBucket,
            Key: srcKey
          }, next);
        },
        function(data, next) {
          var recordsArray = JSON.parse(data.Body.toString('utf-8'));
          var uploadPromises = [];
          var errors = [];

          console.log("Customers in file: " + recordsArray.length);
          console.log("File size: " + srcItem.Size / 1024 / 1024 + " MB");

          recordsArray.map(record => {

            var attrs = record.data.customerIdentifier + checkData(record.profile.firstName) + checkData(record.profile.lastName) + checkData(record.profile.title) + checkData(record.profile.email) +
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
            var promise = new Promise((resolve, reject) => {
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
              }, function(err, data) {
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

          return Promise.all(uploadPromises).then(function(values) {

            const logFileName = 'LOG_' + srcKey + ".log";
            var logObject = {
              bucket: srcBucket,
              file: srcKey,
              timestamp: new Date(),
              customersInFile: recordsArray.length,
              fileSize: srcItem.Size / 1024 / 1024 + " MB",
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
              Key: event.prefix + logFileName,
              Body: JSON.stringify(logObject, null, 4) //logFileBody
            }, function(err) {
              if (err) {
                throw err;
              }
              next();
            });
          });
        }
      ], function(err, result) {
        if (err)
          console.log('Error ' + err);

        console.log(result);
        resolve();
      });
    });
  }

  function checkData(data) {
    if (data !== null && data !== '' && data !== undefined) {
      return data;
    }
    return "";
  }

  function checkNumber(data) {
    if (data !== null && data !== '' && data !== undefined) {
      return data[0].number;
    }
    return "";
  }
}
