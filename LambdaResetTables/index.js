var AWS = require('aws-sdk');
const dynamodb = new AWS.DynamoDB({
  apiVersion: '2012-08-10'
});

exports.handler = (event, context, callback) => {

  function recreateTable(tableName) {

    var promises = [];

    var deletePromise = new Promise((resolve, reject) => {

      var tableParams = {
        TableName: tableName
      };

      dynamodb.deleteTable(tableParams, function(err, data) {
        if (err && err.code === 'ResourceNotFoundException') {
          console.log("Error: Table " + tableName + " not found");
          return;
        } else if (err && err.code === 'ResourceInUseException') {
          console.log("Error: Table " + tableName + " in use");
          return;
        } else {
          console.log("Deleted table " + tableName, data);
        }

        resolve("deleted");
      });

      var operationFinished = false;
      // Call DynamoDB to retrieve the selected table descriptions
      do {
        dynamodb.describeTable(tableParams, function(err, data) {
          if (err) {
            console.log("Error", err);
            return;
          } else {
            console.log("Success", data.TableStatus);
            if (data.TableStatus !== 'DELETING') {
              operationFinished = true;
            }
          }
        });
      } while (operationFinished === false);

    });

    promises.push(deletePromise);

    var createPromise = new Promise((resolve, reject) => {
      var createParams = {
        TableName: tableName,
        KeySchema: [{
            AttributeName: "customerId",
            KeyType: "HASH"
          } //Partition key
        ],
        AttributeDefinitions: [{
          AttributeName: "customerId",
          AttributeType: "S"
        }],
        BillingMode: "PAY_PER_REQUEST"
      };

      dynamodb.createTable(createParams, function(err, data) {
        if (err) {
          console.error("Unable to create table " + tableName + ". Error JSON:", JSON.stringify(err, null, 2));
          //console.error("Unable to create table "+tableName);
          //reject();
        } else {
          //console.log("Created table " + tableName + ". Table description JSON:", JSON.stringify(data, null, 2));
          console.log("Created table " + tableName);
        }
        resolve("created");
      });
    });

    promises.push(createPromise);

    Promise.all(promises).then(function(values) {
      console.log("DONE");
    });
  }


  recreateTable("customers");
  //recreateTable("cdcimport");

  // var step1 = new Promise((resolve, reject) => {
  //   recreateTable("customers");
  //   resolve("step1");
  // });
  //
  // var step2 = new Promise((resolve, reject) => {
  //   recreateTable("cdcimport");
  //   resolve("step2");
  // });


};
