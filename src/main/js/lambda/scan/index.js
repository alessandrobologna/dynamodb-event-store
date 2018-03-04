"use strict";

const async = require('async');
const AWSXRay = require('aws-xray-sdk');
const AWS = AWSXRay.captureAWS(require('aws-sdk'));
const dynamoDb = new AWS.DynamoDB.DocumentClient();
const lambda = new AWS.Lambda();

const timeUnit = 1000;


exports.handler = (event, context, callback) => {
    const params = {
        TableName: process.env.DYNAMO_BUFFER_TABLE,
    };

    if (event.LastEvaluatedKey) {
        console.log("Resuming execution from " + event.LastEvaluatedKey)
        params.ExclusiveStartKey = event.LastEvaluatedKey;
    }

    dynamoDb.scan(params, (err, data) => {
        if (err) {
            return callback(err);
        }
        const promises = data.Items.map(record => {
            // determine if the payload is valid JSON, and decode it if it is   
            try {
                record.record_payload.kinesis.data = JSON.parse(Buffer.from(record.record_payload.kinesis.data, 'base64'));
            } catch (e) {
               // it wasn't JSON after all
            }

            // create an item that has for partition key a slot timestamp, and range key the timestamp
            let item = {
                event_time_slot: new Date(Math.floor(record.event_time_stamp / timeUnit) * timeUnit).toISOString(),
                event_time_stamp: new Date(record.event_time_stamp).toISOString(),
            }
            Object.keys(record.record_payload.kinesis).forEach(function(key) {
                item[key] = record.record_payload.kinesis[key];
            });
            // put the item in the event table and delete the item in the buffer table
            return new Promise((resolve, reject) => {
                dynamoDb.put({
                    TableName: process.env.DYNAMO_EVENT_TABLE,
                    Item: item
                }, (error, result) => {
                    if (error) {
                        reject(error);
                    }
                    dynamoDb.delete({
                        TableName: process.env.DYNAMO_BUFFER_TABLE,
                        Key: {
                            partition_key: record.partition_key
                        }
                    }, (error, result) => {
                        if (error) {
                            reject(error);
                        }
                        resolve(record.partition_key);
                    })
                })
            });
        });

        
        Promise.all(promises).then(results => {
            if (results.length) {
                console.log("Processed " + results.length + " documents:\n" + JSON.stringify(results))
                if (data.LastEvaluatedKey) {
                    console.log("There are more records to process, invoking another execution")
                    event.LastEvaluatedKey = data.LastEvaluatedKey;
                    lambda.invoke({
                        FunctionName: context.functionName,
                        InvocationType: 'Event',
                        Payload: JSON.stringify(event),
                        Qualifier: context.functionVersion
                    }, callback);
                }
            } else {
                console.log("Buffer table is empty, nothing to do")
            }
            callback(null, "success")
        }).catch(err => {
            callback(err);
        })
    });
};
