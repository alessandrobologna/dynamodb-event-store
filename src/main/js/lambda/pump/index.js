"use strict";

const async = require('async');
const AWSXRay = require('aws-xray-sdk');
const AWS = AWSXRay.captureAWS(require('aws-sdk'));
const dynamoDb  = new AWS.DynamoDB.DocumentClient();
const crypto = require('crypto')


exports.handler = (event, context, callback) => {
    console.log("Processing " + event.Records.length + " records");
    const promises = event.Records.map(record => {
        // use a sha256 of the eventSourceARN and the eventID
        let item = {
            partition_key: crypto.createHash('sha256').update(record.eventSourceARN + record.eventID).digest("hex"),
            event_time_stamp: Math.ceil(record.kinesis.approximateArrivalTimestamp * 1000),
            record_payload: record
        }

        return new Promise((resolve, reject) => {
            dynamoDb.put({
                TableName: process.env.DYNAMO_BUFFER_TABLE,
                Item: item
            }, (error, result) => {
                if (error) {
                    reject(error);
                }
                resolve(item.partition_key);
            })
        });
    });

    Promise.all(promises).then(results => {
        console.log("Consumed " + results.length + " records from kinesis");
        console.log(JSON.stringify(results));
        callback(null, "Success");
    }).catch(function (err) {
        console.log('A promise failed to resolve', err);
        callback(err);
    })
};
