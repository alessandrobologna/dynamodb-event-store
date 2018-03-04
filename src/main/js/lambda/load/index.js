'use strict';

const AWSXRay = require('aws-xray-sdk-core');
const AWS = AWSXRay.captureAWS(require('aws-sdk'));
const uuidv4 = require('uuid/v4');
const crypto = require('crypto');
const kinesis = new AWS.Kinesis();


exports.handler = (event, context, callback) => {
	let response = {
		statusCode: 200,
		headers: {
			'Content-Type': 'image/gif',
			'Cache-Control': 'no-cache'
		},
		body: "R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7",
		isBase64Encoded: true
	};
	let requestData = event;
    requestData['@timestamp'] = new Date().toISOString();
    kinesis.putRecord({
        Data: JSON.stringify(requestData),
        PartitionKey: uuidv4(),
        StreamName: process.env.STREAM_NAME
    }).promise().then(data => {
        if (data.SequenceNumber) {
            response.headers['X-SequenceNumber'] = data.SequenceNumber;
        }
    }).catch(error => {
        console.error("Kinesis error", error);
    }).then(() => callback(null, response));
};




