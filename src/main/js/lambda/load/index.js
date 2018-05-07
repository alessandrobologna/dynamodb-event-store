'use strict';

const AWSXRay = require('aws-xray-sdk-core');
const AWS = AWSXRay.captureAWS(require('aws-sdk'));
const uuidv4 = require('uuid/v4');
var randomstring = require('randomstring');

const kinesis = new AWS.Kinesis({
    region: process.env.AWS_REGION
});
/*
 * Load events into Kinesis from API gateway 
 */

exports.handler = (event, context, callback) => {
    // generate a random message body if not provided
    if (event.body && Object.keys(event.body).length === 0) {
        event.body = {
            'MessageType' : 'Claps',
            'MemberId' : randomstring.generate({
                length: 2,
                charset: 'hex'
            }),
            'PostId' : randomstring.generate({
                length: 2,
                charset: 'hex'
            }),
            get EntityId() {
                return `${this.MessageType}-${this.MemberId}-${this.PostId}`;
            }
        };
    }
    let response = {
        statusCode: 200,
        headers: {
            'Content-Type': 'application/json',
            'Cache-Control': 'no-cache'
        },
        body: event.body
    };
    event['@timestamp'] = new Date().toISOString();
    kinesis.putRecord({
        Data: JSON.stringify(event),
        PartitionKey: event.body && event.body.EntityId || uuidv4(), // default to random uuid if no EntityId is provided 
        StreamName: process.env.EVENT_STREAM
    }).promise().then(data => {
        if (data.SequenceNumber) {
            response.headers['X-SequenceNumber'] = data.SequenceNumber;
        }
        callback(null, response);
    }).catch(error => {
        console.error('Kinesis error', error);
        callback(error);
    });
};




