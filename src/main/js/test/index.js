"use strict";

const async = require('async');
const AWS = require('aws-sdk');
const dynamoDb = new AWS.DynamoDB.DocumentClient();
const lambda = new AWS.Lambda();

const timeUnit = 1000;


exports.handler = function () {
    const params = {
        TableName: process.env.DYNAMO_EVENT_TABLE,
    };
};
