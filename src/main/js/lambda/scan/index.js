const AWSXRay = require('aws-xray-sdk');
const AWS = AWSXRay.captureAWS(require('aws-sdk'));
const dynamoDb = new AWS.DynamoDB.DocumentClient({
    region: process.env.AWS_REGION
});
const lambda = new AWS.Lambda({
    region: process.env.AWS_REGION
});

const timeUnit = parseInt(process.env.TIME_UNIT || "1000");
const decodePayload = "true" === (process.env.DECODE_PAYLOAD || "true")

/*
 * Periodically scan the buffer table and  store into the event table 
 */
exports.handler = async (event, context, callback) => {
    const params = {
        TableName: process.env.DYNAMO_BUFFER_TABLE,
        Limit: 500
    };
    if (event.LastEvaluatedKey) {
        console.log("Resuming execution from " + JSON.stringify(event.LastEvaluatedKey));
        params.ExclusiveStartKey = event.LastEvaluatedKey;
    }

    try {
        const response = await dynamoDb.scan(params).promise()
        if (response.LastEvaluatedKey) {
            console.log("There are more records to process, invoking another execution")
            event.LastEvaluatedKey = response.LastEvaluatedKey;
            lambda.invoke({
                FunctionName: context.functionName,
                InvocationType: 'Event',
                Payload: JSON.stringify(event),
                Qualifier: context.functionVersion
            }, (err, data) => {
                if (err) {
                    console.error("Failed lambda invocation", e)
                    callback(e)
                }
            });
        }
        if (response.Items.length > 0) {
            const records = response.Items.map(async record => {
                if (decodePayload) {
                    try {
                        record.record_payload.kinesis.data = JSON.parse(Buffer.from(record.record_payload.kinesis.data, 'base64'));
                    } catch (e) {
                        console.warn("Could not decode the payload")
                        // it wasn't JSON after all
                    }
                }
                // create an item that has for partition key a slot timestamp, and range key the timestamp
                const putResponse = await dynamoDb.put({
                    TableName: process.env.DYNAMO_EVENT_TABLE,
                    Item: {
                        event_time_slot: new Date(Math.floor(record.event_time_stamp / timeUnit) * timeUnit).toISOString(),
                        event_time_stamp: new Date(record.event_time_stamp).toISOString(),
                        event_id: record.event_id,
                        record_payload: record.record_payload
                    }
                }).promise()
                // if the put calls fails, an exception will be thrown and the delete method will not be invoked
                const deleteResponse = await dynamoDb.delete({
                    TableName: process.env.DYNAMO_BUFFER_TABLE,
                    Key: {
                        partition_key: record.partition_key
                    }
                }).promise()
                return record.partition_key;
            });
            console.log(`Processed ${records.length} records`);
        } else {
            console.log("Buffer table is empty, nothing to do")
        }
        return callback(null, "Success")
    } catch (e) {
        console.error("Exception during execution:", e)
        callback(e)
    }
};
