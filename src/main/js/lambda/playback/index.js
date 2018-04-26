const AWS = require('aws-sdk');
const dynamoDb = new AWS.DynamoDB.DocumentClient({
    region: process.env.AWS_REGION
});
const kinesis = new AWS.Kinesis({
    region: process.env.AWS_REGION
});

const timeUnit = parseInt(process.env.TIME_UNIT || "1000");

/*
 * Load events from the DynamoDB event store into the playback Kinesis stream 
 */

exports.handler = async (event, context, callback) => {
    event = event || {}
    // if no event.start is provided, start from 1 hour ago
    event.start = event.start || new Date(Math.floor(new Date() / timeUnit - 15 * 60) * timeUnit).toISOString();

    let start = new Date(Date.parse(event.start)).toISOString()
    let end = event.end ? new Date(Date.parse(event.end)).toISOString() : new Date().toISOString()
    console.log(`Starting event playback on ${process.env.DYNAMO_EVENT_TABLE} from ${event.start} to ${end}`)
    try {
        while (start < end) {
            const params = {
                TableName: process.env.DYNAMO_EVENT_TABLE,
                KeyConditionExpression: 'event_time_slot = :event_time_slot',
                ExpressionAttributeValues: { ':event_time_slot': start },
                Limit: 500 // max number of records in a kinesis put operation
            };
            // make sure all records are played back
            while (true) {
                const records = await dynamoDb.query(params).promise();
                if (records.Items.length > 0) {
                    let response = {
                        SequenceNumber: 0}
                    ;
                    for (let record of records.Items) {
                        const data = JSON.stringify(record.record_payload.kinesis.data);
                        while (true) {
                            try {
                                response = await kinesis.putRecord({
                                    Data: data,
                                    PartitionKey: record.record_payload.kinesis.partitionKey,
                                    SequenceNumberForOrdering: response.SequenceNumber
                                }).promise();
                                break;
                            } catch (error) {
                                console.log(error)
                                continue;
                            }
                        }
                    }
                    console.log(`${records.Items[0].event_time_slot}: ${records.Items[0].event_time_stamp} [${records.Items.length}]`)
                    if (!records.LastEvaluatedKey) {
                        // mo more records for this time slot
                        break;
                    }
                    // set a new start key to extract more records
                    params.ExclusiveStartKey = records.LastEvaluatedKey;

                } else {
                    console.log(start);
                    break;
                }
            }
            start = new Date(Date.parse(start) + timeUnit).toISOString();
        }
    } catch (error) {
        console.error("An error occured during playback", error)
        return callback(error)
    }
    callback(null, "Success");
}

