const AWS = require('aws-sdk');
const dynamoDb = new AWS.DynamoDB.DocumentClient({
    region: process.env.AWS_REGION
});
const lambda = new AWS.Lambda({
    region: process.env.AWS_REGION
});
const timeUnit = parseInt(process.env.TIME_UNIT || "1000");

/*
 * Periodically scan the event table, and link records to their successor 
    
    00:01:00 => {next : 00:01:10}
    00:01:10 => {next : 00:01:15}
    00:01:15 => {next : 00:01:18}
    00:01:18 => {next : undefined}

 */

exports.handler = async (event, context, callback) => {
    event = event || {}
    // if no event.start is provided, start from 15 minutes ago
    event.start = event.start || new Date(Math.floor(new Date() / timeUnit - 15 * 60) * timeUnit).toISOString();
    let current = new Date(Date.parse(event.start)).toISOString()
    
    // stop linking at the current timestamp 
    event.end = event.end ? new Date(Date.parse(event.end)).toISOString() : new Date().toISOString()

    console.log(`Starting event playback on ${process.env.DYNAMO_EVENT_TABLE} from ${event.start} to ${event.end}`)
    try {
        let event_time_slot = undefined;
        let previous = undefined;
        // leave this lambda before it times out
        while (current < event.end && context.getRemainingTimeInMillis() < timeUnit * 5) {
            const params = {
                TableName: process.env.DYNAMO_EVENT_TABLE,
                KeyConditionExpression: 'event_time_slot = :event_time_slot',
                ExpressionAttributeValues: { 
                    ':event_time_slot': current 
                },
                Limit: 1 // we just need to retrieve the first record for each time slot
            };

            const records = await dynamoDb.query(params).promise();
            if (records.Items.length > 0) {
                const record = records.Items[0];
                if (previous) {
                    previous.next_slot = record.event_time_slot
                    const result = await dynamoDb.put({
                        TableName: process.env.DYNAMO_EVENT_TABLE,
                        Item: previous
                    }).promise()
                    console.log(`Linked  : ${previous.event_time_slot} => ${record.event_time_slot}`)
                }
                previous = record;
            } else {
                console.log(`Skipped : ${current}`);
            }
            // set a new start key to extract more records
            current = new Date(Date.parse(current) + timeUnit).toISOString();
        }
        console.log(`Processed ${(Date.parse(current) - Date.parse(event.start))/1000} time slots`)
        if (current < event.end) {
            // exited the loop because of an impeding timeout
            console.log(`Invoking continuation lambda before timeout`)
            const result = await lambda.invoke({
                FunctionName: context.functionName,
                InvocationType: 'Event',
                Payload: JSON.stringify({
                    'start' : current,
                    'end' : event.end,
                    'continue' : true
                }),
                Qualifier: context.functionVersion
            }).promise(); 
            console.log(`Invoked continuation lambda with start: ${current}`)   
        }
    } catch (error) {
        console.error("An error occured during linking", error)
        return callback(error)
    }
    callback(null, "Success");
}

