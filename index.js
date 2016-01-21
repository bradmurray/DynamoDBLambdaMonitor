"use strict";

var cacheStuff = {};

var config = {
    TableName: "YOURTABLEHERE",
    Reads: {
        Threshold: { Lower: 0.5, Upper: 0.75 },
        Rate: { Decrease: 0.75, Increase: 0.5 },
        NumChecksBeforeScaleUp: 2,
        NumThrottledBeforeScaleUp: 1,
        Provisioned: { Min: 10, Max: 200 }
    },
    Writes: {
        Threshold: { Lower: 0.4, Upper: 0.7 },
        Rate: { Decrease: 0.6, Increase: 0.5 },
        NumChecksBeforeScaleUp: 1,
        NumThrottledBeforeScaleUp: 1,
        Provisioned: { Min: 3, Max: 100 }
    },
    NumChecksBeforeScaleDown: 5,
    MinWaitMinutesBetweenScaleDowns: 60,
    CheckIntervalMinutes: 5,
    MaxDecreasesPerDay: 4,
    DryRun: false,
    Debug: false
};

var aws = require("aws-sdk");
var dynamodb = new aws.DynamoDB();
var cloudwatch = new aws.CloudWatch();

exports.handler = function(event, context) {
    if (cacheStuff.lastDate && cacheStuff.containerRunCount) {
        console.log("Last run at " + cacheStuff.lastDate);
        console.log("Container runs: " + cacheStuff.containerRunCount);
    }
    else
        cacheStuff.containerRunCount = 0;
        
    
    var dtEndCollection = new Date();
    var dtStartCollection = new Date(dtEndCollection - config.CheckIntervalMinutes * 60 * config.NumChecksBeforeScaleDown * 1000);
    var collectedData = {};
    var asyncManager = { callbackCount: 0, totalAsyncCalls: 0 };
    
    // Validate settings
    if (config.Reads.Threshold.Upper >= 1 || config.Reads.Threshold.Upper <= 0 || config.Writes.Threshold.Upper >= 1 || config.Writes.Threshold.Upper <= 0 || config.Reads.Threshold.Lower >= config.Reads.Threshold.Upper || config.Writes.Threshold.Lower >= config.Writes.Threshold.Upper || config.Reads.Provisioned.Min < 1 || config.Writes.Provisioned.Min < 1 || config.Reads.Provisioned.Min > config.Reads.Provisioned.Max || config.Writes.Provisioned.Min > config.Writes.Provisioned.Max || config.MaxDecreasesPerDay < 1 || config.CheckIntervalMinutes < 1)
        context.fail("Invalid settings");

    asyncManager.totalAsyncCalls++;
    dynamodb.describeTable({ TableName: config.TableName }, function(err, data) {
        if (err) context.fail(err);
        else {
            if (data.Table.TableStatus === "ACTIVE") {
                if (config.Debug)
                    collectedData.DynamoDBTable = data.Table;
                collectedData.ProvisionedThroughput = data.Table.ProvisionedThroughput;
                completeCollection(context, collectedData, asyncManager, dtEndCollection);
            }
            else
                context.fail("Table is in " + data.Table.TableStatus + " status - aborting.");
        }
    });

    asyncManager.totalAsyncCalls++;
    cloudwatch.getMetricStatistics({ MetricName: 'ConsumedReadCapacityUnits', Statistics: [ 'Sum' ], Unit: 'Count', Dimensions: [ { Name: 'TableName', Value: config.TableName } ], Namespace: 'AWS/DynamoDB', Period: config.CheckIntervalMinutes * 60, StartTime: dtStartCollection, EndTime: dtEndCollection }, function(err, data) {
        if (err) context.fail(err);
        else {
            delete data.ResponseMetadata;
            collectedData.ConsumedReadCapacityUnits = data.Datapoints;
            completeCollection(context, collectedData, asyncManager, dtEndCollection);
        }
    });

    asyncManager.totalAsyncCalls++;
    cloudwatch.getMetricStatistics({ MetricName: 'ConsumedWriteCapacityUnits', Statistics: [ 'Sum' ], Unit: 'Count', Dimensions: [ { Name: 'TableName', Value: config.TableName } ], Namespace: 'AWS/DynamoDB', Period: config.CheckIntervalMinutes * 60, StartTime: dtStartCollection, EndTime: dtEndCollection }, function(err, data) {
        if (err) context.fail(err);
        else {
            delete data.ResponseMetadata;
            collectedData.ConsumedWriteCapacityUnits = data.Datapoints;
            completeCollection(context, collectedData, asyncManager, dtEndCollection);
        }
    });

    asyncManager.totalAsyncCalls++;
    cloudwatch.getMetricStatistics({ MetricName: 'ThrottledRequests', Statistics: [ 'Sum' ], Unit: 'Count', Dimensions: [ { Name: 'TableName', Value: config.TableName } ], Namespace: 'AWS/DynamoDB', Period: config.CheckIntervalMinutes * 60, StartTime: dtStartCollection, EndTime: dtEndCollection }, function(err, data) {
        if (err) {
            // No need to fail on this call. Just pretend we received no throttles.
            collectedData.ThrottledRequests = [];
        }
        else {
            delete data.ResponseMetadata;
            collectedData.ThrottledRequests = data.Datapoints;
        }
        collectedData.ThrottledRequestCount = collectedData.ThrottledRequests.length;
        console.log(collectedData.ThrottledRequestCount + " throttled requests");
        completeCollection(context, collectedData, asyncManager, dtEndCollection);
    });

};

function completeCollection(context, collectedData, asyncManager, dtStart) {
    asyncManager.callbackCount++;
    if (asyncManager.callbackCount == asyncManager.totalAsyncCalls) {
        var response = { Action: { ScaleDirection: "None" },
            ReadLowerLimit: roundNumber(collectedData.ProvisionedThroughput.ReadCapacityUnits * config.Reads.Threshold.Lower, 1),
            ReadUpperLimit: roundNumber(collectedData.ProvisionedThroughput.ReadCapacityUnits * config.Reads.Threshold.Upper, 1),
            WriteUpperLimit: roundNumber(collectedData.ProvisionedThroughput.WriteCapacityUnits * config.Writes.Threshold.Upper, 1),
            WriteLowerLimit: roundNumber(collectedData.ProvisionedThroughput.WriteCapacityUnits * config.Writes.Threshold.Lower, 1),
            ReadSamples: { AboveThreshold: 0, BelowThreshold: 0, Min: config.Reads.Provisioned.Max, Max: -1, Avg: 0, WeightedAvg: 0, Data: [] },
            WriteSamples: { AboveThreshold: 0, BelowThreshold: 0, Min: config.Writes.Provisioned.Max, Max: -1, Avg: 0, WeightedAvg: 0, Data: [] },
            StartDate: dtStart
        };

        var avgSum = 0;
        var avgSumWeighted = 0;
        var weightedSumDivisor = 0;
        for (var i = 0; i < collectedData.ConsumedReadCapacityUnits.length; i++) {
            response.ReadSamples.Data.push(roundNumber(collectedData.ConsumedReadCapacityUnits[i].Sum / (config.CheckIntervalMinutes * 60), 1));
            response.ReadSamples.Max = Math.max(response.ReadSamples.Max, response.ReadSamples.Data[i]);
            response.ReadSamples.Min = Math.min(response.ReadSamples.Min, response.ReadSamples.Data[i]);
            avgSum += response.ReadSamples.Data[i];
            avgSumWeighted += response.ReadSamples.Data[i] * Math.pow(i, 2);
            weightedSumDivisor += Math.pow(i, 2);
            if (response.ReadSamples.Data[i] >= response.ReadUpperLimit)
                response.ReadSamples.AboveThreshold++;
            else if (response.ReadSamples.Data[i] <= response.ReadLowerLimit)
                response.ReadSamples.BelowThreshold++;
        }
        if (avgSum > 0)
            response.ReadSamples.Avg = roundNumber(avgSum / response.ReadSamples.Data.length, 1);
        if (avgSumWeighted > 0)
            response.ReadSamples.WeightedAvg = roundNumber(avgSumWeighted / weightedSumDivisor, 1);

        avgSum = 0;
        avgSumWeighted = 0;
        weightedSumDivisor = 0;
        for (i = 0; i < collectedData.ConsumedWriteCapacityUnits.length; i++) {
            response.WriteSamples.Data.push(roundNumber(collectedData.ConsumedWriteCapacityUnits[i].Sum / (config.CheckIntervalMinutes * 60), 1));
            response.WriteSamples.Max = Math.max(response.WriteSamples.Max, response.WriteSamples.Data[i]);
            response.WriteSamples.Min = Math.min(response.WriteSamples.Min, response.WriteSamples.Data[i]);
            avgSum += response.WriteSamples.Data[i];
            avgSumWeighted += response.WriteSamples.Data[i] * Math.pow(i, 2);
            weightedSumDivisor += Math.pow(i, 2);
            if (response.WriteSamples.Data[i] >= response.WriteUpperLimit)
                response.WriteSamples.AboveThreshold++;
            else if (response.WriteSamples.Data[i] <= response.WriteLowerLimit)
                response.WriteSamples.BelowThreshold++;
        }
        if (avgSum > 0)
            response.WriteSamples.Avg = roundNumber(avgSum / response.WriteSamples.Data.length, 1);
        if (avgSumWeighted > 0)
            response.WriteSamples.WeightedAvg = roundNumber(avgSumWeighted / weightedSumDivisor, 1);

        if (response.ReadSamples.AboveThreshold >= config.Reads.NumChecksBeforeScaleUp || response.WriteSamples.AboveThreshold >= config.Writes.NumChecksBeforeScaleUp || collectedData.ThrottledRequests.length > config.Reads.NumThrottledBeforeScaleUp) {
            response.Action.ScaleDirection = "Up";
            if (response.ReadSamples.AboveThreshold < config.Reads.NumChecksBeforeScaleUp && collectedData.ThrottledRequests.length < config.Reads.NumThrottledBeforeScaleUp)
                response.Action.NewReads = collectedData.ProvisionedThroughput.ReadCapacityUnits;
            else {
                response.Action.NewReads = roundNumber(collectedData.ProvisionedThroughput.ReadCapacityUnits * (1.0 + config.Reads.Rate.Increase), 1);
                response.Action.NewReads = Math.min(response.Action.NewReads, roundNumber(response.ReadSamples.WeightedAvg * (1.0 + config.Reads.Rate.Increase), 1));
                response.Action.NewReads = Math.min(response.Action.NewReads, config.Reads.Provisioned.Max);
                response.Action.NewReads = Math.max(response.Action.NewReads, collectedData.ProvisionedThroughput.ReadCapacityUnits);
            }

            if (response.WriteSamples.AboveThreshold < config.Writes.NumChecksBeforeScaleUp)
                response.Action.NewWrites = collectedData.ProvisionedThroughput.WriteCapacityUnits;
            else {
                response.Action.NewWrites = roundNumber(collectedData.ProvisionedThroughput.WriteCapacityUnits * (1.0 + config.Writes.Rate.Increase), 1);
                response.Action.NewWrites = Math.min(response.Action.NewWrites, roundNumber(response.WriteSamples.WeightedAvg * (1.0 + config.Reads.Rate.Increase), 1));
                response.Action.NewWrites = Math.min(response.Action.NewWrites, config.Writes.Provisioned.Max);
                response.Action.NewWrites = Math.max(response.Action.NewWrites, collectedData.ProvisionedThroughput.WriteCapacityUnits);
            }

            // If all of the checking did not create an increase switch in back to None
            if (response.Action.NewReads <= collectedData.ProvisionedThroughput.ReadCapacityUnits && response.Action.NewWrites <= collectedData.ProvisionedThroughput.WriteCapacityUnits)
                response.Action.ScaleDirection = "None";
        }
        else if (response.ReadSamples.BelowThreshold >= config.NumChecksBeforeScaleDown || response.WriteSamples.BelowThreshold >= config.NumChecksBeforeScaleDown) {
            response.Action.ScaleDirection = "Down";
            response.Action.NewReads = Math.max(roundNumber(collectedData.ProvisionedThroughput.ReadCapacityUnits * (1.0 - config.Reads.Rate.Decrease), 1), response.ReadSamples.WeightedAvg);
            response.Action.NewReads = Math.max(response.Action.NewReads, config.Reads.Provisioned.Min);
            response.Action.NewReads = Math.min(response.Action.NewReads, collectedData.ProvisionedThroughput.ReadCapacityUnits);

            response.Action.NewWrites = Math.max(roundNumber(collectedData.ProvisionedThroughput.WriteCapacityUnits * (1.0 - config.Writes.Rate.Decrease), 1), response.WriteSamples.WeightedAvg);
            response.Action.NewWrites = Math.max(response.Action.NewWrites, config.Writes.Provisioned.Min);
            response.Action.NewWrites = Math.min(response.Action.NewWrites, collectedData.ProvisionedThroughput.WriteCapacityUnits);

            // If all of the checking did not create an decrease switch in back to None
            if (response.Action.NewReads >= collectedData.ProvisionedThroughput.ReadCapacityUnits && response.Action.NewWrites >= collectedData.ProvisionedThroughput.WriteCapacityUnits)
                response.Action.ScaleDirection = "None";
        }

        response.Action.NewReads = Math.round(response.Action.NewReads);
        response.Action.NewWrites = Math.round(response.Action.NewWrites);

        if (response.Action.ScaleDirection == "Down") {
            // On a decrease make sure that we are not decreasing too often
            if (collectedData.ProvisionedThroughput.NumberOfDecreasesToday >= config.MaxDecreasesPerDay)
                response.Action.ScaleError = "Maximum decreases already used. " + (Math.round((new Date().setHours(24, 0, 0, 0) - new Date()) / 6e4)) + " minutes until reset.";

            // Throttle decreases through the day so they are not all used up early
            else if (collectedData.ProvisionedThroughput.NumberOfDecreasesToday * (24 / config.MaxDecreasesPerDay) > new Date().getHours())
                response.Action.ScaleError = "Too many decreases (" + collectedData.ProvisionedThroughput.NumberOfDecreasesToday + ") already used today. Next available at " + (collectedData.ProvisionedThroughput.NumberOfDecreasesToday * (24 / config.MaxDecreasesPerDay)) + ":00 UTC.";

            // Throttle decreases to once per hour
            else if (new Date() - new Date(collectedData.ProvisionedThroughput.LastDecreaseDateTime) < config.MinWaitMinutesBetweenScaleDowns * 60000)
                response.Action.ScaleError = "Waiting " + (Math.round((new Date(collectedData.ProvisionedThroughput.LastDecreaseDateTime).getTime() + config.MinWaitBetweenScaleDownsMinutes * 60000 - new Date().getTime()) / 60000)) + " minutes for next scale down window";
        }

        if (config.Debug) {
            response.DynamoDBTable = collectedData.DynamoDBTable;
            response.ProvisionedThroughput = collectedData.ProvisionedThroughput;
            if (collectedData.ThrottledRequests.length > 0)
                response.ThrottledRequests = collectedData.ThrottledRequests;
        }

        response.Action.CurrentReads = collectedData.ProvisionedThroughput.ReadCapacityUnits;
        response.Action.CurrentWrites = collectedData.ProvisionedThroughput.WriteCapacityUnits;

        if ((response.Action.ScaleDirection == "Up" || response.Action.ScaleDirection == "Down") && !response.Action.ScaleError) {
            if (config.DryRun) {
                response.DryRun = true;
                contextSucceed(context, response);
            }
            else {
                // Scale the table
                var dynamodb = new aws.DynamoDB();
                dynamodb.updateTable({ TableName: config.TableName, ProvisionedThroughput: { ReadCapacityUnits: response.Action.NewReads, WriteCapacityUnits: response.Action.NewWrites } }, function(err, data) {
                    if (err) context.fail(err);
                    else {
                        console.log(config.TableName + " --SCALED " + response.Action.ScaleDirection.toUpperCase() + "-- to " + response.Action.NewReads + "/" + response.Action.NewWrites + " Pr:" + collectedData.ProvisionedThroughput.ReadCapacityUnits + "/" + collectedData.ProvisionedThroughput.WriteCapacityUnits + " Avg:" + response.ReadSamples.Avg + "/" + response.WriteSamples.Avg + " WeightedAvg:" + response.ReadSamples.WeightedAvg + "/" + response.WriteSamples.WeightedAvg);
                        response.DynamoUpdateResponse = data;
                        contextSucceed(context, response);
                    }
                });
            }
        }
        else {
            if (response.Action.ScaleError) {
                console.log(response.Action.ScaleError);
                console.log(response.Action.NewReads + "/" + response.Action.NewWrites + " Pr:" + collectedData.ProvisionedThroughput.ReadCapacityUnits + "/" + collectedData.ProvisionedThroughput.WriteCapacityUnits + " Avg:" + response.ReadSamples.Avg + "/" + response.WriteSamples.Avg + " WeightedAvg:" + response.ReadSamples.WeightedAvg + "/" + response.WriteSamples.WeightedAvg);
            }
            else
                console.log("Nothing to do Pr:" + collectedData.ProvisionedThroughput.ReadCapacityUnits + "/" + collectedData.ProvisionedThroughput.WriteCapacityUnits + " Avg:" + response.ReadSamples.Avg + "/" + response.WriteSamples.Avg);
            contextSucceed(context, response);
        }
    }
}

function roundNumber(num, decPlaces) {
    if (num < 1 && decPlaces == 1)
        decPlaces = 2;
    return Number(Math.round(num + 'e' + decPlaces) + 'e-' + decPlaces);
}

function contextSucceed(context, response) {
    response.RunTime = new Date() - response.StartDate;
    console.log(response);
    cacheStuff.lastDate = new Date();
    cacheStuff.containerRunCount++;
    
    if (config.Debug)
        context.succeed(response);
    else
        context.succeed(response.Action);
}
