var _ = require('lodash'),
  Q = require('q'),
  KinesisUtil = require('./kinesisUtil');

require('q-repeat');

function LambdaUtil () {
  this.kinesisUtil = new KinesisUtil();
}

LambdaUtil.prototype.unpackKinesisRecords = function (records) {
  records = _.map(records, function (rec) {
    return {
      Data: new Buffer(rec.kinesis.data, 'base64'),
      SequenceNumber: rec.kinesis.sequenceNumber,
      PartitionKey: rec.kinesis.partitionKey
    };
  });

  return this.kinesisUtil.unpackRecords(records);
};

LambdaUtil.prototype.formatKinesisRecords = function (records, shard, stream, region, account) {
  return _.map(records, function (record) {
    return {
      eventID: shard + ':' + record.SequenceNumber,
      eventVersion: '1.0',
      kinesis: {
        partitionKey: record.PartitionKey,
        data: record.Data,
        kinesisSchemaVersion: '1.0',
        sequenceNumber: record.SequenceNumber
      },
      eventName: 'aws:kinesis:record',
      eventSourceARN: 'arn:aws:kinesis:' + region + ':' + account + ':stream/' + stream,
      eventSource: 'aws:kinesis',
      awsRegion: 'us-west-2'
    };
  });
};

LambdaUtil.prototype.runLambdaKinesisHarness = function (app, workerState, config) {
  var self = this;
  return Q.forever(function () {
    return workerState.initialize().then(function () {
      var params = {
        ShardId: shard,
        ShardIteratorType: config.defaultIteratorType || 'LATEST',
        StreamName: stream
      };

      if (workerState.checkpoint) {
        params.ShardIteratorType = 'AFTER_SEQUENCE_NUMBER';
        params.StartingSequenceNumber = workerState.checkpoint;
      }

      return self.kinesisUtil.kinesis.getShardIterator(params).q().then(function (shardData) {
        var iterator = shardData.ShardIterator;
        return Q.forever(function () {
          return self.kinesisUtil.kinesis.getRecords({
            ShardIterator: iterator,
            Limit: config.limit || 100
          }).q().then(function (recordData) {
            iterator = recordData.NextShardIterator;
            var records = self.formatKinesisRecords(recordData.Records, config.shard, config.stream, config.region, config.account);
            return app.handler({Records: records}, context);
          });
        }).catch(function (err) {
          console.log(err.stack);
        });
      });
    });
  });
};

module.exports = LambdaUtil;