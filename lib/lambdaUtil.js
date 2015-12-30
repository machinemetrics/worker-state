var _ = require('lodash'),
  KinesisUtil = require('./kinesisUtil');

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

module.exports = LambdaUtil;