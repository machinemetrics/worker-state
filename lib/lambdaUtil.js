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

module.exports = LambdaUtil;