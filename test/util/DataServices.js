var _ = require('lodash'),
    async = require('async-q'),
    md5 = require('md5');

function RecordProducer (partitionCount) {
  this.partitions = _.times(partitionCount, function (n) {
    return md5(n);
  });
  this.index = 0;
  this.records = [];
}

RecordProducer.prototype.generate = function (count) {
  var self = this;
  var recordSet = _.times(count, function () {
    return {
      PartitionKey: _.sample(self.partitions),
      Data: self.index++
    };
  });

  this.records = this.records.concat(recordSet);
  return recordSet;
};

RecordProducer.prototype.generateRoundRobin = function (count) {
  var self = this;
  var pIndex = 0;
  var recordSet = _.times(count, function () {
    if (pIndex >= self.partitions.length)
      pIndex = 0;
    return {
      PartitionKey: self.partitions[pIndex++],
      Data: self.index++
    };
  });

  this.records = this.records.concat(recordSet);
  return recordSet;
};

RecordProducer.prototype.validateStream = function (kinesis, stream, shard, seq) {
  var self = this;
  var iteratorParams = {
    ShardId: shard,
    ShardIteratorType: 'TRIM_HORIZON',
    StreamName: stream
  };

  if (seq) {
    iteratorParams.ShardIteratorType = 'AFTER_SEQUENCE_NUMBER';
    iteratorParams.StartingSequenceNumber = seq;
  }

  var recs = [];
  return kinesis.getShardIterator(iteratorParams).q().then(function (data) {
    var iterator = data.ShardIterator;

    return async.whilst(function () {
      return  iterator && iterator.length > 0;
    }, function () {
      return kinesis.getRecords({
        ShardIterator: iterator
      }).q().then(function (data) {
        iterator = data.NextShardIterator;
        if (_.isEmpty(data.Records))
          iterator = undefined;
        Array.prototype.push.apply(recs, data.Records);
      });
    });
  }).then(function () {
    var A = _.groupBy(self.records, 'PartitionKey');
    var B = _.groupBy(recs, 'PartitionKey');
    if (_.xor(_.keys(A), _.keys(B)).length > 0)
      return false;

    var failures = _(self.partitions).map(function (key) {
      var pairs = _.zip(A[key], B[key]);
      return _.filter(pairs, function (pair) {
        return pair[0].Data != pair[1].Data.toString('utf8');
      });
    }).flatten().value();

    return _.isEmpty(failures);
  });
};

module.exports = {
  RecordProducer: RecordProducer
};