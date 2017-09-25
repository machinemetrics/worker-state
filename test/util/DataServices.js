const _ = require('lodash');
const async = require('async-q');
const md5 = require('md5');
const KinesisUtil = require('../../lib/kinesisUtil');

function RecordProducer(partitionCount) {
  if (_.isArray(partitionCount))
    this.partitions = partitionCount;
  else {
    this.partitions = _.times(partitionCount, function (n) {
      return md5(n);
    });
  }

  this.index = 0;
  this.records = [];
}

RecordProducer.prototype.generate = function (count) {
  var self = this;
  var recordSet = _.times(count, function () {
    return {
      PartitionKey: _.sample(self.partitions),
      Data: self.index++,
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
      Data: self.index++,
    };
  });

  this.records = this.records.concat(recordSet);
  return recordSet;
};

RecordProducer.prototype.validateStream = function (kinesis, stream, shard, options) {
  var self = this;
  var util = new KinesisUtil(kinesis);
  var iteratorParams = {
    ShardId: shard,
    ShardIteratorType: 'TRIM_HORIZON',
    StreamName: stream,
  };

  options = options || {};
  if (options.seq) {
    iteratorParams.ShardIteratorType = 'AFTER_SEQUENCE_NUMBER';
    iteratorParams.StartingSequenceNumber = options.seq;
  }

  var recs = [];
  return kinesis.getShardIterator(iteratorParams).q().then(function (data) {
    var iterator = data.ShardIterator;

    return async.whilst(function () {
      return iterator && iterator.length > 0;
    }, function () {
      return kinesis.getRecords({
        ShardIterator: iterator,
      }).q().then(function (data) {
        iterator = data.NextShardIterator;
        if (_.isEmpty(data.Records))
          iterator = undefined;
        Array.prototype.push.apply(recs, data.Records);
      });
    });
  }).then(function () {
    recs = util.unpackRecords(recs, false);
    if (options.packCounts) {
      if (options.packCounts.length !== recs.length)
        return false;

      var countsMatch = _.every(_.zip(recs, options.packCounts), function (item) {
        return item[0].length === item[1];
      });
      if (!countsMatch)
        return false;
    }

    recs = _.flatten(recs);
    var A = _.groupBy(self.records, 'PartitionKey');
    var B = _.groupBy(recs, 'PartitionKey');
    delete B.MARKER;

    if (options.partitions)
      A = _.pick(A, options.partitions);

    if (_.xor(_.keys(A), _.keys(B)).length > 0)
      return false;

    var failures = _(self.partitions).map(function (key) {
      if (options.partitions && !_.includes(options.partitions, key))
        return [];
      if (A[key].length !== B[key].length)
        return false;

      var pairs = _.zip(A[key], B[key]);
      return _.filter(pairs, function (pair) {
        return pair[0].Data !== pair[1].Data;
      });
    }).flatten().value();

    return _.isEmpty(failures);
  });
};

module.exports = {
  RecordProducer: RecordProducer,
};
