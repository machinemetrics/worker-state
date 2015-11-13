var AWS = require('aws-sdk-q'),
    async = require('async-q'),
    bigInt = require('big-integer'),
    _ = require('lodash'),
    WorkerState = require('./workerState'),
    KinesisUtil = require('./kinesisUtil');

function KinesisPusher (workerState, stream, kinesis) {
  this.workerState = workerState;
  this.stream = stream;
  this.util = new KinesisUtil(kinesis);
  this.kinesis = this.util.kinesis;
}

KinesisPusher.prototype.pushRecords = function (records, seq) {
  var self = this;

  records = self.util.groupAndPackRecords(records);
  if (_.isEmpty(records))
    return self.workerState.flush(seq);

  return self.util.pushRecords(self.stream, records).then(function (shardMaxSequence) {
    _.each(shardMaxSequence, function (shardSeq, shard) {
      self.workerState.setSubValue(self.stream, shard, shardSeq);
    });
    return self.workerState.flush(seq);
  });
};

KinesisPusher.prototype.cullPreviouslyPushedRecords = function (records) {
  var self = this;
  records = _.groupBy(records, 'PartitionKey');

  return async.parallel([
    function () {
      return self.util.getShards(self.stream);
    },
    function () {
      return self.workerState.initialize([self.stream]);
    }
  ]).spread(function (shards) {
    var lineages = _.map(_.keys(records), function (key) {
      return _(self.util.getShardLineage(shards, key)).filter(function (shard) {
        var cp = self.workerState.getSubValue(self.stream, shard.ShardId);
        if (cp && shard.SequenceNumberRange.EndingSequenceNumber)
          return bigInt(shard.SequenceNumberRange.EndingSequenceNumber).gte(bigInt(cp));
        return true;
      }).map('ShardId');
    });

    var shardPartitions = _.invert(lineages, true);
    var writeableShards = self.util.filterWriteableShards(_.pick(shards, _.keys(shardPartitions)));

    return self.pushMarkerToShards(writeableShards, 'MARKER', self.workerState.checkpoint).then(function (markerSeqs) {
      return async.each(shardPartitions, function (parts, shardId) {
        var iteratorParams = {
          ShardId: shardId,
          ShardIteratorType: 'TRIM_HORIZON',
          StreamName: self.stream
        };

        var shardCP = self.workerState.getSubValue(self.stream, shardId);
        if (shardCP) {
          iteratorParams.ShardIteratorType = 'AFTER_SEQUENCE_NUMBER';
          iteratorParams.StartingSequenceNumber = shardCP;
        }

        var terminalSeq = markerSeqs[shardId] || _.get(shards[shardId], 'SequenceNumberRange.EndingSequenceNumber');
        return self.kinesis.getShardIterator(iteratorParams).q().then(function (data) {
          var iterator = data.ShardIterator;
          var lastSeq = '';

          return async.whilst(function () {
            return lastSeq < terminalSeq;
          }, function () {
            return self.kinesis.getRecords({
              ShardIterator: iterator
            }).q().then(function (data) {
              iterator = data.NextShardIterator;
              if (_.isEmpty(data.Records))
                return;

              lastSeq = _.last(data.Records).SequenceNumber;
              if (!iterator && bigInt(lastSeq).lt(bigInt(terminalSeq)))
                lastSeq = terminalSeq;

              var item = record.Data.toString('utf8');
              _.each(parts, function (part) {
                _.remove(records[part], 'Data', item);
              });
            });
          });
        });
      });
    });
  }).then(function () {
    return _.flatten(records);
  });
};

KinesisPusher.prototype.pushMarkerToShards = function (shards, marker, seq) {
  var data = _.map(shards, function (shard) {
    return {
      Data: new Buffer(seq),
      PartitionKey: marker,
      ExplicitHashKey: shard.HashKeyRange.StartingHashKey
    };
  });

  return this.util.pushRecords(this.stream, data);
};

module.exports = KinesisPusher;