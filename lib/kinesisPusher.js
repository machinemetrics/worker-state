var async = require('async-q'),
    bigInt = require('big-integer'),
    _ = require('lodash'),
    KinesisUtil = require('./kinesisUtil');

function KinesisPusher (workerState, stream, kinesis) {
  this.workerState = workerState;
  this.stream = stream;
  this.util = new KinesisUtil(kinesis);
  this.kinesis = this.util.kinesis;
}

KinesisPusher.pushKey = '__pushmark';

KinesisPusher.prototype.appKey = function () {
  return this.stream;
};

KinesisPusher.prototype.pushRecords = function (records, seq) {
  var self = this;

  records = self.util.groupAndPackRecords(records);
  if (_.isEmpty(records))
    return;

  return self.workerState.readFromWorkerState(self.appKey(), KinesisPusher.pushKey, seq).then(function (data) {
    if (data.sequence == seq)
      return self.cullPreviouslyPushedRecords(records);
    return records;
  }).then(function (records) {
    return self.workerState.writeToWorkerState(self.appKey(), KinesisPusher.pushKey, null, null, seq).then(function () {
      return records;
    })
  }).then(function (records) {
    return self.util.pushRecords(self.stream, records).then(function (shardMaxSequence) {
      _.each(shardMaxSequence, function (shardSeq, shard) {
        self.workerState.setSubValue(self.appKey(), shard, shardSeq);
      });
    });
  });
};

KinesisPusher.prototype.cullPreviouslyPushedRecords = function (records) {
  var self = this;
  records = _.groupBy(records, 'PartitionKey');

  return self.util.getShards(self.stream).then(function (shards) {
    var lineages = _.map(_.keys(records), function (key) {
      return _(self.util.getShardLineage(shards, key)).filter(function (shard) {
        var cp = self.workerState.getSubValue(self.stream, shard.ShardId);
        if (cp && shard.SequenceNumberRange.EndingSequenceNumber)
          return bigInt(shard.SequenceNumberRange.EndingSequenceNumber).gte(bigInt(cp));
        return true;
      }).map('ShardId').value();
    });

    var shardPartitions = _(lineages).flatten().uniq().value();
    var writeableShards = self.util.filterWriteableShards(_.pick(shards, shardPartitions));

    return self.pushMarkerToShards(writeableShards, 'MARKER', self.workerState.checkpoint).then(function (markerSeqs) {
      return async.each(shardPartitions, function (shardId) {
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

        var terminalSeq = bigInt(markerSeqs[shardId] || _.get(shards[shardId], 'SequenceNumberRange.EndingSequenceNumber') || 0);
        return self.kinesis.getShardIterator(iteratorParams).q().then(function (data) {
          var iterator = data.ShardIterator;
          var lastSeq = bigInt(0);

          return async.whilst(function () {
            return lastSeq.lt(terminalSeq);
          }, function () {
            return self.kinesis.getRecords({
              ShardIterator: iterator
            }).q().then(function (data) {
              iterator = data.NextShardIterator;
              if (_.isEmpty(data.Records)) {
                if (!iterator)
                  lastSeq = terminalSeq;
                return;
              }

              lastSeq = bigInt(_.last(data.Records).SequenceNumber);
              if (!iterator && lastSeq.lt(terminalSeq))
                lastSeq = terminalSeq;

              var recordsBeforeMarker = _.filter(data.Records, function (r) {
                return bigInt(r.SequenceNumber).lt(terminalSeq);
              });

              var items = _.groupBy(self.util.unpackRecords(recordsBeforeMarker), 'PartitionKey');
              records = _.mapValues(records, function (recordSet, key) {
                if (!items[key])
                  return recordSet;
                return self.util.filterRecords(recordSet, items[key]);
              });
            });
          });
        });
      });
    });
  }).then(function () {
    return _(records).values().flatten().value();
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