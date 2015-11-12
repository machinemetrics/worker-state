var md5 = require('md5'),
    async = require('async-q'),
    bigInt = require('big-integer'),
    _ = require('lodash');

function KinesisUtil (kinesis) {
  this.kinesis = kinesis || new AWS.Kinesis();
}

KinesisUtil.prototype.getShards = function (stream) {
  var self = this;
  var params = {
    StreamName: stream
  };

  var processMoreRecords = true;
  var shards = [];

  return async.whilst(function () {
    return processMoreRecords;
  }, function () {
    return self.kinesis.describeStream(params).q().then(function (data) {
      shards = shards.concat(data.StreamDescription.Shards);
      if (data.HasMoreShards)
        params.ExclusiveStartShardId = _.last(data.StreamDescription.Shards).ShardId;
      else
        processMoreRecords = false;
    });
  }).then(function () {
    return _.indexBy(shards, 'ShardId');
  });
};

KinesisUtil.prototype.filterWriteableShards = function (shards) {
  return _.filter(shards, function (shard) {
    return !_.get(shard, 'SequenceNumberRange.EndingSequenceNumber');
  });
};

KinesisUtil.prototype.getShardLineage = function (shards, partitionKey) {
  var hash = bigInt(md5(partitionKey), 16);

  return _(shards).filter(function (shard) {
    return hash.geq(parseHashKey(shard.HashKeyRange.StartingHashKey)) && hash.leq(parseHashKey(shard.HashKeyRange.EndingHashKey));
  }).sortBy('SequenceNumberRange.StartingSequenceNumber').value();
};

function parseHashKey (hash) {
  // A hack to get around (Kinesalite?) setting hashes in e+ format after a split.  It still contains all the digits.
  if (hash.indexOf('e+') > -1) {
    var exp = parseInt(hash.match(/e\+(\d+)/)[1]);
    hash = _.padRight(hash.replace(/\.|e\+\d+/g, ''), exp + 1, 0);
  }

  return bigInt(hash);
}

module.exports = KinesisUtil;