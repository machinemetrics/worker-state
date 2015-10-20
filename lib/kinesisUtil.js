var md5 = require('md5');

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
  var hash = md5(partitionKey);

  return _(shards).filter(function (shard) {
    return hash >= shard.HashKeyRange.StartingHashKey && shard <= shard.HashKeyRange.EndingHashKey;
  }).sortBy('SequenceNumberRange.StartingSequenceNumber').value();
};

module.exports = KinesisUtil;