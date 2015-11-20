var WorkerState = require('./workerState'),
    AWS = require('aws-sdk-q'),
    async = require('async-q'),
    _ = require('lodash');

function KinesisWorkerState (table, app, shard, stream, kinesis, dynamodb) {
  WorkerState.call(this, table, app, shard, dynamodb);
  this.stream = stream;
  this.kinesis = kinesis || new AWS.Kinesis();
}

KinesisWorkerState.prototype = _.create(WorkerState.prototype, {
  constructor: KinesisWorkerState
});

KinesisWorkerState.DefaultTable = WorkerState.DefaultTable;

KinesisWorkerState.prototype.initialize = function (keys) {
  var self = this;
  return WorkerState.prototype.initialize.call(self, keys).then(function () {
    if (!self.checkpoint.length && !_.isFinite(self.checkpoint)) {
      return self.getParentShards().then(function (parents) {
        switch (parents.length) {
          case 0:
            return;
          case 1:
            return self.splitShard(parents[0]);
          case 2:
            return self.mergeShards(parents[0], parents[1]);
        }
      });
    }
  });
};

KinesisWorkerState.prototype.makeAppKey = function (key) {
  var streamValue = this.stream;
  if (_.isFinite(key) || (_.isString(key) && key.length > 0))
    streamValue += '.' + key;
  return WorkerState.prototype.makeAppKey.call(this, streamValue);
};

KinesisWorkerState.prototype.parseAppKey = function (key) {
  var streamValue = WorkerState.prototype.parseAppKey.call(this, key);
  return _.rest(streamValue.split('.')).join('.');
};

KinesisWorkerState.prototype.getParentShards = function () {
  var self = this;
  var params = {
    StreamName: self.stream
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
    var records = _.indexBy(shards, 'ShardId');

    var shard = records[self.shard];
    return shard ? _.filter([shard.ParentShardId, shard.AdjacentParentShardId]) : [];
  });
};

module.exports = KinesisWorkerState;