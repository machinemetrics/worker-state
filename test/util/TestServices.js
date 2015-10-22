var Q = require('q'),
    _ = require('lodash'),
    kinesalite = require('kinesalite'),
    dynalite = require('dynalite'),
    AWS = require('aws-sdk-q');

AWS.config.update({
  s3ForcePathStyle: true,
  sslEnabled: false,
  region: process.env['AWS_REGION'],
  accessKeyId: process.env['AWS_ACCESS_KEY_ID'],
  secretAccessKey: process.env['AWS_SECRET_ACCESS_KEY']
});

function TestServices() {}

TestServices.NextPort = (function() {
  var nextPort = 4567;
  return function () {
    return nextPort++;
  };
})();

TestServices.prototype.initKinesis = function(cfg) {
  var deferred = Q.defer();
  var self = this;

  var kinesaliteServer = kinesalite({
    createStreamMs: 25,
    deleteStreamMs: 25,
    updateStreamMs: 25
  });
  var port = TestServices.NextPort();
  kinesaliteServer.listen(port, function(err) {
    if(err)
      return deferred.reject(err);

    self.kinesis = new AWS.Kinesis({ endpoint: 'localhost:' + port });

    deferred.resolve(self.ensureStream(cfg.stream));
  });

  return deferred.promise;
};

TestServices.prototype.initDynamo = function(cfg) {
  var deferred = Q.defer();
  var self = this;

  var dynaliteServer = dynalite({
    createTableMs: 25,
    deleteTableMs: 25,
    updateTableMs: 25
  });
  var port = TestServices.NextPort();
  dynaliteServer.listen(port, function (err) {
    if (err)
      return deferred.reject(err);

    cfg.endpoint = new AWS.Endpoint('localhost:' + port);
    self.dynamodb = new AWS.DynamoDB();
    self.dynamodb.endpoint = cfg.endpoint;

    deferred.resolve(self.ensureWorkerStateTable(cfg.table));
  });

  return deferred.promise;
};

TestServices.prototype.ensureStream = function(streamName) {
  var self = this;

  return this.kinesis.listStreams().q().then(function (data) {
    if (!_.contains(data.StreamNames, streamName)) {
      return self.kinesis.createStream({
        ShardCount: 1,
        StreamName: streamName
      }).q().delay(50);
    }
  });
};

TestServices.prototype.ensureWorkerStateTable = function(tableName) {
  var self = this;

  return this.dynamodb.describeTable({
    TableName: tableName
  }).q().catch(function (err) {
    if (_.startsWith(err.message, 'ResourceNotFoundException')) {
      return self.dynamodb.createTable({
        AttributeDefinitions: [{
          AttributeName: 'app.key',
          AttributeType: 'S'
        }, {
          AttributeName: 'shard.key',
          AttributeType: 'S'
        }],
        KeySchema: [{
          AttributeName: 'app.key',
          KeyType: 'HASH'
        }, {
          AttributeName: 'shard.key',
          KeyType: 'RANGE'
        }],
        ProvisionedThroughput: {
          ReadCapacityUnits: 10,
          WriteCapacityUnits: 10
        },
        TableName: tableName
      }).q().delay(100);
    }
    throw err;
  });
};

TestServices.prototype.getAnyOpenShard = function(streamName) {
  return this.kinesis.describeStream({
    StreamName: streamName
  }).q().then(function (resp) {
    return _(resp.StreamDescription.Shards).reject('SequenceNumberRange.EndingSequenceNumber').map('ShardId').first();
  });
};

TestServices.prototype.splitShard = function(streamName, shardId) {
  var self = this;
  return this.kinesis.describeStream({
    StreamName: streamName
  }).q().then(function (res) {
    var shard = _(res.StreamDescription.Shards).find('ShardId', shardId);
    return self.kinesis.splitShard({
      NewStartingHashKey: averageMD5(shard.HashKeyRange.StartingHashKey, shard.HashKeyRange.EndingHashKey),
      ShardToSplit: shardId,
      StreamName: streamName
    }).q().delay(50);
  }).then(function () {
    return self.kinesis.describeStream({
      StreamName: streamName
    }).q().then(function (res) {
      return _(res.StreamDescription.Shards).filter('ParentShardId', shardId).map('ShardId').value();
    });
  });
};

TestServices.prototype.mergeShards = function(streamName, shardId1, shardId2) {
  var self = this;
  return this.kinesis.mergeShards({
    AdjacentShardToMerge: shardId2,
    ShardToMerge: shardId1,
    StreamName: streamName
  }).q().delay(50).then(function () {
    return self.kinesis.describeStream({
      StreamName: streamName
    }).q().then(function (res) {
      return _(res.StreamDescription.Shards).find(function (shard) {
        return shard.AdjacentParentShardId = shardId2 && shard.ParentShardId == shardId1;
      }).ShardId;
    });
  });
};

function averageMD5 (hash1, hash2) {
  hash1 = hash1 + _.repeat('0', Math.max(hash1.length, hash2.length) - hash1.length);
  hash2 = hash2 + _.repeat('0', Math.max(hash1.length, hash2.length) - hash2.length);
  return _(_.zip(hash1.split(''), hash2.split(''))).map(function (pair) {
    return (~~((parseInt(pair[0], 16) + parseInt(pair[1], 16)) / 2)).toString(16);
  }).value().join('').substring(0, 8);
}

module.exports = TestServices;