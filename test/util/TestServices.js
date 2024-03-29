const Q = require('q');
const _ = require('lodash');
const bigInt = require('big-integer');
const kinesalite = require('kinesalite');
const dynalite = require('dynalite');
const AWS = require('aws-sdk-q');
const https = require('https');

// Kinesalite is self-signed and normally invalid for node, so ignore it.
process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0';

AWS.config.update({
  s3ForcePathStyle: true,
  sslEnabled: true,
  region: process.env.AWS_REGION,
  accessKeyId: process.env.AWS_ACCESS_KEY_ID,
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
});

function TestServices() {}

TestServices.NextPort = (function () {
  var nextPort = 4567;
  return function () {
    return nextPort++;
  };
})();

TestServices.prototype.initKinesis = function (cfg) {
  var deferred = Q.defer();
  var self = this;

  self.kinesaliteServer = kinesalite({
    ssl: true,
    createStreamMs: 25,
    deleteStreamMs: 25,
    updateStreamMs: 25,
    shardLimit: 1000,
  });
  var port = TestServices.NextPort();
  self.kinesaliteServer.listen(port, function (err) {
    if (err) {
      return deferred.reject(err);
    }

    AWS.config.httpOptions = { agent: new https.Agent() };
    self.kinesis = new AWS.Kinesis({ endpoint: `https://localhost:${port}` });

    deferred.resolve(self.ensureStream(cfg.stream));
  });

  return deferred.promise;
};

TestServices.prototype.stopKinesis = function () {
  var deferred = Q.defer();
  this.kinesaliteServer.close(function (err) {
    if (err)
      return deferred.reject(err);
    deferred.resolve();
  });

  return deferred.promise;
};

TestServices.prototype.initDynamo = function (cfg) {
  var deferred = Q.defer();
  var self = this;

  var dynaliteServer = dynalite({
    createTableMs: 25,
    deleteTableMs: 25,
    updateTableMs: 25,
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

TestServices.prototype.ensureStream = function (streamName) {
  var self = this;

  return self.kinesis.listStreams().q().then(function (data) {
    if (!_.contains(data.StreamNames, streamName)) {
      return self.kinesis.createStream({
        ShardCount: 1,
        StreamName: streamName,
      }).q().delay(50);
    }
  });
};

TestServices.prototype.ensureWorkerStateTable = function (tableName) {
  var self = this;

  return this.dynamodb.describeTable({
    TableName: tableName,
  }).q().catch(function (err) {
    if (_.startsWith(err.message, 'ResourceNotFoundException')) {
      return self.dynamodb.createTable({
        AttributeDefinitions: [{
          AttributeName: 'app.key',
          AttributeType: 'S',
        }, {
          AttributeName: 'shard.key',
          AttributeType: 'S',
        }],
        KeySchema: [{
          AttributeName: 'app.key',
          KeyType: 'HASH',
        }, {
          AttributeName: 'shard.key',
          KeyType: 'RANGE',
        }],
        ProvisionedThroughput: {
          ReadCapacityUnits: 10,
          WriteCapacityUnits: 10,
        },
        TableName: tableName,
      }).q().delay(100);
    }
    throw err;
  });
};

TestServices.prototype.getAnyOpenShard = function (streamName) {
  return this.kinesis.describeStream({
    StreamName: streamName,
  }).q().then(function (resp) {
    return _(resp.StreamDescription.Shards).reject('SequenceNumberRange.EndingSequenceNumber').map('ShardId').first();
  });
};

TestServices.prototype.splitShard = function (streamName, shardId) {
  var self = this;
  return this.kinesis.describeStream({
    StreamName: streamName,
  }).q().then(function (res) {
    var shard = _(res.StreamDescription.Shards).find('ShardId', shardId);
    return self.kinesis.splitShard({
      NewStartingHashKey: averageMD5(shard.HashKeyRange.StartingHashKey, shard.HashKeyRange.EndingHashKey),
      ShardToSplit: shardId,
      StreamName: streamName,
    }).q().delay(50);
  }).then(function () {
    return self.kinesis.describeStream({
      StreamName: streamName,
    }).q().then(function (res) {
      return _(res.StreamDescription.Shards).filter('ParentShardId', shardId).map('ShardId').value();
    });
  });
};

TestServices.prototype.mergeShards = function (streamName, shardId1, shardId2) {
  var self = this;
  return this.kinesis.mergeShards({
    AdjacentShardToMerge: shardId2,
    ShardToMerge: shardId1,
    StreamName: streamName,
  }).q().delay(50).then(function () {
    return self.kinesis.describeStream({
      StreamName: streamName,
    }).q().then(function (res) {
      return _(res.StreamDescription.Shards).find(function (shard) {
        return shard.AdjacentParentShardId === shardId2 && shard.ParentShardId === shardId1;
      }).ShardId;
    });
  });
};

TestServices.prototype.validateRedisSecondary = function (state) {
  var keys = state.initKeys;
  var subkeys = state.initSubkeys;

  if (subkeys && state.store.canReadSubkeyList) {
    subkeys = _.map(subkeys, function (sk) {
      return state.makeShardKey(sk);
    });
    subkeys.push(state.makeShardKey());
  } else {
    subkeys = state.makeShardKey();
  }

  state.store.readFromSecondary();

  var result = { };

  return Q.all(_.map(keys, function (key) {
    var appKey = state.makeAppKey(key);
    result[appKey] = { };

    return state.store.readItems(appKey, subkeys, true).then(function (data) {
      result[appKey].secondary = data;
    }).then(function () {
      state.store.readFromPrimary();
      return state.store.readItems(appKey, subkeys, true);
    }).then(function (data) {
      result[appKey].primary = data;
    });
  })).then(function () {
    return result;
  });
};

function averageMD5(hash1, hash2) {
  return parseHashKey(hash1).add(parseHashKey(hash2)).divide(2).toString();
}

function parseHashKey(hash) {
  // A hack to get around (Kinesalite?) setting hashes in e+ format after a split.  It still contains all the digits.
  if (hash.indexOf('e+') > -1) {
    var exp = parseInt(hash.match(/e\+(\d+)/)[1], 10);
    hash = _.padRight(hash.replace(/\.|e\+\d+/g, ''), exp + 1, 0);
  }

  return bigInt(hash);
}

module.exports = TestServices;
