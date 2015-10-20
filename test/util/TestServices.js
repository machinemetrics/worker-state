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

module.exports = TestServices;