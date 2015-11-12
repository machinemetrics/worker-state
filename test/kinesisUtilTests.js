var _ = require('lodash'),
    should = require('should'),
    KinesisUtil = require('../lib/kinesisUtil');
    TestServices = require('./util/TestServices');

var services = new TestServices();

describe('Something', function () {
  before(function () {
    return services.initKinesis({ stream: 'stream1' });
  });

  it('Should get 1 shard from stream', function () {
    var kutil = new KinesisUtil(services.kinesis);
    return kutil.getShards('stream1').then(function (shards) {
      should.exist(shards);
      shards.should.have.property('shardId-000000000000');
    });
  });

  it('Should get 50 shards from stream', function () {
    var kutil = new KinesisUtil(services.kinesis);
    return services.kinesis.createStream({
      ShardCount: 50,
      StreamName: 'stream2'
    }).q().delay(50).then(function () {
      return kutil.getShards('stream2').then(function (shards) {
        should.exist(shards);
        _.keys(shards).length.should.equal(50);
      });
    });
  });

  it('Should have correct writable shards', function () {
    var kutil = new KinesisUtil(services.kinesis);
    return services.kinesis.createStream({
      ShardCount: 2,
      StreamName: 'stream2'
    }).q().delay(50).then(function () {
      return services.splitShard('stream2', 'shardId-000000000000');
    }).then(function () {
      return kutil.getShards('stream2');
    }).then(function (shards) {
      var writable = kutil.filterWriteableShards(shards);
      _.keys(shards).length.should.equal(4);
      _.keys(writable).length.should.equal(3);
      _.indexBy(writable, 'ShardId').should.not.have.property('shardId-000000000000');
    });
  });

  it('Should identify shard lineage', function () {
    var kutil = new KinesisUtil(services.kinesis);
    return services.splitShard('stream1', 'shardId-000000000000').then(function () {
      return services.splitShard('stream1', 'shardId-000000000002');
    }).then(function () {
      return services.mergeShards('stream1', 'shardId-000000000001', 'shardId-000000000003');
    }).then(function () {
      return kutil.getShards('stream1');
    }).then(function (shards) {
      _.map(kutil.getShardLineage(shards, 'alpha'), 'ShardId').should.containDeepOrdered(['shardId-000000000000', 'shardId-000000000001', 'shardId-000000000005']);
      _.map(kutil.getShardLineage(shards, 'bravo'), 'ShardId').should.containDeepOrdered(['shardId-000000000000', 'shardId-000000000002', 'shardId-000000000004']);
      _.map(kutil.getShardLineage(shards, 'charlie'), 'ShardId').should.containDeepOrdered(['shardId-000000000000', 'shardId-000000000002', 'shardId-000000000003', 'shardId-000000000005']);
    });
  });
});