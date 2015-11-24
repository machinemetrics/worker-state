process.env['AWS_REGION'] = 'us-west-2';

var _ = require('lodash'),
    Q = require('q'),
    should = require('should'),
    WorkerState = require('../lib').KinesisWorkerState,
    TestServices = require('./util/TestServices');

var services = new TestServices();

describe('Parent resolution', function () {
  before(function () {
    return Q.all([
      services.initKinesis({ stream: 'kwsstream1' }),
      services.initDynamo({ table: WorkerState.DefaultTable })
    ]);
  });

  it('Should correctly report parents through split and merge', function () {
    return services.getAnyOpenShard('kwsstream1').then(function (shardId) {
      var worker = new WorkerState(WorkerState.DefaultTable, 'test1', shardId, 'kwsstream1', services.kinesis, services.dynamodb);
      return worker.getParentShards().then(function (parents) {
        parents.length.should.equal(0);
        return services.splitShard('kwsstream1', shardId);
      }).then(function (children) {
        var worker2 = new WorkerState(WorkerState.DefaultTable, 'test1', children[0], 'kwsstream1', services.kinesis, services.dynamodb);
        var worker3 = new WorkerState(WorkerState.DefaultTable, 'test1', children[1], 'kwsstream1', services.kinesis, services.dynamodb);
        return worker2.getParentShards().then(function (parents) {
          parents.length.should.equal(1);
          parents[0].should.equal(shardId);
          return worker3.getParentShards();
        }).then(function (parents) {
          parents.length.should.equal(1);
          parents[0].should.equal(shardId);
          return worker.getParentShards();
        }).then(function (parents) {
          parents.length.should.equal(0);
          return services.mergeShards('kwsstream1', children[0], children[1]);
        }).then(function (child) {
          var worker4 = new WorkerState(WorkerState.DefaultTable, 'test1', child, 'kwsstream1', services.kinesis, services.dynamodb);
          return worker4.getParentShards().then(function (parents) {
            parents.length.should.equal(2);
            children.should.matchAny(parents[0]);
            children.should.matchAny(parents[1]);
          });
        });
      });
    });
  });
});