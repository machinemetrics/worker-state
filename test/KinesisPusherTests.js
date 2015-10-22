process.env['AWS_REGION'] = 'us-west-2';

var _ = require('lodash'),
    Q = require('q'),
    should = require('should'),
    KinesisPusher = require('../lib').KinesisPusher,
    WorkerState = require('../lib').WorkerState,
    TestServices = require('./util/TestServices'),
    DataServices = require('./util/DataServices');

var services = new TestServices();

describe('Something', function () {
  before(function () {
    return Q.all([
      services.initKinesis({ stream: 'stream1' }),
      //services.initDynamo({ table: WorkerState.DefaultTable })
    ]);
  });

  it('Pushes some records of a single key', function () {
    var producer = new DataServices.RecordProducer(1);
    var worker = new WorkerState(WorkerState.DefaultTable, 'test1', 'shardId-000000000000', services.dynamodb);
    var pusher = new KinesisPusher(worker, 'stream1', services.kinesis);

    return pusher.pushRecords(producer.generate(100), 10).then(function () {
      return producer.validateStream(services.kinesis, 'stream1', 'shardId-000000000000').then(function (result) {
        result.should.be.true();
      });
    });
  });
});