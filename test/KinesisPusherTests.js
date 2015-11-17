process.env['AWS_REGION'] = 'us-west-2';

var _ = require('lodash'),
    Q = require('q'),
    should = require('should'),
    KinesisPusher = require('../lib').KinesisPusher,
    KinesisUtil = require('../lib/kinesisUtil'),
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
    var worker = new WorkerState(WorkerState.DefaultTable, 'test1', 'shardId-000000000000');
    var pusher = new KinesisPusher(worker, 'stream1', services.kinesis);

    return pusher.pushRecords(producer.generate(100), 10).then(function () {
      return producer.validateStream(services.kinesis, 'stream1', 'shardId-000000000000').then(function (result) {
        result.should.be.true();
      });
    }).then(function () {
      return worker.expungeAllKnownSavedState();
    });
  });


});

describe('Record Culling', function () {
  var worker, producer, pusher;

  before(function () {
    return services.initKinesis({stream: 'stream1'}).then(function () {
      worker = new WorkerState(WorkerState.DefaultTable, 'testcull', 'shardId-000000000000');
      pusher = new KinesisPusher(worker, 'stream1', services.kinesis);
      producer = new DataServices.RecordProducer(1);
    });
  });

  after(function () {
    worker.expungeAllKnownSavedState();
  });

  it.only('Should cull all records from empty worker on 1 shard', function () {
    var kutil = new KinesisUtil(services.kinesis);
    var records = producer.generate(100);

    return kutil.pushRecords('stream1', records).then(function (maxSeqs) {
      return pusher.cullPreviouslyPushedRecords(records).then(function (filtered) {
        filtered.length.should.equal(0);
      });
    });
  });

  
});