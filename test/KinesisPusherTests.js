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

describe.only('Record Culling', function () {
  var worker, producer, pusher;

  beforeEach(function () {
    return services.initKinesis({stream: 'stream1'}).then(function () {
      worker = new WorkerState(WorkerState.DefaultTable, 'testcull', 'shardId-000000000000');
      pusher = new KinesisPusher(worker, 'stream1', services.kinesis);
      producer = new DataServices.RecordProducer(1);
    });
  });

  afterEach(function () {
    return Q.all([
      worker.expungeAllKnownSavedState(),
      services.stopKinesis()
    ]);
  });

  it('Should cull all records from empty worker on 1 shard', function () {
    var kutil = new KinesisUtil(services.kinesis);
    var records = producer.generate(100);

    return kutil.pushRecords('stream1', records).then(function (maxSeqs) {
      return pusher.cullPreviouslyPushedRecords(records).then(function (filtered) {
        filtered.length.should.equal(0);
      });
    });
  });

  it('Should cull some records from empty worker on 1 shard', function () {
    var kutil = new KinesisUtil(services.kinesis);
    var records = producer.generate(100);

    return kutil.pushRecords('stream1', _.take(records, 50)).then(function (maxSeqs) {
      return pusher.cullPreviouslyPushedRecords(records).then(function (filtered) {
        filtered.length.should.equal(50);
        _.each(filtered, function (r) {
          r.Data.should.be.aboveOrEqual(50);
        });
      });
    });
  });

  it('Should cull all records from empty worker on 2 shards', function () {
    producer = new DataServices.RecordProducer(['alpha', 'bravo']);
    pusher = new KinesisPusher(worker, 'stream2', services.kinesis);

    var kutil = new KinesisUtil(services.kinesis);
    var records = producer.generateRoundRobin(100);

    return services.kinesis.createStream({
      ShardCount: 2,
      StreamName: 'stream2'
    }).q().delay(50).then(function () {
      return kutil.pushRecords('stream2', records).then(function (maxSeqs) {
        _.keys(maxSeqs).length.should.equal(2);
        return pusher.cullPreviouslyPushedRecords(records).then(function (filtered) {
          filtered.length.should.equal(0);
        });
      });
    });
  });

  it('Should cull some records from empty worker on 2 shards', function () {
    producer = new DataServices.RecordProducer(['alpha', 'bravo']);
    pusher = new KinesisPusher(worker, 'stream2', services.kinesis);

    var kutil = new KinesisUtil(services.kinesis);
    var records = producer.generateRoundRobin(100);

    return services.kinesis.createStream({
      ShardCount: 2,
      StreamName: 'stream2'
    }).q().delay(50).then(function () {
      return kutil.pushRecords('stream2', _.take(records, 60)).then(function (maxSeqs) {
        _.keys(maxSeqs).length.should.equal(2);
        return pusher.cullPreviouslyPushedRecords(records).then(function (filtered) {
          filtered.length.should.equal(40);
          _.groupBy(filtered, 'PartitionKey').alpha.length.should.equal(20);
          _.groupBy(filtered, 'PartitionKey').bravo.length.should.equal(20);
          _.each(filtered, function (r) {
            r.Data.should.be.aboveOrEqual(50);
          });
        });
      });
    });
  });

  it('Should cull some records from empty worker on split shard', function () {
    producer = new DataServices.RecordProducer(['alpha', 'bravo']);

    var kutil = new KinesisUtil(services.kinesis);
    var records = producer.generateRoundRobin(100);

    return kutil.pushRecords('stream1', _.take(records, 30)).then(function () {
      return services.splitShard('stream1', 'shardId-000000000000');
    }).then(function () {
      return kutil.pushRecords('stream1', _.slice(records, 30, 60));
    }).then(function () {
      return pusher.cullPreviouslyPushedRecords(records).then(function (filtered) {
        filtered.length.should.equal(40);
        _.groupBy(filtered, 'PartitionKey').alpha.length.should.equal(20);
        _.groupBy(filtered, 'PartitionKey').bravo.length.should.equal(20);
        _.each(filtered, function (r) {
          r.Data.should.be.aboveOrEqual(60);
        });
      });
    });
  });

  it.only('Should cull some records from empty worker on merged shard', function () {
    producer = new DataServices.RecordProducer(['alpha', 'bravo']);
    pusher = new KinesisPusher(worker, 'stream2', services.kinesis);

    var kutil = new KinesisUtil(services.kinesis);
    var records = producer.generateRoundRobin(100);

    return services.kinesis.createStream({
      ShardCount: 2,
      StreamName: 'stream2'
    }).q().delay(50).then(function () {
      return kutil.pushRecords('stream2', _.take(records, 30)).then(function () {
        return services.mergeShards('stream2', 'shardId-000000000000', 'shardId-000000000001');
      }).then(function () {
        return kutil.pushRecords('stream2', _.slice(records, 30, 60));
      }).then(function () {
        return pusher.cullPreviouslyPushedRecords(records).then(function (filtered) {
          filtered.length.should.equal(40);
          _.groupBy(filtered, 'PartitionKey').alpha.length.should.equal(20);
          _.groupBy(filtered, 'PartitionKey').bravo.length.should.equal(20);
          _.each(filtered, function (r) {
            r.Data.should.be.aboveOrEqual(60);
          });
        });
      });
    });
  });
});