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
var workerTable = 'TestWorkerTable';

describe('Something', function () {
  before(function () {
    return Q.all([
      services.initKinesis({ stream: 'stream1' })
    ]);
  });

  it('Pushes some records of a single key', function () {
    var producer = new DataServices.RecordProducer(1);
    var worker = new WorkerState(workerTable, 'test1', 'shardId-000000000000');
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
  var worker, producer, pusher, streamName;
  var streamIndex = 0;

  beforeEach(function () {
    streamName = 'streamrc' + streamIndex++;
    return services.initKinesis({stream: streamName}).then(function () {
      worker = new WorkerState(workerTable, 'testcull', 'shardId-000000000000');
      pusher = new KinesisPusher(worker, streamName, services.kinesis);
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

    return kutil.pushRecords(streamName, records).then(function (maxSeqs) {
      return pusher.cullPreviouslyPushedRecords(records).then(function (filtered) {
        filtered.length.should.equal(0);
      });
    });
  });

  it('Should cull some records from empty worker on 1 shard', function () {
    var kutil = new KinesisUtil(services.kinesis);
    var records = producer.generate(100);

    return kutil.pushRecords(streamName, _.take(records, 50)).then(function (maxSeqs) {
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
    pusher = new KinesisPusher(worker, 'mstream1', services.kinesis);

    var kutil = new KinesisUtil(services.kinesis);
    var records = producer.generateRoundRobin(100);

    return services.kinesis.createStream({
      ShardCount: 2,
      StreamName: 'mstream1'
    }).q().delay(50).then(function () {
      return kutil.pushRecords('mstream1', records).then(function (maxSeqs) {
        _.keys(maxSeqs).length.should.equal(2);
        return pusher.cullPreviouslyPushedRecords(records).then(function (filtered) {
          filtered.length.should.equal(0);
        });
      });
    });
  });

  it('Should cull some records from empty worker on 2 shards', function () {
    producer = new DataServices.RecordProducer(['alpha', 'bravo']);
    pusher = new KinesisPusher(worker, 'mstream2', services.kinesis);

    var kutil = new KinesisUtil(services.kinesis);
    var records = producer.generateRoundRobin(100);

    return services.kinesis.createStream({
      ShardCount: 2,
      StreamName: 'mstream2'
    }).q().delay(50).then(function () {
      return kutil.pushRecords('mstream2', _.take(records, 60)).then(function (maxSeqs) {
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

    return kutil.pushRecords(streamName, _.take(records, 30)).then(function () {
      return services.splitShard(streamName, 'shardId-000000000000');
    }).then(function () {
      return kutil.pushRecords(streamName, _.slice(records, 30, 60));
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

  it('Should cull some records from empty worker on merged shard', function () {
    producer = new DataServices.RecordProducer(['alpha', 'bravo']);
    pusher = new KinesisPusher(worker, 'mstream3', services.kinesis);

    var kutil = new KinesisUtil(services.kinesis);
    var records = producer.generateRoundRobin(100);

    return services.kinesis.createStream({
      ShardCount: 2,
      StreamName: 'mstream3'
    }).q().delay(50).then(function () {
      return kutil.pushRecords('mstream3', _.take(records, 30)).then(function () {
        return services.mergeShards('mstream3', 'shardId-000000000000', 'shardId-000000000001');
      }).then(function () {
        return kutil.pushRecords('mstream3', _.slice(records, 30, 60));
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

  it('Should cull all records from failed worker on 1 shard', function () {
    var records1 = producer.generate(50);
    var records2 = producer.generate(25);

    return worker.initialize([pusher.appKey()]).then(function () {
      return pusher.pushRecords(records1, 150).then(function () {
        return worker.flush(150);
      });
    }).then(function () {
      return pusher.pushRecords(records2, 200);
    }).then(function () {
      return worker.initialize([pusher.appKey()]);
    }).then(function () {
      return pusher.pushRecords(records2, 200).then(function () {
        return worker.flush(200);
      });
    }).then(function () {
      return producer.validateStream(services.kinesis, streamName, 'shardId-000000000000').then(function (result) {
        result.should.be.true();
      });
    });
  });

  it('Should cull all records from failed worker on 2 shards', function () {
    producer = new DataServices.RecordProducer(['alpha', 'bravo']);
    pusher = new KinesisPusher(worker, 'mstream4', services.kinesis);

    var records1 = producer.generateRoundRobin(50);
    var records2 = producer.generateRoundRobin(50);

    return services.kinesis.createStream({
      ShardCount: 2,
      StreamName: 'mstream4'
    }).q().delay(50).then(function () {
      return worker.initialize([pusher.appKey()]).then(function () {
        return pusher.pushRecords(records1, 150).then(function () {
          return worker.flush(150);
        });
      });
    }).then(function () {
      return pusher.pushRecords(records2, 200);
    }).then(function () {
      return worker.initialize([pusher.appKey()]);
    }).then(function () {
      return pusher.pushRecords(records2, 200).then(function () {
        return worker.flush(200);
      });
    }).then(function () {
      return producer.validateStream(services.kinesis, 'mstream4', 'shardId-000000000000', { partitions: ['alpha'] }).then(function (result) {
        result.should.be.true();
      });
    }).then(function () {
      return producer.validateStream(services.kinesis, 'mstream4', 'shardId-000000000001', { partitions: ['bravo'] }).then(function (result) {
        result.should.be.true();
      });
    });
  });
});