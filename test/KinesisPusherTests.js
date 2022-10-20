process.env.AWS_REGION = 'us-west-2';

const _ = require('lodash');
const Q = require('q');
const KinesisPusher = require('../lib').KinesisPusher;
const KinesisUtil = require('../lib/kinesisUtil');
const WorkerState = require('../lib').WorkerState;
// const WorkerLogger = require('../lib').Logger;
const TestServices = require('./util/TestServices');
const DataServices = require('./util/DataServices');
require('should');

var services = new TestServices();
var workerTable = 'TestWorkerTable';
// var loggerTable = 'WorkerStateLog';

describe('Record Pushing', function () {
  before(function () {
    return Q.all([
      services.initKinesis({ stream: 'stream1' }),
    ]);
  });

  it('Pushes some records of a single key', function () {
    var producer = new DataServices.RecordProducer(1);
    var worker = new WorkerState('test1', 'shardId-000000000000', workerTable);
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
  var worker;
  var producer;
  var pusher;
  var streamName;
  var streamIndex = 0;

  beforeEach(function () {
    streamName = 'streamrc' + streamIndex++;
    return services.initKinesis({ stream: streamName }).then(function () {
      worker = new WorkerState('testcull', 'shardId-000000000000', workerTable);
      pusher = new KinesisPusher(worker, streamName, services.kinesis);
      // pusher.logger = new WorkerLogger('testcull', 'shardId-000000000000', loggerTable);
      producer = new DataServices.RecordProducer(1);
    });
  });

  afterEach(function () {
    return Q.all([
      worker.expungeAllKnownSavedState(),
      services.stopKinesis(),
    ]);
  });

  it('Should cull all records from empty worker on 1 shard', function () {
    var kutil = new KinesisUtil(services.kinesis);
    var records = producer.generate(100);

    return kutil.pushRecords(streamName, records).then(function (_maxSeqs) {
      return pusher.cullPreviouslyPushedRecords(records).then(function (filtered) {
        filtered.length.should.equal(0);
      });
    });
  });

  it('Should cull some records from empty worker on 1 shard', function () {
    var kutil = new KinesisUtil(services.kinesis);
    var records = producer.generate(100);

    return kutil.pushRecords(streamName, _.take(records, 50)).then(function (_maxSeqs) {
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
    // pusher.logger = new WorkerLogger('testcull', 'shardId-000000000000', loggerTable);

    var kutil = new KinesisUtil(services.kinesis);
    var records = producer.generateRoundRobin(100);

    return services.kinesis.createStream({
      ShardCount: 2,
      StreamName: 'mstream1',
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
    // pusher.logger = new WorkerLogger('testcull', 'shardId-000000000000', loggerTable);

    var kutil = new KinesisUtil(services.kinesis);
    var records = producer.generateRoundRobin(100);

    return services.kinesis.createStream({
      ShardCount: 2,
      StreamName: 'mstream2',
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
    // pusher.logger = new WorkerLogger('testcull', 'shardId-000000000000', loggerTable);

    var kutil = new KinesisUtil(services.kinesis);
    var records = producer.generateRoundRobin(100);

    return services.kinesis.createStream({
      ShardCount: 2,
      StreamName: 'mstream3',
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

  it('Should cull all records from failed worker on 1 shard extended record set', function () {
    var records1 = producer.generate(50);
    var records2 = producer.generate(25);
    var records3 = _.flatten([records2, producer.generate(25)]);

    return worker.initialize([pusher.appKey()]).then(function () {
      return pusher.pushRecords(records1, 150).then(function () {
        return worker.flush(150);
      });
    }).then(function () {
      return pusher.pushRecords(records2, 200);
    }).then(function () {
      worker.simulateFailure = true;
      return worker.flush(200);
    }).then(function () {
      return worker.initialize([pusher.appKey()]);
    }).then(function () {
      return pusher.pushRecords(records3, 300).then(function () {
        worker.simulateFailure = false;
        return worker.flush(300);
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
    // pusher.logger = new WorkerLogger('testcull', 'shardId-000000000000', loggerTable);

    var records1 = producer.generateRoundRobin(50);
    var records2 = producer.generateRoundRobin(50);

    return services.kinesis.createStream({
      ShardCount: 2,
      StreamName: 'mstream4',
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
