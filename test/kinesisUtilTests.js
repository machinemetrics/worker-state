const _ = require('lodash');
const should = require('should');
const KinesisUtil = require('../lib/kinesisUtil');
const TestServices = require('./util/TestServices');
const DataServices = require('./util/DataServices');

var services = new TestServices();

describe('Kinesis Utilities', function () {
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
      StreamName: 'stream2',
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
      StreamName: 'stream3',
    }).q().delay(50).then(function () {
      return services.splitShard('stream3', 'shardId-000000000000');
    }).then(function () {
      return kutil.getShards('stream3');
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

  it('Should find max sequence numbers in all shards', function () {
    var kutil = new KinesisUtil(services.kinesis);
    var data = [
      { ShardId: 'shardId-000000000000', SequenceNumber: '49556303523432154881539545814062592752077878316425543682' },
      { ShardId: 'shardId-000000000000', SequenceNumber: '59556303523432154881539545814062592752077878316425543682' },
      { ShardId: 'shardId-000000000000', SequenceNumber: '43556303523432154881539545814062592752077878316425543682' },
      { ShardId: 'shardId-000000000000', SequenceNumber: '9955630352343215488153954581406259275207787831642554368' },
      { ShardId: 'shardId-000000000001', SequenceNumber: '49556303523432154881539545814062592752097878316425543682' },
      { ShardId: 'shardId-000000000001', SequenceNumber: '59556303523432154881539545814062592752097878316425543682' },
      { ShardId: 'shardId-000000000001', SequenceNumber: '43556303523432154881539545814062592752097878316425543682' },
      { ShardId: 'shardId-000000000001', SequenceNumber: '9955630352343215488153954581406259275209787831642554368' },
    ];

    var maxSeq = kutil.getMaxSequenceNumbers(data);
    maxSeq.should.have.properties(['shardId-000000000000', 'shardId-000000000001']);
    maxSeq['shardId-000000000000'].toString().should.equal('59556303523432154881539545814062592752077878316425543682');
    maxSeq['shardId-000000000001'].toString().should.equal('59556303523432154881539545814062592752097878316425543682');
  });

  it('Should group records by count limit', function () {
    var kutil = new KinesisUtil(services.kinesis);

    var makeSet = (key, n) => {
      return _.times(n, (i) => {
        return { PartitionKey: key, Data: { value: i } };
      });
    };

    var data = [
      ...makeSet('key1', 5),
      ...makeSet('key2', 10),
    ];

    var packed = kutil.groupAndPackRecords(data, 5);
    packed.length.should.equal(2);
    packed[0].length.should.equal(2);
    packed[1].length.should.equal(1);
  });

  it('Should group records by byte limit', function () {
    var kutil = new KinesisUtil(services.kinesis);

    var makeSet = (key, n) => {
      return _.times(n, (i) => {
        return { PartitionKey: key, Data: { value: i } };
      });
    };

    var data = [
      ...makeSet('key1', 5),
      ...makeSet('key2', 10),
    ];

    var packed = kutil.groupAndPackRecords(data, null, 70);
    packed.length.should.equal(4);
    packed[0].length.should.equal(2);
    packed[1].length.should.equal(2);
    packed[2].length.should.equal(1);
    packed[3].length.should.equal(1);
  });
});

describe('Pushing Records', function () {
  var streamName;
  var streamIndex = 0;

  beforeEach(function () {
    streamName = 'streamUtilPush' + streamIndex++;
    return services.initKinesis({ stream: streamName });
  });

  afterEach(function () {
    return services.stopKinesis();
  });

  it('Should push 10 records as 10 kinesis records', function () {
    var producer = new DataServices.RecordProducer(1);
    var kutil = new KinesisUtil(services.kinesis);

    return kutil.pushRecords(streamName, producer.generate(10)).then(function (seqs) {
      _.keys(seqs).length.should.equal(1);
      return producer.validateStream(services.kinesis, streamName, 'shardId-000000000000').then(function (result) {
        result.should.be.true();
      });
    });
  });

  it('Should push 10 records as 1 kinesis agg record', function () {
    var producer = new DataServices.RecordProducer(1);
    var kutil = new KinesisUtil(services.kinesis);

    var records = kutil.groupAndPackRecords(producer.generate(10));
    return kutil.pushRecords(streamName, records).then(function (seqs) {
      _.keys(seqs).length.should.equal(1);
      return producer.validateStream(services.kinesis, streamName, 'shardId-000000000000', { packCount: [10] }).then(function (result) {
        result.should.be.true();
      });
    });
  });

  it('Should push 10 records as 3 kinesis agg records', function () {
    var producer = new DataServices.RecordProducer(1);
    var kutil = new KinesisUtil(services.kinesis);

    var records = kutil.groupAndPackRecords(producer.generate(250));
    return kutil.pushRecords(streamName, records).then(function (seqs) {
      _.keys(seqs).length.should.equal(1);
      return producer.validateStream(services.kinesis, streamName, 'shardId-000000000000', { packCounts: [100, 100, 50] }).then(function (result) {
        result.should.be.true();
      });
    });
  });

  it('Should push 10 records as 10 kinesis agg record', function () {
    var producer = new DataServices.RecordProducer(10);
    var kutil = new KinesisUtil(services.kinesis);

    var records = kutil.groupAndPackRecords(producer.generateRoundRobin(10));
    return kutil.pushRecords(streamName, records).then(function (seqs) {
      _.keys(seqs).length.should.equal(1);
      return producer.validateStream(services.kinesis, streamName, 'shardId-000000000000', { packCount: [1, 1, 1, 1, 1, 1, 1, 1, 1, 1] }).then(function (result) {
        result.should.be.true();
      });
    });
  });
});
