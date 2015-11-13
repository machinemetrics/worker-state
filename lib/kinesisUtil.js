var md5 = require('md5'),
    async = require('async-q'),
    bigInt = require('big-integer'),
    _ = require('lodash');

function KinesisUtil (kinesis) {
  this.kinesis = kinesis || new AWS.Kinesis();
}

KinesisUtil.prototype.getShards = function (stream) {
  var self = this;
  var params = {
    StreamName: stream
  };

  var processMoreRecords = true;
  var shards = [];

  return async.whilst(function () {
    return processMoreRecords;
  }, function () {
    return self.kinesis.describeStream(params).q().then(function (data) {
      shards = shards.concat(data.StreamDescription.Shards);
      if (data.HasMoreShards)
        params.ExclusiveStartShardId = _.last(data.StreamDescription.Shards).ShardId;
      else
        processMoreRecords = false;
    });
  }).then(function () {
    return _.indexBy(shards, 'ShardId');
  });
};

KinesisUtil.prototype.filterWriteableShards = function (shards) {
  return _.filter(shards, function (shard) {
    return !_.get(shard, 'SequenceNumberRange.EndingSequenceNumber');
  });
};

KinesisUtil.prototype.getShardLineage = function (shards, partitionKey) {
  var hash = bigInt(md5(partitionKey), 16);

  return _(shards).filter(function (shard) {
    return hash.geq(parseHashKey(shard.HashKeyRange.StartingHashKey)) && hash.leq(parseHashKey(shard.HashKeyRange.EndingHashKey));
  }).sortBy('SequenceNumberRange.StartingSequenceNumber').value();
};

KinesisUtil.prototype.groupAndPackRecords = function(records, count) {
  count = count || 100;

  return _(records).groupBy('PartitionKey').map(function (items, key) {
    return _.map(_.chunk(items, count), function (chunk) {
      return {
        PartitionKey: key,
        Data: JSON.stringify(_.map(chunk, 'Data'))
      };
    });
  }).unzip().map(_.compact).value();
};

KinesisUtil.prototype.unpackRecords = function (records, flatten) {
  var result = _.map(records, function (item) {
    var data = JSON.parse(item.Data.toString('utf8'));
    if (!_.isArray(data)) {
      return {
        Data: data,
        SequenceNumber: item.SequenceNumber,
        PartitionKey: item.PartitionKey
      };
    }

    return _.map(data, function (subItem) {
      return {
        Data: subItem,
        SequenceNumber: item.SequenceNumber,
        PartitionKey: item.PartitionKey
      };
    });
  });

  if (flatten || _.isUndefined(flatten))
    result = _.flatten(result);

  return result;
};

KinesisUtil.prototype.takeNextBatch = function (records, size) {
  size = size || 1024 * 1024;

  if (_.isObject(records) && records.PartitionKey) {
    return [_.extend(_.clone(records), {
      Data: new Buffer(records.Data.toString(), 'utf8')
    })];
  }

  var accum = 0;
  return _(records).transform(function (buffer, next) {
    accum += next.Data.length + next.PartitionKey.length;
    buffer.push(next);
    return accum < size && buffer.length < 500;
  }, []).map(function (rec) {
    rec.Data = new Buffer(rec.Data.toString(), 'utf8');
    return rec;
  }).value();
};

KinesisUtil.prototype.pushRecords = function (stream, records, maxBatchSize) {
  var self = this;
  var backoff = 25;
  var shardMaxSequence = {};

  return async.eachSeries(records, function (group) {
    return async.until(function () {
      return _.isEmpty(group);
    }, function () {
      var request = self.takeNextBatch(group, maxBatchSize);
      return self.kinesis.putRecords({
        Records: request,
        StreamName: stream
      }).q().then(function (resp) {
        var shards = self.getMaxSequenceNumbers(resp.Records);
        shardMaxSequence = _.merge(shardMaxSequence, shards, function (left, right) {
          if (!left)
            return right;
          return left.gt(right) ? left : right;
        });

        group = _.drop(group, request.length);
        if (resp.FailedRecordCount) {
          group = group.concat(_.reject(request, function (rec, index) {
            return resp.Records[index].ShardId;
          }));
          backoff = Math.min(backoff * 2, 1000);
          return Q.delay(backoff);
        }
      });
    });
  }).then(function () {
    return _.mapValues(shardMaxSequence, _.method('toString'));
  });
};

KinesisUtil.prototype.getMaxSequenceNumbers = function (records) {
  return _(records).filter('ShardId').groupBy('ShardId').mapValues(function (recs) {
    return _(recs).map(function (rec) {
      return bigInt(rec.SequenceNumber);
    }).reduce(function (result, seq) {
      return seq.gt(result) ? seq : result;
    });
  }).value();
};

function parseHashKey (hash) {
  // A hack to get around (Kinesalite?) setting hashes in e+ format after a split.  It still contains all the digits.
  if (hash.indexOf('e+') > -1) {
    var exp = parseInt(hash.match(/e\+(\d+)/)[1]);
    hash = _.padRight(hash.replace(/\.|e\+\d+/g, ''), exp + 1, 0);
  }

  return bigInt(hash);
}

module.exports = KinesisUtil;