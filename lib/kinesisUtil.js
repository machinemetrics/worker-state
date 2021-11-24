var AWS = require('aws-sdk-q'),
    md5 = require('md5'),
    async = require('async-q'),
    bigInt = require('big-integer'),
    moment = require('moment'),
    Q = require('q'),
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

KinesisUtil.prototype.groupAndPackRecords = function (records, count, byteLimit) {
  count = count || 100;
  byteLimit = byteLimit || 1024 * 900;

  var takeNextGroup = (key, items) => {
    var accum = key.length + 20;
    return _(items).transform(function (buffer, next) {
      accum += Buffer.byteLength(JSON.stringify(next.Data), 'utf8') + 4;
      if (accum > byteLimit || buffer.length >= count) {
        return false;
      }

      buffer.push(next);
      return true;
    }, []).value();
  };

  return _(records).groupBy('PartitionKey').map(function (items, key) {
    var groups = [];
    while (items.length > 0) {
      var nextGroup = takeNextGroup(key, items);
      if (nextGroup.length === 0) {
        throw new Error('Could not pull record that meets count and size limits');
      }

      groups.push(nextGroup);
      items = _.drop(items, nextGroup.length);
    }

    return _.map(groups, function (chunk) {
      return {
        PartitionKey: key,
        Data: JSON.stringify(_.map(chunk, 'Data'))
      };
    });
  }).unzip().map(_.compact).value();
};

KinesisUtil.prototype.unpackRecords = function (records, flatten) {
  var result = _.map(records, function (item) {
    if (item.PartitionKey == '__MARKER')
      return;

    var str = item.Data.toString('utf8');
    if (!str || str.length == 0) {
      return {
        Data: '',
        SequenceNumber: item.SequenceNumber,
        PartitionKey: item.PartitionKey
      };
    }

    var data = {};
    try {
      data = JSON.parse(str);
    } catch (e) {}

    if (!_.isArray(data)) {
      return {
        Data: data,
        SequenceNumber: item.SequenceNumber,
        PartitionKey: item.PartitionKey
      };
    }

    return _.map(data, function (subItem, index) {
      return {
        Data: subItem,
        SequenceNumber: item.SequenceNumber,
        SubSequenceNumber: index,
        PartitionKey: item.PartitionKey
      };
    });
  });

  result = _.filter(result);

  if (flatten || _.isUndefined(flatten))
    result = _.flatten(result);

  return result;
};

KinesisUtil.prototype.takeNextBatch = function (records, size) {
  size = size || 1024 * 900;

  if (_.isObject(records) && records.PartitionKey) {
    return [_.extend(_.clone(records), {
      Data: new Buffer(records.Data.toString(), 'utf8')
    })];
  }

  var accum = 0;
  return _(records).transform(function (buffer, next) {
    accum += next.Data.length + next.PartitionKey.length;
    if (accum > size || buffer.length >= 500) {
      return false;
    }

    buffer.push(next);
    return true;
  }, []).map(function (rec) {
    rec.Data = new Buffer(rec.Data.toString(), 'utf8');
    return rec;
  }).value();
};

KinesisUtil.prototype.filterRecords = function (records, filterRecords) {
  var data = _.map(filterRecords, function (r) {
    return JSON.stringify(r.Data);
  });

  return _.filter(records, function (rec) {
    return !_.includes(data, JSON.stringify(rec.Data));
  });
};

KinesisUtil.prototype.batchRecords = function (records, approxBatchSize) {
  var batches = [];
  for (var i = approxBatchSize, p = 0; i < records.length; p = i, i += approxBatchSize) {
    for (; i < records.length; i++) {
      if (records[i - 1].SequenceNumber !== records[i].SequenceNumber)
        break;
    }
    batches.push(_.slice(records, p, i));
  }

  i = records.length;
  if (i > p)
    batches.push(_.slice(records, p, i));

  return batches;
};

/**
 * Pushes records into a Kinesis stream.
 *
 * Records are structured as an array of arrays of data records, with each data record carrying a Data and PartitionKey
 * field.  The leading record of each array will be pulled into one or more Kinesis requests, which must complete
 * without failures before the next records in line will be pulled.
 *
 * @param stream
 * @param records
 * @param {Object} options
 * @param {number} options.maxBatchSize - Maximum size in bytes of each Kinesis request.  Defaults to 1M.
 * @param {number} options.minInterval - Minimum time in ms that must elapse between constructing requests.  Defaults to 0.
 */
KinesisUtil.prototype.pushRecords = function (stream, records, options) {
  var self = this;
  var backoff = 25;
  var shardMaxSequence = {};

  // Legacy parameter transform
  if (_.isNumber(options)) {
    options = {
      maxBatchSize: options,
    };
  }

  options = options || {};

  return async.eachSeries(records, function (group) {
    return async.until(function () {
      return _.isEmpty(group);
    }, function () {
      var startTime = moment().valueOf();
      var request = self.takeNextBatch(group, options.maxBatchSize);
      return self.kinesis.putRecords({
        Records: request,
        StreamName: stream,
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
          console.log(`FailedRecordCount=${resp.FailedRecordCount}.  Retrying after ${backoff}ms`);
          return Q.delay(backoff);
        }

        if (options.minInterval) {
          var runtime = moment().valueOf() - startTime;
          return Q.delay(Math.max(options.minInterval - runtime, 0));
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
