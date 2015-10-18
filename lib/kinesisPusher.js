var AWS = require('aws-sdk-q'),
    workerState = require('./workerState');

function KinesisPusher (workerState, stream) {
  this.workerState = workerState;
  this.stream = stream;
  this.kinesis = new AWS.Kinesis();
}

KinesisPusher.prototype.pushRecords = function (records, seq) {
  var self = this;
  records = _(records).groupBy('partitionKey').values().unzip().map(_.compact).value();
  if (_.isEmpty(records))
    return Q();

  var shardMaxSequence = {};
  return async.eachSeries(records, function (group) {
    var backoff = 50;
    group = _.map(group, function (item) {
      return {
        Data: new Buffer(JSON.stringify(item.data)),
        PartitionKey: item.partitionKey
      };
    });

    return async.until(function () {
      return _.isEmpty(group);
    }, function () {
      var request = _.take(group, 500);
      return self.kinesis.putRecords({
        Records: request,
        StreamName: self.stream
      }).q().then(function (resp) {
        var processed = _.partition(request, function (rec, index) {
          return resp.Records[index].ShardId;
        });

        shardMaxSequence = _.merge(shardMaxSequence, _.groupBy(processed[0], 'ShardId'), function (left, right) {
          return left > right ? left : right;
        });

        group = _.drop(group, request.length);
        if (resp.FailedRecordCount) {
          group = group.concat(processed[1]);
          backoff = Math.min(backoff * 2, 1000);
          return Q.delay(backoff);
        }
      });
    });
  }).then(function () {
    _.each(shardMaxSequence, function (shardSeq, shard) {
      self.workerState.setSubValue(self.stream, shard, shardSeq);
    });
    return self.workerState.flush(seq);
  });
};