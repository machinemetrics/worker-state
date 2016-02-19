var moment = require('moment'),
  bigInt = require('big-integer'),
  _ = require('lodash'),
  Q = require('q'),
  DynamoWorkerStore = require('./stores/dynamoWorkerStore');

function WorkerLogger (app, shard, workerStore) {
  if (_.isString(workerStore))
    workerStore = new DynamoWorkerStore(workerStore);

  this.store = workerStore;
  this.app = app;
  this.shard = shard;
}

WorkerLogger.prototype.log = function (key, item) {
  var eventTime = moment.utc().format('YYYY-MM-DDTHH:mm:ss.SSS');
  return this.store.writeItem(this.makeAppKey(key), this.makeShardKey(eventTime), item);
};

WorkerLogger.prototype.logImmediate = function (key, item) {
  var self = this;
  return self.log(key, item).then(function () {
    return self.flush();
  });
};

WorkerLogger.prototype.flush = function () {
  return this.store.commit();
};

WorkerLogger.prototype.makeAppKey = function (key) {
  var appValue = this.app;
  if (_.isFinite(key) || (_.isString(key) && key.length > 0))
    appValue += '.' + key;
  return appValue;
};

WorkerLogger.prototype.makeShardKey = function (key) {
  var shardValue = this.shard || '';
  if (_.isFinite(key) || (_.isString(key) && key.length > 0)) {
    if (this.shard)
      shardValue += '.';
    shardValue += key;
  }
  return shardValue;
};

module.exports = WorkerLogger;