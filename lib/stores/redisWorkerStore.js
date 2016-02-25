var _ = require('lodash'),
  Q = require('q'),
  Redis = require('../util/redis');

function RedisWorkerStore (host, port, prefix) {
  this.redis = Redis.createClient(port, host);
  this.prefix = prefix || 'workerstore';
  this.pendingWrite = {};
  this.pendingDelete = [];
}

RedisWorkerStore.prototype.KeyField = 'app.key';
RedisWorkerStore.prototype.SubkeyField = 'shard.key';

RedisWorkerStore.prototype.canReadPartialMatch = false;
RedisWorkerStore.prototype.canReadSubkeyList = true;

RedisWorkerStore.prototype.getStore = function (baseAppKey, shard) {
  return this;
};

RedisWorkerStore.prototype.readItems = function (key, subkey) {
  var self = this;
  if (_.isArray(subkey)) {
    var keylist = _.map(subkey, function (sk) {
      return self.unifyKey(key, sk);
    });
    return self.redis.mgetAsync(keylist).then(function (data) {
      if (!data)
        return [];
      return _(data).map(function (item) {
        return JSON.parse(item);
      }).compact().value();
    })
  }
  else {
    return this.redis.getAsync(this.unifyKey(key, subkey)).then(function (data) {
      if (!data)
        return [];
      return [JSON.parse(data)];
    });
  }
};

RedisWorkerStore.prototype.writeItem = function (key, subkey, item) {
  item = _.assign(this.buildKey(key, subkey), item);
  this.pendingWrite[this.unifyKey(key, subkey)] = JSON.stringify(item);
  return Q();
};

RedisWorkerStore.prototype.deleteItem = function (key, subkey) {
  this.pendingDelete.push(this.unifyKey(key, subkey));
  return Q();
};

RedisWorkerStore.prototype.deleteItems = function (items) {
  var self = this;
  _.each(items, function (item) {
    var key = item[self.KeyField];
    var subkey = item[self.SubkeyField];
    self.pendingDelete.push(self.unifyKey(key, subkey));
  });

  return Q();
};

RedisWorkerStore.prototype.writeAttributeIfNotExists = function (key, subkey, attr, value) {
  var self = this;
  return this.redis.getAsync(this.unifyKey(key, subkey)).then(function (data) {
    data = (data) ? JSON.parse(data) : {};
    if (_.has(data, attr))
      return false;
    data[attr] = value;

    return self.redis.setAsync(self.unifyKey(key, subkey), JSON.stringify(data)).then(function () {
      return true;
    });
  });
};

RedisWorkerStore.prototype.commit = function () {
  var self = this;
  return Q.fcall(function () {
    if (!_.isEmpty(self.pendingWrite))
      return self.redis.msetAsync(_.flatten(_.pairs(self.pendingWrite)));
  }).then(function () {
    self.pendingWrite = {};
    if (!_.isEmpty(self.pendingDelete))
      return self.redis.delAsync(self.pendingDelete);
  }).then(function () {
    self.pendingDelete = [];
  });
};

RedisWorkerStore.prototype.buildKey = function (key, subkey) {
  var params = {};
  params[this.KeyField] = key;
  params[this.SubkeyField] = subkey;
  return params;
};

RedisWorkerStore.prototype.unifyKey = function (key, subkey) {
  return this.prefix + ':' + key + ':' + subkey;
};

module.exports = RedisWorkerStore;