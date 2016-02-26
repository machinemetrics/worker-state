var _ = require('lodash'),
  Q = require('q'),
  Redis = require('../util/redis');

function RedisWorkerStore (host, port, prefix) {
  this.redis = Redis.createClient(port, host);
  this.prefix = prefix || 'workerstore';
  this.pendingWrite = {};
  this.pendingDelete = [];
  this.pendingKeyAdd = {};
  this.pendingKeyDel = {};
}

RedisWorkerStore.prototype.KeyField = 'app.key';
RedisWorkerStore.prototype.SubkeyField = 'shard.key';

RedisWorkerStore.prototype.canReadPartialMatch = false;
RedisWorkerStore.prototype.canReadSubkeyList = true;

RedisWorkerStore.prototype.getStore = function (baseAppKey, shard) {
  return this;
};

RedisWorkerStore.prototype.readItems = function (key, subkey, partialMatch) {
  var self = this;
  if (_.isArray(subkey) || partialMatch) {
    return Q.fcall(function () {
      if (_.isArray(subkey)) {
        return _.map(subkey, function (sk) {
          return self.unifyKey(key, sk);
        });
      }

      var unified = self.unifyKey(key, subkey);
      return self.redis.zrangebylexAsync(self.indexKey(key), '[' + unified, '[' + unified + '\xff');
    }).then(function (keylist) {
      if (_.isEmpty(keylist))
        return [];

      return self.redis.mgetAsync(keylist).then(function (data) {
        if (!data)
          return [];
        return _(data).map(function (item) {
          return JSON.parse(item);
        }).compact().value();
      });
    });
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
  var unified = this.unifyKey(key, subkey);

  this.pendingWrite[unified] = JSON.stringify(item);
  delete this.pendingDelete[unified];

  if (!this.pendingKeyAdd[key])
    this.pendingKeyAdd[key] = [];
  this.pendingKeyAdd[key].push(unified);

  if (this.pendingKeyDel[key])
    this.pendingKeyDel[key] = _.without(this.pendingKeyDel[key], unified);

  return Q();
};

RedisWorkerStore.prototype.deleteItem = function (key, subkey) {
  var unified = this.unifyKey(key, subkey);

  this.pendingDelete.push(unified);
  delete this.pendingWrite[unified];

  if (!this.pendingKeyDel[key])
    this.pendingKeyDel[key] = [];
  this.pendingKeyDel[key].push(unified);

  if (this.pendingKeyAdd[key])
    this.pendingKeyAdd[key] = _.without(this.pendingKeyAdd[key], unified);

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
  var multi = self.redis.multi();

  if (!_.isEmpty(self.pendingWrite))
    multi.mset(_.flatten(_.pairs(self.pendingWrite)));
  if (!_.isEmpty(self.pendingDelete))
    multi.del(self.pendingDelete);

  if (!_.isEmpty(self.pendingKeyAdd)) {
    _.each(self.pendingKeyAdd, function (ukeys, key) {
      if (!_.isEmpty(ukeys)) {
        var items = _.flatten(_.zip(_.times(ukeys.length, _.constant(0)), ukeys));
        multi.zadd(self.indexKey(key), items);
      }
    });
  }

  if (!_.isEmpty(self.pendingKeyDel)) {
    _.each(self.pendingKeyDel, function (ukeys, key) {
      if (!_.isEmpty(ukeys))
        multi.zrem(self.indexKey(key), ukeys);
    });
  }

  return multi.execAsync().then(function () {
    self.pendingWrite = {};
    self.pendingDelete = [];
    self.pendingKeyAdd = {};
    self.pendingKeyDel = {};
  });

  /*return Q.fcall(function () {
    if (!_.isEmpty(self.pendingWrite))
      return self.redis.msetAsync(_.flatten(_.pairs(self.pendingWrite)));
  }).then(function () {
    self.pendingWrite = {};
    if (!_.isEmpty(self.pendingDelete))
      return self.redis.delAsync(self.pendingDelete);
  }).then(function () {
    self.pendingDelete = [];
    if (!_.isEmpty(self.pendingKeyAdd)) {
      return Q.all(_.map(self.pendingKeyAdd, function (key, ukeys) {
        if (_.isEmpty(ukeys))
          return Q();

        var items = _.flatten(_.zip(_.times(0, _.constant(ukeys.length)), ukeys));
        return self.redis.zaddAsync(self.indexKey(key), items);
      }));
    }
  }).then(function () {
    self.pendingKeyAdd = {};
    if (!_.isEmpty(self.pendingKeyDel)) {
      return Q.all(_.map(self.pendingKeyDel, function (key, ukeys) {
        if (_.isEmpty(ukeys))
          return Q();

        return self.redis.zremAsync(self.indexKey(key), ukeys);
      }));
    }
  }).then(function () {
    self.pendingKeyDel = {};
  });*/
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

RedisWorkerStore.prototype.indexKey = function (key) {
  return this.prefix + '-index:' + key;
};

module.exports = RedisWorkerStore;