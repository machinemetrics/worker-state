'use strict';

var _ = require('lodash');
var Q = require('q');
var Redis = require('../util/redis');


function RedisWorkerStore(host, port, prefix, redisOptions) {
  this.prefix = prefix || 'workerstore';
  this.pendingWrite = {};
  this.pendingDelete = [];
  this.pendingKeyAdd = {};
  this.pendingKeyDel = {};
  this.printStatistics = process.env.verboseSharedRedisWorkerStore;
  this.commandLog = null;

  var redisKey = ['RedisWorkerStoreRedisPool', host + ':' + port];
  this.redis = _.get(global, redisKey);

  if (!this.redis) {
    this.redis = Redis.createClient(port, host, _.omit(redisOptions, 'secondary'));
    _.set(global, redisKey, this.redis);
  }

  this.readFromSecondaryRedis = false;

  if (_.has(redisOptions, 'secondary')) {
    var options = redisOptions.secondary;
    var altRedisKey = ['RedisWorkerStoreRedisPool', options.host + ':' + options.port];
    this.altRedis = _.get(global, altRedisKey);

    if (!this.altRedis) {
      this.altRedis = Redis.createClient(options.port, options.host, options.options);
      _.set(global, altRedisKey, this.altRedis);
    }
  }
}

RedisWorkerStore.prototype.KeyField = 'app.key';
RedisWorkerStore.prototype.SubkeyField = 'shard.key';

RedisWorkerStore.prototype.canReadPartialMatch = true;
RedisWorkerStore.prototype.canReadSubkeyList = true;

RedisWorkerStore.prototype.readFromPrimary = function () {
  this.readFromSecondaryRedis = false;
};

RedisWorkerStore.prototype.readFromSecondary = function () {
  if (this.altRedis) {
    this.readFromSecondaryRedis = true;
  }
};

RedisWorkerStore.prototype.getStore = function (_baseAppKey, _shard) {
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
      self.log('readItems', 'Partial match lookup on "' + unified + '"');

      return zrangebylex(self.indexKey(key), '[' + unified, '[' + unified + '\xff');
    }).then(function (keylist) {
      if (_.isEmpty(keylist))
        return [];

      self.log('readItems', 'Multi-key lookup; cardinality = ' + keylist.length);
      return mget(keylist).then(function (data) {
        if (!data)
          return [];
        return _(data).map(function (item) {
          return JSON.parse(item);
        }).compact().value();
      });
    });
  } else {
    self.log('readItems', 'Single-key lookup');
    return get(this.unifyKey(key, subkey)).then(function (data) {
      if (!data)
        return [];
      return [JSON.parse(data)];
    });
  }

  function zrangebylex(key, min, max) {
    const logEntry = self.logCommand('readItems', ['zrangebylex', key, min, max]);
    const redis = self.readFromSecondaryRedis ? self.altRedis : self.redis;

    return redis.zrangebylexAsync(key, min, max).tap((result) => {
      logEntry.result = _.clone(result);
    });
  }

  function mget(keys) {
    const logEntry = self.logCommand('readItems', ['mget', keys]);
    const redis = self.readFromSecondaryRedis ? self.altRedis : self.redis;

    return redis.mgetAsync(keys).tap((result) => {
      logEntry.result = _.clone(result);
    });
  }

  function get(key) {
    const logEntry = self.logCommand('readItems', ['get', key]);
    const redis = self.readFromSecondaryRedis ? self.altRedis : self.redis;

    return redis.getAsync(key).tap((result) => {
      logEntry.result = result;
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

  return get(this.unifyKey(key, subkey)).then(function (data) {
    data = (data) ? JSON.parse(data) : {};
    if (_.has(data, attr))
      return false;

    data[attr] = value;
    return set(self.unifyKey(key, subkey), JSON.stringify(data)).then(function () {
      return true;
    });
  });

  function get(key) {
    const logEntry = self.logCommand('writeAttributeIfNotExists', ['get', key]);
    const redis = self.readFromSecondaryRedis ? self.altRedis : self.redis;

    return redis.getAsync(key).tap((result) => {
      logEntry.result = result;
    });
  }

  function set(key, value) {
    self.logCommand('writeAttributeIfNotExists', ['set', key, value]);
    return self.redis.setAsync(key, value).then(function () {
      if (self.altRedis) {
        return self.altRedis.setAsync(key, value);
      }
      return Q();
    });
  }
};

RedisWorkerStore.prototype.commit = function () {
  var self = this;
  var multi = self.redis.multi();

  var altMulti = null;
  if (self.altRedis) {
    altMulti = self.altRedis.multi();
  }

  if (!_.isEmpty(self.pendingWrite)) {
    mset(_.flatten(_.pairs(self.pendingWrite)));
  }

  if (!_.isEmpty(self.pendingDelete)) {
    del(self.pendingDelete);
  }

  var subAdd = 0;
  if (!_.isEmpty(self.pendingKeyAdd)) {
    _.each(self.pendingKeyAdd, function (ukeys, key) {
      if (!_.isEmpty(ukeys)) {
        var items = _.flatten(_.zip(_.times(ukeys.length, _.constant(0)), ukeys));
        subAdd += items.length / 2;
        zadd(self.indexKey(key), items);
      }
    });
  }

  var subDel = 0;
  if (!_.isEmpty(self.pendingKeyDel)) {
    _.each(self.pendingKeyDel, function (ukeys, key) {
      if (!_.isEmpty(ukeys)) {
        subDel += ukeys.length;
        zrem(self.indexKey(key), ukeys);
      }
    });
  }

  self.log('commit', 'Write = ' + _.size(self.pendingWrite) + ', Delete = ' + _.size(self.pendingDelete) +
    ', KeyAdd = ' + _.size(self.pendingKeyAdd) + ',' + subAdd + ', KeyDel = ' + _.size(self.pendingDelete) + ',' + subDel);

  return Q.all([
    multi.execAsync(),
    Q.fcall(function () {
      if (altMulti) {
        return altMulti.execAsync();
      }
      return Q();
    }),
  ]).then(function () {
    self.pendingWrite = {};
    self.pendingDelete = [];
    self.pendingKeyAdd = {};
    self.pendingKeyDel = {};
  });

  function mset(items) {
    self.logCommand('commit', ['mset', items]);
    multi.mset(items);

    if (altMulti) {
      altMulti.mset(items);
    }
  }

  function del(items) {
    self.logCommand('commit', ['del', items]);
    multi.del(items);

    if (altMulti) {
      altMulti.del(items);
    }
  }

  function zadd(key, items) {
    self.logCommand('commit', ['zadd', key, items]);
    multi.zadd(key, items);

    if (altMulti) {
      altMulti.zadd(key, items);
    }
  }

  function zrem(key, items) {
    self.logCommand('commit', ['zrem', key, items]);
    multi.zrem(key, items);

    if (altMulti) {
      altMulti.zrem(key, items);
    }
  }
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

RedisWorkerStore.prototype.log = function (domain, message) {
  if (this.printStatistics)
    console.log('RedisWorkerStore: ' + domain + ': ' + message);
};

RedisWorkerStore.prototype.logCommand = function (domain, command) {
  if (_.isArray(this.commandLog)) {
    if (_.isArray(command)) {
      command = _.flattenDeep(command).join(' ');
    }

    const entry = { domain, command };
    this.commandLog.push(entry);
    return entry;
  }

  return {};
};

module.exports = RedisWorkerStore;
