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
  this.redis = _.get(GLOBAL, redisKey);
  if (!this.redis) {
    this.redis = Redis.createClient(port, host, redisOptions);
    _.set(GLOBAL, redisKey, this.redis);
  }
}

RedisWorkerStore.prototype.KeyField = 'app.key';
RedisWorkerStore.prototype.SubkeyField = 'shard.key';

RedisWorkerStore.prototype.canReadPartialMatch = true;
RedisWorkerStore.prototype.canReadSubkeyList = true;

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
      var commandParts = [self.indexKey(key), '[' + unified, '[' + unified + '\xff'];

      self.log('readItems', 'Partial match lookup on "' + unified + '"');
      self.logCommand('readItems', 'zrangebylex ' + commandParts.join(' '));

      return self.redis.zrangebylexAsync(commandParts);
    }).then(function (keylist) {
      if (_.isEmpty(keylist))
        return [];

      self.log('readItems', 'Multi-key lookup; cardinality = ' + keylist.length);
      self.logCommand('readItems', 'mget ' + keylist.join(' '));

      return self.redis.mgetAsync(keylist).then(function (data) {
        if (!data)
          return [];
        return _(data).map(function (item) {
          return JSON.parse(item);
        }).compact().value();
      });
    });
  } else {
    self.log('readItems', 'Single-key lookup');
    self.logCommand('readItems', 'get ' + this.unifyKey(key, subkey));

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
  self.logCommand('writeAttributeIfNotExists', 'get ' + self.unifyKey(key, subkey));

  return this.redis.getAsync(this.unifyKey(key, subkey)).then(function (data) {
    data = (data) ? JSON.parse(data) : {};
    if (_.has(data, attr))
      return false;

    data[attr] = value;
    var commandParts = [self.unifyKey(key, subkey), JSON.stringify(data)];

    self.logCommand('writeAttributeIfNotExists', 'set ' + commandParts.join(' '));
    return self.redis.setAsync(commandParts).then(function () {
      return true;
    });
  });
};

RedisWorkerStore.prototype.commit = function () {
  var self = this;
  var multi = self.redis.multi();

  if (!_.isEmpty(self.pendingWrite)) {
    var writeParts = _.flatten(_.pairs(self.pendingWrite));
    self.logCommand('commit', 'set ' + writeParts.join(' '));
    multi.mset(writeParts);
  }

  if (!_.isEmpty(self.pendingDelete)) {
    self.logCommand('commit', 'del ' + self.pendingDelete.join(' '));
    multi.del(self.pendingDelete);
  }

  var subAdd = 0;
  if (!_.isEmpty(self.pendingKeyAdd)) {
    _.each(self.pendingKeyAdd, function (ukeys, key) {
      if (!_.isEmpty(ukeys)) {
        var items = _.flatten(_.zip(_.times(ukeys.length, _.constant(0)), ukeys));
        subAdd += items.length / 2;

        self.logCommand('commit', 'zadd ' + self.indexKey(key) + ' ' + items.join(' '));
        multi.zadd(self.indexKey(key), items);
      }
    });
  }

  var subDel = 0;
  if (!_.isEmpty(self.pendingKeyDel)) {
    _.each(self.pendingKeyDel, function (ukeys, key) {
      if (!_.isEmpty(ukeys)) {
        subDel += ukeys.length;

        self.logCommand('commit', 'zrem ' + self.indexKey(key) + ' ' + ukeys.join(' '));
        multi.zrem(self.indexKey(key), ukeys);
      }
    });
  }

  self.log('commit', 'Write = ' + _.size(self.pendingWrite) + ', Delete = ' + _.size(self.pendingDelete) +
    ', KeyAdd = ' + _.size(self.pendingKeyAdd) + ',' + subAdd + ', KeyDel = ' + _.size(self.pendingDelete) + ',' + subDel);

  return multi.execAsync().then(function () {
    self.pendingWrite = {};
    self.pendingDelete = [];
    self.pendingKeyAdd = {};
    self.pendingKeyDel = {};
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

RedisWorkerStore.prototype.indexKey = function (key) {
  return this.prefix + '-index:' + key;
};

RedisWorkerStore.prototype.log = function (domain, message) {
  if (this.printStatistics)
    console.log('RedisWorkerStore: ' + domain + ': ' + message);
};

RedisWorkerStore.prototype.logCommand = function (domain, command) {
  if (_.isArray(this.commandLog))
    this.commandLog.push(domain + ': ' + command);
};

module.exports = RedisWorkerStore;
