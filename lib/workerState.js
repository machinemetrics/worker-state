var async = require('async-q'),
    moment = require('moment'),
    bigInt = require('big-integer'),
    _ = require('lodash'),
    DynamoWorkerStore = require('./stores/dynamoWorkerStore');

var CheckpointKey = '__checkpoint';

function WorkerState (table, app, shard, dynamodb) {
  this.state = {};
  this.substate = {};
  this.store = new DynamoWorkerStore(table, dynamodb);
  this.app = app;
  this.shard = shard;
  this.checkpoint = '';
  this.checkpointTime = null;
}

WorkerState.DefaultTable = 'KinesisWorkerState';
WorkerState.CheckpointKey = '__checkpoint';

WorkerState.prototype.afterCheckpoint = function (seq) {
  return bigInt(seq).gt(bigInt(this.checkpoint));
};

WorkerState.prototype.getValue = function (key) {
  return _.get(this.state, [key, 'value']);
};

WorkerState.prototype.getSubValue = function (key, subkey) {
  return _.get(this.substate, [key, subkey, 'value']);
};

/**
 * Sets a primary value with an optional timestamp.
 *
 * @param {String} key
 * @param {String} value
 * @param {Object} [timestamp] A moment object; defaults to UTC now.
 */
WorkerState.prototype.setValue = function (key, value, timestamp) {
  var item = _.get(this.state, key, {});

  item.value = value;
  item.sequence = this.checkpoint;
  item.modified = true;
  item.timestamp = timestamp || moment.utc();

  _.set(this.state, key, item);
};

/**
 * Sets a secondary value with an optional timestamp.
 *
 * @param {String} key
 * @param {String} subkey
 * @param {String} value
 * @param {Object} [timestamp] A moment object; defaults to UTC now.
 */
WorkerState.prototype.setSubValue = function (key, subkey, value, timestamp) {
  var item = _.get(this.substate, [key, subkey], {});

  item.value = value;
  item.sequence = this.checkpoint;
  item.modified = true;
  item.timestamp = timestamp || moment.utc();

  _.set(this.substate, [key, subkey], item);
};

WorkerState.prototype.deleteValue = function (key) {
  var item = _.get(this.state, key);
  if (item) {
    item.value = undefined;
    item.modified = true;
  }
};

WorkerState.prototype.deleteSubValue = function (key, subkey) {
  var item = _.get(this.substate, [key, subkey]);
  if (item) {
    item.value = undefined;
    item.modified = true;
  }
};

WorkerState.prototype.initialize = function (keys) {
  var self = this;
  self.initKeys = keys;

  return this.readFromWorkerState(CheckpointKey).then(function (item) {
    if (item) {
      if (item.sequence)
        self.checkpoint = item.sequence;
      if (item.timestamp)
        self.checkpointTime = moment.utc(item.timestamp);
    }
  }).then(function () {
    if (_.isEmpty(keys))
      return;
    if (!self.checkpoint.length && !_.isFinite(self.checkpoint))
      return self.expungeAllKnownSavedState();
    return async.each(keys, function (key) {
      return self.initializeKey(key);
    });
  });
};

WorkerState.prototype.initializeKey = function (key) {
  var self = this;
  return this.store.readItems(this.makeAppKey(key), this.makeShardKey(), true).then(function (data) {
    var cpseq = bigInt(self.checkpoint);
    _.each(data, function (item) {
      var key = self.parseAppKey(item[self.store.KeyField]);
      var subkey = self.parseShardKey(item[self.store.SubkeyField]);

      var value = _.pick(item, ['sequence', 'value', 'previous', 'timestamp']);
      if (cpseq.geq(bigInt(value.sequence)))
        value.previous = _.clone(value.value);
      else {
        value.value = _.clone(value.previous);
        value.sequence = self.checkpoint;
      }

      value.modified = false;
      value.timestamp = moment.utc(value.timestamp);

      if (_.isEmpty(subkey))
        _.set(self.state, key, value);
      else
        _.set(self.substate, [key, subkey], value);
    });
  })
};

WorkerState.prototype.flush = function (seq, timestamp) {
  var self = this;
  return async.each(_.keys(this.state), function (key) {
    var item = self.state[key];
    if (item.modified && !item.simulateFailure)
      return self.writeToWorkerState(key, null, item.value, item.previous, seq, item.timestamp).then(function () {
        item.previous = _.clone(item.value);
        item.modified = false;
        item.sequence = seq;
      });
  }).then(function () {
    return async.each(_.keys(self.substate), function (key) {
      return async.each(_.keys(self.substate[key]), function (subKey) {
        var item = self.substate[key][subKey];
        if (item.modified && !item.simulateFailure)
          return self.writeToWorkerState(key, subKey, item.value, item.previous, seq, item.timestamp).then(function () {
            item.previous = _.clone(item.value);
            item.modified = false;
            item.sequence = seq;
          });
      });
    });
  }).then(function () {
    if (self.simulateFailure)
      return;
    return self.writeToWorkerState(CheckpointKey, null, null, null, seq, timestamp).then(function () {
      self.checkpoint = seq;
      self.checkpointTime = timestamp;
    });
  }).then(function () {
    if (self.simulateFailure)
      return;
    return async.each(_.keys(self.state), function (key) {
      if (_.isUndefined(self.state[key].value))
        return self.deleteFromWorkerState(key, null);
    });
  }).then(function () {
    if (self.simulateFailure)
      return;
    return async.each(_.keys(self.substate), function (key) {
      return async.each(_.keys(self.substate[key]), function (subKey) {
        if (_.isUndefined(self.substate[key][subKey].value))
          return self.deleteFromWorkerState(key, subKey);
      });
    });
  }).then(function () {
    return self.store.commit();
  });
};

/**
 * Deletes any primary or secondary keys with timestamps older than the given timestamp.
 *
 * @param {Object} timestamp A moment object.
 */
WorkerState.prototype.expire = function (timestamp) {
  var self = this;
  _.each(self.state, function (item, key) {
    if (item.timestamp.isBefore(timestamp))
      self.deleteValue(key);
  });

  _.each(self.susbtate, function (item, key) {
    _.each(item, function (subitem, subkey) {
      if (subitem.timestamp.isBefore(timestamp))
        self.deleteSubValue(key, subkey);
    });
  });
};

WorkerState.prototype.readFromWorkerState = function (key, subkey, seq) {
  return this.store.readItems(this.makeAppKey(key), this.makeShardKey(subkey)).then(function (data) {
    var item = _.first(data);
    if (!item || seq && bigInt(item.sequence).gt(bigInt(seq)))
      return;

    item.timestamp = moment.utc(item.timestamp);
    return item;
  });
};

WorkerState.prototype.writeToWorkerState = function (key, subkey, value, previous, seq, timestamp) {
  var item = {};
  if (value)
    item.value = value;
  if (previous)
    item.previous = previous;
  if (seq)
    item.sequence = seq;
  if (timestamp)
    item.timestamp = timestamp.format();

  return this.store.writeItem(this.makeAppKey(key), this.makeShardKey(subkey), item);
};

WorkerState.prototype.deleteFromWorkerState = function (key, subkey) {
  return this.store.deleteItem(this.makeAppKey(key), this.makeShardKey(subkey));
};

/**
 * Deletes all stored records under the given key for this worker's app/shard combination.
 *
 * @param {String} key Primary key
 * @return Promise
 */
WorkerState.prototype.deleteAllFromWorkerState = function (key) {
  var self = this;
  return this.store.readItems(this.makeAppKey(key), this.makeShardKey(), true).then(function (data) {
    return self.store.deleteItems(data);
  }).then(function () {
    delete self.state[key];
    delete self.substate[key];
  });
};

/**
 * Deletes all stored records for all known primary and secondary keys for this worker's app/shard combination.
 * "Known" key sources are the current keys in state, substate, and the keys passed to initialize.
 *
 * @return Promise
 */
WorkerState.prototype.expungeAllKnownSavedState = function () {
  var self = this;
  return async.each(_.union(self.initKeys, _.keys(self.state), _.keys(self.substate)), function (key) {
    return self.deleteAllFromWorkerState(key);
  }).then(function () {
    return self.deleteFromWorkerState(CheckpointKey);
  });
};

/**
 * Merges two parent shard states into the current workerState.
 *
 * Merging a shard recursively copies its parent's state.  If both parents conflict on a key, the data with the more
 * recent sequence number will be copied.  It is advisable to avoid scenarios where two merging parents will have a
 * key collision (except primary keys in the substate table).  All stored state in both parent shards will be deleted.
 *
 * @param {String} shardLeft
 * @param {String} shardRight
 * @return Promise
 */
WorkerState.prototype.mergeShards = function (shardLeft, shardRight) {
  var self = this;
  var stateLeft = new WorkerState(self.store.table.tableName, self.app, shardLeft);
  var stateRight = new WorkerState(self.store.table.tableName, self.app, shardRight);

  return async.each([stateLeft, stateRight], function (state) {
    return state.initialize(self.initKeys);
  }).then(function () {
    if (stateLeft.checkpoint == '' && stateRight.checkpoint == '')
      return;

    self.state = _.merge({}, stateLeft.state, stateRight.state, function (left, right) {
      if (left && right && left.sequence && right.sequence) {
        return (bigInt(left.sequence).gt(bigInt(right.sequence))) ? left : right;
      }
    });
    self.substate = _.merge({}, stateLeft.substate, stateRight.substate, function (left, right) {
      if (left && right && left.sequence && right.sequence)
        return (bigInt(left.sequence).gt(bigInt(right.sequence))) ? left : right;
    });

    _.each(self.state, function (item) {
      item.modified = true;
    });
    _.each(self.substate, function (item) {
      _.each(item, function (subItem) {
        subItem.modified = true;
      });
    });

    return self.flush(bigInt(stateLeft.checkpoint).gt(bigInt(stateRight.checkpoint)) ? stateLeft.checkpoint : stateRight.checkpoint);
  }).then(function () {
    return async.each([stateLeft, stateRight], function (state) {
      return state.expungeAllKnownSavedState();
    });
  });
};

/**
 * Splits a parent shard's state into the current workerState.
 *
 * Splitting a shard copies all of the parent shard's state.  A shard is meant to be split into two children.
 * The first time a shard is split, it will be marked that a transfer has occurred.  The second time a shard is split,
 * the shard's stored state will be deleted.
 *
 * @param {String} shardParent
 * @return Promise
 */
WorkerState.prototype.splitShard = function (shardParent) {
  var self = this;
  var stateParent = new WorkerState(self.store.table.tableName, self.app, shardParent);

  return stateParent.initialize(self.initKeys).then(function () {
    if (stateParent.checkpoint == '')
      return;

    self.state = _.clone(stateParent.state);
    self.substate = _.clone(stateParent.substate);

    _.each(self.state, function (item) {
      item.modified = true;
    });
    _.each(self.substate, function (item) {
      _.each(item, function (subItem) {
        subItem.modified = true;
      });
    });

    return self.flush(stateParent.checkpoint);
  }).then(function () {
    if (stateParent.checkpoint == '')
      return;

    return stateParent.store.writeAttributeIfNotExists(stateParent.makeAppKey(WorkerState.CheckpointKey),
      stateParent.makeShardKey(), 'transfer', self.shard).then(function (success) {
      if (!success)
        return stateParent.expungeAllKnownSavedState();
    });
  });
};

WorkerState.prototype.makeAppKey = function (key) {
  var appValue = this.app;
  if (_.isFinite(key) || (_.isString(key) && key.length > 0))
    appValue += '.' + key;
  return appValue;
};

WorkerState.prototype.makeShardKey = function (key) {
  var shardValue = this.shard || '';
  if (_.isFinite(key) || (_.isString(key) && key.length > 0)) {
    if (this.shard)
      shardValue += '.';
    shardValue += key;
  }
  return shardValue;
};

WorkerState.prototype.parseAppKey = function (key) {
  return _.rest(key.split('.')).join('.');
};

WorkerState.prototype.parseShardKey = function (key) {
  return this.shard ? _.rest(key.split('.')).join('.') : key;
};

module.exports = WorkerState;