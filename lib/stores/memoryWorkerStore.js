'use strict';

const Q = require('q');
const _ = require('lodash');

function MemoryWorkerStore (obj) {
  this.store = obj || {};
}

MemoryWorkerStore.prototype.KeyField = 'app.key';
MemoryWorkerStore.prototype.SubkeyField = 'shard.key';

MemoryWorkerStore.prototype.getStore = function (_baseAppKey, _shard) {
  return this;
};

MemoryWorkerStore.prototype.readItems = function (key, subkey, partialMatch) {
  if (partialMatch) {
    return Q(_(_.get(this.store, key, {})).filter((values, refkey) => {
      return _.startsWith(refkey, subkey);
    }).flatten().value());
  }

  return Q([_.get(this.store, [key, subkey])]);
};

MemoryWorkerStore.prototype.writeItem = function (key, subkey, item) {
  item = _.assign(this.buildKey(key, subkey), _.cloneDeep(item));
  _.set(this.store, [key, subkey], item);

  return Q();
};

MemoryWorkerStore.prototype.deleteItem = function (key, subkey) {
  if (this.store[key] && this.store[key][subkey])
    delete this.store[key][subkey];

  return Q();
};

MemoryWorkerStore.prototype.deleteItems = function (items) {
  _.each(items, (item) => {
    const key = item[this.KeyField];
    const subkey = item[this.SubkeyField];

    if (this.store[key] && this.store[key][subkey])
      delete this.store[key][subkey];
  });

  return Q();
};

MemoryWorkerStore.prototype.writeAttributeIfNotExists = function (key, subkey, attr, value) {
  const item = _.get(this.store, [key, subkey]);
  if (item && _.isUndefined(item.attr)) {
    item.attr = _.cloneDeep(value);
    return Q(true);
  }

  return Q(false);
};

MemoryWorkerStore.prototype.commit = function () {
  return Q();
};

MemoryWorkerStore.prototype.buildKey = function (key, subkey) {
  const params = {};
  params[this.KeyField] = key;
  params[this.SubkeyField] = subkey;
  return params;
};

module.exports = MemoryWorkerStore;
