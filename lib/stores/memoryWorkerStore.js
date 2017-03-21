'use strict';

const Q = require('q');
const _ = require('lodash');

function MemoryWorkerStore(obj) {
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

/**
 * Constructs a class instance of helper functions for testing with this memory store.  Signatures of methods vary
 * depending on whether bindings or groupKeyBinding are specified during construction.
 *
 * @param expect - Testing instance conforming to chai.expect
 * @param workerState
 * @param bindings - Optional object containing 'sequence' or 'timestamp' properties
 * @param groupKeyBinding - Optional string for implicitly setting a single shard sub key.
 */
MemoryWorkerStore.prototype.createTestHelper = function (expect, workerState, bindings, groupKeyBinding) {
  const workerStore = this;
  const keyField = this.KeyField;
  const subkeyField = this.SubkeyField;

  function TestHelper() { }

  const createInitialStore = function (sequence, timestamp, values) {
    const store = {};

    const setAt = (appKeyPart, shardKeyPart, obj) => {
      const mainKey = workerState.makeAppKey(appKeyPart);
      const shardKey = workerState.makeShardKey(shardKeyPart);

      obj[keyField] = mainKey;
      obj[subkeyField] = shardKey;
      _.set(store, [mainKey, shardKey], obj);
    };

    setAt('__checkpoint', timestamp, { sequence: sequence });
    _.each(values, (groupItems, groupKey) => {
      _.each(groupItems, (data, item) => {
        setAt(item, groupKey, {
          sequence: sequence,
          timestamp: timestamp,
          value: JSON.stringify(data),
        });
      });
    });

    return store;
  };

  if (!bindings && !groupKeyBinding) {
    TestHelper.prototype.createInitialStore = createInitialStore;
  } else if (!bindings) {
    TestHelper.prototype.createInitialStore = function (sequence, timestamp, values) {
      return createInitialStore(sequence, timestamp, { [groupKeyBinding]: values });
    };
  } else if (!groupKeyBinding) {
    TestHelper.prototype.createInitialStore = function (values) {
      return createInitialStore(bindings.sequence || 0, bindings.timestamp, values);
    };
  } else {
    TestHelper.prototype.createInitialStore = function (values) {
      return createInitialStore(bindings.sequence || 0, bindings.timestam, { [groupKeyBinding]: values });
    };
  }

  const validateStore = function (groupKey, key, value) {
    const mainKey = workerState.makeAppKey(key);
    const subKey = workerState.makeShardKey(groupKey);
    const compObj = _.get(workerStore.store, [mainKey, subKey]);
    expect(compObj).to.not.eq(undefined);

    if (_.isObject(value)) {
      const comp = JSON.parse(compObj.value);
      expect(comp).to.deep.eq(value);
    } else {
      expect(compObj.value).to.eq(value);
    }
  };

  if (!groupKeyBinding) {
    TestHelper.prototype.validateStore = validateStore;
  } else {
    TestHelper.prototype.validateStore = function (key, value) {
      validateStore(groupKeyBinding, key, value);
    };
  }

  TestHelper.prototype.validateCardinality = function (cardinality) {
    const comp = _.reduce(workerStore.store, (sum, group) => {
      return sum + _.size(group);
    }, 0);

    expect(comp).to.eq(cardinality + 1);
  };
};

module.exports = MemoryWorkerStore;
