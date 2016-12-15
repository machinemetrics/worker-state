var _ = require('lodash');
var Q = require('q');
var Table = require('../table');

function WorkerStateTable (tableName, dynamodb) {
  Table.call(this, tableName, dynamodb);
}

WorkerStateTable.prototype = _.create(Table.prototype, {
  constructor: WorkerStateTable
});

WorkerStateTable.prototype.hashKey = 'app.key';
WorkerStateTable.prototype.rangeKey = 'shard.key';

function DynamoWorkerStore (tableName, dynamodb) {
  this.table = new WorkerStateTable(tableName, dynamodb);
}

DynamoWorkerStore.prototype.KeyField = 'app.key';
DynamoWorkerStore.prototype.SubkeyField = 'shard.key';

DynamoWorkerStore.prototype.getStore = function (_baseAppKey, _shard) {
  return this;
};

DynamoWorkerStore.prototype.readItems = function (key, subkey, partialMatch) {
  var condition = '#h = :hash AND #s = :shard';
  if (partialMatch)
    condition = '#h = :hash AND begins_with(#s, :shard)';

  return this.table.queryItems({
    ConsistentRead: true,
    ExpressionAttributeNames: {
      '#h': this.KeyField,
      '#s': this.SubkeyField
    },
    ExpressionAttributeValues: {
      ':hash': { S: key },
      ':shard': { S: subkey }
    },
    KeyConditionExpression: condition
  }).then(function (data) {
    return data.Items;
  });
};

DynamoWorkerStore.prototype.writeItem = function (key, subkey, item) {
  return this.table.writeItem(_.extend(this.buildKey(key, subkey), item));
};

DynamoWorkerStore.prototype.deleteItem = function (key, subkey) {
  var keyParams = _.mapValues(this.buildKey(key, subkey), function (val) {
    return { S: val };
  });

  return this.table.deleteItem({
    Key: keyParams
  });
};

DynamoWorkerStore.prototype.deleteItems = function (items) {
  var self = this;
  if (items.length == 0)
    return Q();

  var records = _.map(items, function (item) {
    return {
      Key: {
        'app.key': { S: item[self.KeyField] },
        'shard.key': { S: item[self.SubkeyField] }
      }
    };
  });

  return this.table.deleteItems(records);
};

DynamoWorkerStore.prototype.writeAttributeIfNotExists = function (key, subkey, attr, value) {
  var keyParams = _.mapValues(this.buildKey(key, subkey), function (val) {
    return { S: val };
  });

  var attributes = {};
  attributes[':' + attr] = { S: value };

  return this.table.updateItem({
    Key: keyParams,
    ExpressionAttributeValues: attributes,
    UpdateExpression: 'SET ' + attr + '=:' + attr,
    ConditionExpression: 'attribute_not_exists(' + attr + ')'
  }).then(function () {
    return true;
  }).catch(function (err) {
    if (_.startsWith(err.message, 'ConditionalCheckFailedException'))
      return false;
    throw err;
  });
};

DynamoWorkerStore.prototype.commit = function () {
  return Q();
};

DynamoWorkerStore.prototype.buildKey = function (key, subkey) {
  var params = {};
  params[this.KeyField] = key;
  params[this.SubkeyField] = subkey;
  return params;
};

module.exports = DynamoWorkerStore;
