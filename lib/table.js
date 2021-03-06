var AWS = require('aws-sdk-q'),
  https = require('https'),
  marshaler = require('dynamodb-marshaler'),
  _ = require('lodash'),
  Q = require('q'),
  async = require('async-q');

require('q-repeat');

const verbose = +process.env.VERBOSE_WORKER_STATE;

var agent = new https.Agent();
agent.maxSockets = Infinity;
AWS.config.httpOptions = { agent: agent };

function Table (tableName, dynamodb) {
  this.tableName = tableName;
  this.dynamodb = dynamodb || new AWS.DynamoDB();
}

Table.prototype.defaultReadCapacity = 10;
Table.prototype.defaultWriteCapacity = 10;

Table.prototype.hashKey = 'hashkey';
Table.prototype.rangeKey = 'rangekey';

Table.prototype.opTimeout = 2000;
Table.prototype.opMaxRetries = 3;
Table.prototype.opTimeoutLinearBackoff = 0;
Table.prototype.opTimeoutExpBackoff = 1;

Table.prototype.queryItems = function (query) {
  var self = this;
  if (!query.TableName)
    query.TableName = self.tableName;

  return self.dynamodb.query(query).q().catch(function (err) {
    if (_.startsWith(err.message, 'ResourceNotFoundException')) {
      return self.verifyTableValid().then(function () {
        return self.dynamodb.query(query).q();
      });
    }
    throw err;
  }).then(function (data) {
    data.Items = _.map(data.Items, marshaler.unmarshalItem);
    return data;
  });
};

Table.prototype.readItems = function (items) {
  var self = this;
  var keys = _.map(items, function (item) {
    var key = {};
    key[self.hashKey] = { S: item[self.hashKey] };
    key[self.rangeKey] = { S: item[self.rangeKey] };
    return key;
  });

  return batchRead(keys, this);
};

Table.prototype.writeItem = function (item) {
  var self = this;
  var params = {
    TableName: self.tableName,
    Item: marshaler.marshalItem(item)
  };

  return Q.timeoutRetry(self.opTimeout, function () {
    return self.dynamodb.putItem(params).q();
  }, {
    maxRetries: self.opMaxRetries,
    linearBackoff: self.opTimeoutLinearBackoff,
    expBackoff: self.opTimeoutExpBackoff,
  }).catch(function (err) {
    if (verbose) {
      console.error(`Error in table.writeItem: ${err.message}`);
    }
    if (_.startsWith(err.message, 'ResourceNotFoundException')) {
      return self.verifyTableValid().then(function () {
        return self.dynamodb.putItem(params).q();
      });
    }
    throw err;
  });
};

Table.prototype.writeItems = function (items) {
  var records = _.map(items, function (item) {
    return {
      PutRequest: {
        Item: marshaler.marshalItem(item)
      }
    };
  });

  return batchWrite(records, this);
};

Table.prototype.updateItem = function (params) {
  var self = this;
  if (!params.TableName)
    params.TableName = self.tableName;

  return provisionedOperation(function () {
    return Q.timeoutRetry(self.opTimeout, function () {
      return self.dynamodb.updateItem(params).q();
    }, {
      maxRetries: self.opMaxRetries,
      linearBackoff: self.opTimeoutLinearBackoff,
      expBackoff: self.opTimeoutExpBackoff,
    }).catch(function (err) {
      if (_.startsWith(err.message, 'ResourceNotFoundException')) {
        return self.verifyTableValid().then(function () {
          return self.dynamodb.updateItem(params).q();
        });
      }
      throw err;
    });
  });
};

Table.prototype.deleteItem = function (params) {
  var self = this;
  if (!params.TableName)
    params.TableName = self.tableName;

  return Q.timeoutRetry(self.opTimeout, function () {
    return self.dynamodb.deleteItem(params).q();
  }, {
    maxRetries: self.opMaxRetries,
    linearBackoff: self.opTimeoutLinearBackoff,
    expBackoff: self.opTimeoutExpBackoff,
  }).catch(function (err) {
    if (_.startsWith(err.message, 'ResourceNotFoundException'))
      return;
    throw err;
  });
};

Table.prototype.deleteItems = function (items) {
  var records = _.map(items, function (item) {
    return {
      DeleteRequest: item
    };
  });

  return batchWrite(records, this);
};

function batchRead (keys, table) {
  var self = table;
  var backoff = 25;

  return async.until(function () {
    return _.isEmpty(keys);
  }, function () {
    var request = {};
    request[self.tableName] = { Keys: _.take(keys, 100) };
    return self.dynamodb.batchGetItem({
      RequestItems: request
    }).q().then(function (resp) {
      keys = _.drop(keys, 100);

      var unprocessed = _.get(resp, ['UnprocessedKeys', self.tableName, 'Keys'], []);
      if (!_.isEmpty(unprocessed)) {
        keys = keys.concat(unprocessed);
        backoff = Math.min(backoff * 1.5, 1000);
        return Q.delay(backoff);
      }
    }).catch(function (err) {
      if (_.startsWith(err.message, 'ResourceNotFoundException'))
        return self.verifyTableValid();
      if (_.startsWith(err.message, 'ProvisionedThroughputExceededException')) {
        backoff = Math.min(backoff * 1.5, 1000);
        return Q.delay(backoff);
      }

      throw err;
    });
  });
}

function batchWrite (records, table) {
  var self = table;
  var backoff = 25;

  return async.until(function () {
    return _.isEmpty(records);
  }, function () {
    var request = {};
    request[self.tableName] = _.take(records, 25);
    return self.dynamodb.batchWriteItem({
      RequestItems: request
    }).q().then(function (resp) {
      records = _.drop(records, 25);

      var unprocessed = _.get(resp, ['UnprocessedItems', self.tableName], []);
      if (!_.isEmpty(unprocessed)) {
        records = records.concat(unprocessed);
        backoff = Math.min(backoff * 1.5, 1000);
        return Q.delay(backoff);
      }
    }).catch(function (err) {
      if (_.startsWith(err.message, 'ResourceNotFoundException'))
        return self.verifyTableValid();
      if (_.startsWith(err.message, 'ProvisionedThroughputExceededException')) {
        backoff = Math.min(backoff * 1.5, 1000);
        return Q.delay(backoff);
      }
      
      throw err;
    });
  });
}

Table.prototype.verifyTableValid = function () {
  if (this.verifyPromise)
    return this.verifyPromise;

  var self = this;
  var status = '';

  this.verifyPromise = async.until(function () {
    return status == 'ACTIVE'
  }, function () {
    return self.dynamodb.describeTable({ TableName: self.tableName }).q().then(function (data) {
      status = data.Table.TableStatus;
      if (status != 'ACTIVE')
        return Q.delay(1000);
    }).then(function () {
      delete self.verifyPromise;
    }).catch(function (err) {
      if (_.startsWith(err.message, 'ResourceNotFoundException')) {
        return self.dynamodb.createTable({
          AttributeDefinitions: [{
            AttributeName: self.hashKey,
            AttributeType: 'S'
          }, {
            AttributeName: self.rangeKey,
            AttributeType: 'S'
          }],
          KeySchema: [{
            AttributeName: self.hashKey,
            KeyType: 'HASH'
          }, {
            AttributeName: self.rangeKey,
            KeyType: 'RANGE'
          }],
          ProvisionedThroughput: {
            ReadCapacityUnits: self.defaultReadCapacity,
            WriteCapacityUnits: self.defaultWriteCapacity
          },
          TableName: self.tableName
        }).q().delay(1000);
      }
      throw err;
    });
  });

  return this.verifyPromise;
};

function provisionedOperation (op) {
  var backoff = 25;
  var opDone = false;
  var opResult;

  return async.whilst(function () {
    return !opDone;
  }, function () {
    return op().then(function (result) {
      opResult = result;
      opDone = true;
    }).catch(function (err) {
      if (_.startsWith(err.message, 'ProvisionedThroughputExceededException')) {
        backoff = Math.min(backoff * 1.5, 1000);
        return Q.delay(backoff);
      }
      throw err;
    });
  }).then(function () {
    return opResult;
  });
}

module.exports = Table;
