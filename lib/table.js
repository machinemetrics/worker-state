var AWS = require('aws-sdk-q'),
  marshaler = require('dynamodb-marshaler'),
  _ = require('lodash'),
  Q = require('q'),
  async = require('async-q');

function Table (tableName, dynamodb) {
  this.tableName = tableName;
  this.dynamodb = dynamodb || new AWS.DynamoDB();
}

Table.prototype.defaultReadCapacity = 10;
Table.prototype.defaultWriteCapacity = 10;

Table.prototype.hashKey = 'hashkey';
Table.prototype.rangeKey = 'rangekey';

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

Table.prototype.writeItem = function (item) {
  var self = this;
  var params = {
    TableName: self.tableName,
    Item: marshaler.marshalItem(item)
  };

  return this.dynamodb.putItem(params).q().catch(function (err) {
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

  return this.dynamodb.updateItem(params).q().catch(function (err) {
    if (_.startsWith(err.message, 'ResourceNotFoundException')) {
      return self.verifyTableValid().then(function () {
        return self.dynamodb.updateItem(params).q();
      });
    }
    throw err;
  });
};

Table.prototype.deleteItem = function (params) {
  var self = this;
  if (!params.TableName)
    params.TableName = self.tableName;

  return this.dynamodb.deleteItem(params).q().catch(function (err) {
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
      throw err;
    });
  });
}

Table.prototype.verifyTableValid = function () {
  var self = this;
  var status = '';

  return async.until(function () {
    return status == 'ACTIVE'
  }, function () {
    return self.dynamodb.describeTable({ TableName: self.tableName }).q().then(function (data) {
      status = data.Table.TableStatus;
      if (status != 'ACTIVE')
        return Q.delay(1000);
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
};

module.exports = Table;