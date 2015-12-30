var Q = require('q'),
    AWS = require('aws-sdk-q'),
    _ = require('lodash');

function S3WorkerStore (bucket, baseAppKey, shard) {
  this.bucket = bucket;
  this.path = baseAppKey + '.' + shard + '.json';
  this.s3 = null;
  this.data = {};
}

S3WorkerStore.prototype.KeyField = 'app.key';
S3WorkerStore.prototype.SubkeyField = 'shard.key';

S3WorkerStore.prototype.getStore = function (baseAppKey, shard) {
  return new S3WorkerStore(this.bucket, baseAppKey, shard);
};

S3WorkerStore.prototype.readItems = function (key, subkey, partialMatch) {
  var self = this;
  return this.initialize().then(function () {
    if (partialMatch) {
      return _(_.get(this.data, key, {})).filter(function (values, refkey) {
        if (_.startsWith(refkey, subkey))
          return _.map(values, function (item) {
            return self.extendItem(item, key, refkey);
          });
      }).flatten().run();
    }
    else
      return self.extendItem(_.get(this.data, [key, subkey]), key, subkey);
  });
};

S3WorkerStore.prototype.writeItem = function (key, subkey, item) {
  _.set(this.data, [key, subkey], item);
  return Q();
};

S3WorkerStore.prototype.deleteItem = function (key, subkey) {
  if (_.has(this.data, [key, subkey]))
    delete this.data[key][subkey];
  return Q();
};

S3WorkerStore.prototype.deleteItems = function (items) {
  var self = this;
  _.each(items, function (item) {
    var key = item[self.KeyField];
    var subkey = item[self.SubkeyField];
    self.deleteItem(key, subkey);
  });

  return Q();
};

S3WorkerStore.prototype.writeAttributeIfNotExists = function (key, subkey, attr, value) {
  var item = _.get(this.data, [key, subkey]);
  if (item && !_.has(item, attr))
    item[attr] = value;
  return Q();
};

S3WorkerStore.prototype.commit = function () {
  if (!this.s3)
    this.s3 = new AWS.S3();

  return this.s3.putObject({
    Bucket: this.bucket,
    Key: this.path,
    Body: JSON.stringify(this.data),
    ContentType: 'text/json'
  }).q();
};

S3WorkerStore.prototype.buildKey = function (key, subkey) {
  var params = {};
  params[this.KeyField] = key;
  params[this.SubkeyField] = subkey;
  return params;
};

S3WorkerStore.prototype.initialize = function () {
  var self = this;
  if (this.s3)
    return Q();

  this.s3 = new AWS.S3();
  return this.s3.getObject({
    Bucket: this.bucket,
    Key: this.path
  }).q().then(function (data) {
    self.data = JSON.parse(data);
  });
};

S3WorkerStore.prototype.extendItem = function (item, key, subkey) {
  if (item) {
    item[this.KeyField] = key;
    item[this.SubkeyField] = subkey;
  }

  return item;
};

module.exports = S3WorkerStore;