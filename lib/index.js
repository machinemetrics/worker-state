var WorkerState = require('./workerState'),
    KinesisWorkerState = require('./kinesisWorkerState'),
    KinesisPusher = require('./kinesisPusher'),
    DynamoTable = require('./table'),
    KinesisUtil = require('./kinesisUtil'),
    LambdaUtil = require('./lambdaUtil'),
    Logger = require('./workerLogger'),
    DynamoWorkerStore = require('./stores/dynamoWorkerStore'),
    S3WorkerStore = require('./stores/s3WorkerStore'),
    RedisWorkerStore = require('./stores/redisWorkerStore'),
    MemoryWorkerStore = require('./stores/memoryWorkerStore');

if (module) {
  module.exports = {
    WorkerState: WorkerState,
    KinesisWorkerState: KinesisWorkerState,
    KinesisPusher: KinesisPusher,
    DynamoTable: DynamoTable,
    KinesisUtil: KinesisUtil,
    LambdaUtil: LambdaUtil,
    Logger: Logger,
    WorkerStores: {
      Dynamo: DynamoWorkerStore,
      S3: S3WorkerStore,
      Redis: RedisWorkerStore,
      Memory: MemoryWorkerStore,
    }
  };
}
