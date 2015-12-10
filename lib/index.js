var WorkerState = require('./workerState'),
    KinesisWorkerState = require('./kinesisWorkerState'),
    KinesisPusher = require('./kinesisPusher');
    DynamoTable = require('./table');

if (module) {
  module.exports = {
    WorkerState: WorkerState,
    KinesisWorkerState: KinesisWorkerState,
    KinesisPusher: KinesisPusher,
    DynamoTable: Table
  };
}
