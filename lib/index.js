var WorkerState = require('./workerState'),
    KinesisWorkerState = require('./kinesisWorkerState');

if (module) {
  module.exports = {
    WorkerState: WorkerState,
    KinesisWorkerState: KinesisWorkerState
  };
}
