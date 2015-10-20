var WorkerState = require('./workerState'),
    KinesisWorkerState = require('./kinesisWorkerState'),
    KinesisPusher = require('./kinesisPusher');

if (module) {
  module.exports = {
    WorkerState: WorkerState,
    KinesisWorkerState: KinesisWorkerState,
    KinesisPusher: KinesisPusher
  };
}
