process.env['AWS_REGION'] = 'us-west-2';

var _ = require('lodash'),
    bigInt = require('big-integer'),
    should = require('should'),
    WorkerState = require('../lib').WorkerState,
    RedisStore = require('../lib').WorkerStores.Redis;

var workerTable = 'TestWorkerTable';

function makeWorkerState () {
  var store = new RedisStore('localhost', 6379);
  return new WorkerState('wsunit', 'shardId-000000000000', workerTable);
}

function makeWorkerStateWithShard (shard) {
  var store = new RedisStore('localhost', 6379);
  return new WorkerState('wsunit', shard, workerTable);
}

describe('Worker state initialization', function () {
  it('Should initialize empty', function () {
    var state = makeWorkerState();
    return state.initialize([]).then(function () {
      state.checkpoint.should.equal('');
      state.state.should.be.empty();
      state.substate.should.be.empty();
    });
  });

  it('Should initialize empty (keys version)', function () {
    var state = makeWorkerState();
    return state.initialize(['empty1', 'empty2']).then(function () {
      state.checkpoint.should.equal('');
      state.state.should.be.empty();
      state.substate.should.be.empty();
    });
  });

  it('Should initialize checkpoint equivalent to 0', function () {
    var state = makeWorkerState();
    return state.initialize([]).then(function () {
      state.checkpoint.should.equal('');
      bigInt(state.checkpoint).toString().should.equal('0');
      state.afterCheckpoint('0').should.equal(false);
      state.afterCheckpoint('1').should.equal(true);
    });
  });
});

describe('Worker state basic set and get', function () {
  var state;
  before(function () {
    state = makeWorkerState();
    state.initialize().done();
  });

  it('Should set and get primary key', function () {
    state.setValue('set1', 'content');
    itemShouldMatch(state.state.set1, 'content', undefined, state.checkpoint, true);
    state.getValue('set1').should.equal('content');
  });

  it('Should set and get secondary key', function () {
    state.setSubValue('set2', 'sec', 'content');
    itemShouldMatch(state.substate.set2.sec, 'content', undefined, state.checkpoint, true);
    state.getSubValue('set2', 'sec').should.equal('content');
  });

  it('Should update primary key', function () {
    state.setValue('set3', 'content');
    itemShouldMatch(state.state.set3, 'content', undefined, state.checkpoint, true);
    state.setValue('set3', 'update');
    itemShouldMatch(state.state.set3, 'update', undefined, state.checkpoint, true);
    state.getValue('set3').should.equal('update');
  });

  it('Should update secondary key', function () {
    state.setSubValue('set4', 'sec', 'content');
    itemShouldMatch(state.substate.set4.sec, 'content', undefined, state.checkpoint, true);
    state.setSubValue('set4', 'sec', 'update');
    itemShouldMatch(state.substate.set4.sec, 'update', undefined, state.checkpoint, true);
    state.getSubValue('set4', 'sec').should.equal('update');
  });

  it('Should delete primary key', function () {
    state.setValue('set5', 'content');
    itemShouldMatch(state.state.set5, 'content', undefined, state.checkpoint, true);
    state.deleteValue('set5');
    itemShouldMatch(state.state.set5, undefined, undefined, state.checkpoint, true);
  });

  it('Should delete secondary key', function () {
    state.setSubValue('set6', 'sec', 'content');
    itemShouldMatch(state.substate.set6.sec, 'content', undefined, state.checkpoint, true);
    state.deleteSubValue('set6', 'sec');
    itemShouldMatch(state.substate.set6.sec, undefined, undefined, state.checkpoint, true);
  });
});

describe('Primitive dynamo operations', function () {
  var state;
  before(function (done) {
    state = makeWorkerState();
    state.initialize(['write1', 'write2', 'write3', 'write4', 'write5', 'write6']).then(function () {
      done();
    }).done(null, done);
  });

  it('Should set and get primary key', function () {
    return state.writeToWorkerState('write1', null, 'prose', 'old', 1).then(function () {
      return state.commit();
    }).then(function () {
      return state.readFromWorkerState('write1', null, 2).then(function (item) {
        recordShouldMatch(item, 'prose', 'old', 1);
      });
    });
  });

  it('Should not get future-seq item', function () {
    return state.writeToWorkerState('write2', null, 'prose', 'old', 8).then(function () {
      return state.commit();
    }).then(function () {
      return state.readFromWorkerState('write2', null, 3).then(function (item) {
        should.not.exist(item);
      });
    });
  });

  it('Should delete primary key', function () {
    return state.writeToWorkerState('write3', null, 'prose', 'old', 2).then(function () {
      return state.commit();
    }).then(function () {
      return state.deleteFromWorkerState('write3', null).then(function () {
        return state.commit();
      }).then(function () {
        return state.readFromWorkerState('write3', null, 4).then(function (item) {
          should.not.exist(item);
        });
      });
    });
  });

  it('Should set and get secondary key', function () {
    return state.writeToWorkerState('write4', 'sec1', 'prose', 'old', 3).then(function () {
      return state.commit();
    }).then(function () {
      return state.readFromWorkerState('write4', 'sec1', 4).then(function (item) {
        recordShouldMatch(item, 'prose', 'old', 3);
      });
    });
  });

  it('Should delete secondary key', function () {
    return state.writeToWorkerState('write5', 'sec2', 'prose', 'old', 2).then(function () {
      return state.commit();
    }).then(function () {
      return state.deleteFromWorkerState('write5', 'sec2').then(function () {
        return state.commit();
      }).then(function () {
        return state.readFromWorkerState('write5', 'sec2', 4).then(function (item) {
          should.not.exist(item);
        });
      });
    });
  });

  it('Should delete mixed key', function () {
    this.timeout(10000);
    state.initSubkeys = ['sec1', 'sec2'];
    return state.writeToWorkerState('write6', null, 'primary', 'old', 1).then(function () {
      return state.writeToWorkerState('write6', 'sec1', 'secondary', 'old', 2).then(function () {
        return state.writeToWorkerState('write6', 'sec2', 'secondary', 'old', 3);
      });
    }).then(function () {
      return state.commit();
    }).then(function () {
      return state.deleteAllFromWorkerState('write6');
    }).then(function () {
      return state.commit();
    }).then(function () {
      return state.readFromWorkerState('write6', null, 4).then(function (item) {
        should.not.exist(item);
        return state.readFromWorkerState('write6', 'sec1', 4).then(function (item) {
          should.not.exist(item);
          return state.readFromWorkerState('write6', 'sec2', 4).then(function (item) {
            should.not.exist(item);
          });
        });
      });
    })
  });

  after(function (done) {
    state.expungeAllKnownSavedState().then(function () {
      done();
    }).done(null, done);
  });
});

describe('Checkpointing', function () {
  it('Should checkpoint from blank and existing state', function () {
    this.timeout(30000);
    var state = makeWorkerState();
    return state.initialize([]).then(function () {
      state.checkpoint.should.equal('');
      state.setValue('cp1-1', 'a');
      state.setValue('cp1-2', 'b');
      state.setValue('cp1-4', 'e');
      state.setSubValue('cp1-3', 'sub1', 'c');
      state.setSubValue('cp1-3', 'sub2', 'd');
      state.setSubValue('cp1-3', 'sub3', 'f');

      return state.flush(5).then(function () {
        _.each(state.state, function (item) {
          item.modified.should.be.false();
        });
        _.each(state.substate['cp1-3'], function (item) {
          item.modified.should.be.false();
        });
      });
    }).then(function () {
      var restate = makeWorkerState();
      return restate.initialize(['cp1-1', 'cp1-2', 'cp1-3', 'cp1-4'], ['sub1', 'sub2', 'sub3']).then(function () {
        restate.checkpoint.should.equal(5);
        itemShouldMatch(restate.state['cp1-1'], 'a', 'a', 5, false);
        itemShouldMatch(restate.state['cp1-2'], 'b', 'b', 5, false);
        itemShouldMatch(restate.state['cp1-4'], 'e', 'e', 5, false);
        itemShouldMatch(restate.substate['cp1-3'].sub1, 'c', 'c', 5, false);
        itemShouldMatch(restate.substate['cp1-3'].sub2, 'd', 'd', 5, false);
        itemShouldMatch(restate.substate['cp1-3'].sub3, 'f', 'f', 5, false);

        restate.setValue('cp1-2', 'g');
        restate.setSubValue('cp1-3', 'sub2', 'h');
        restate.deleteValue('cp1-4');
        restate.deleteSubValue('cp1-3', 'sub3');

        return restate.flush(10).then(function () {
          itemShouldMatch(restate.state['cp1-2'], 'g', 'g', 10, false);
          itemShouldMatch(restate.state['cp1-4'], undefined, undefined, 10, false);
          itemShouldMatch(restate.substate['cp1-3'].sub2, 'h', 'h', 10, false);
          itemShouldMatch(restate.substate['cp1-3'].sub3, undefined, undefined, 10, false);
        });
      });
    }).then(function () {
      var terstate = makeWorkerState();
      return terstate.initialize(['cp1-1', 'cp1-2', 'cp1-3', 'cp1-4'], ['sub1', 'sub2', 'sub3']).then(function () {
        terstate.checkpoint.should.equal(10);
        itemShouldMatch(terstate.state['cp1-1'], 'a', 'a', 5, false);
        itemShouldMatch(terstate.state['cp1-2'], 'g', 'g', 10, false);
        itemShouldMatch(terstate.substate['cp1-3'].sub1, 'c', 'c', 5, false);
        itemShouldMatch(terstate.substate['cp1-3'].sub2, 'h', 'h', 10, false);

        terstate.state.should.not.have.property('cp1-4');
        terstate.substate['cp1-3'].should.not.have.property('sub3');
      });
    }).then(function () {
      return state.expungeAllKnownSavedState();
    });
  });

  it('Should roll back in checkpoint fail from blank', function () {
    this.timeout(30000);
    var state = makeWorkerState();
    return state.initialize([]).then(function () {
      state.checkpoint.should.equal('');
      state.setValue('cp2-1', 'a');
      state.setValue('cp2-2', 'b');
      state.setSubValue('cp2-3', 'sub1', 'c');
      state.setSubValue('cp2-3', 'sub2', 'd');

      state.simulateFailure = true;
      state.state['cp2-2'].simulateFailure = true;
      state.substate['cp2-3'].sub2.simulateFailure = true;

      return state.flush(5);
    }).then(function () {
      var restate = makeWorkerState();
      return restate.initialize(['cp2-1', 'cp2-2', 'cp2-3'], ['sub1', 'sub2']).then(function () {
        restate.checkpoint.should.equal('');
        _.keys(restate.state).length.should.equal(0);
        _.keys(restate.substate).length.should.equal(0);

        should.not.exist(restate.getValue('cp2-1'));
        should.not.exist(restate.getValue('cp2-2'));
        should.not.exist(restate.getSubValue('cp2-3', 'sub1'));
        should.not.exist(restate.getSubValue('cp2-3', 'sub2'));
      });
    }).then(function () {
      return state.expungeAllKnownSavedState();
    });
  });

  it('Should roll back in checkpoint fail from existing', function () {
    this.timeout(30000);
    var state = makeWorkerState();
    return state.initialize([]).then(function () {
      state.checkpoint.should.equal('');
      state.setValue('cp3-1', 'a');
      state.setValue('cp3-2', 'b');
      state.setValue('cp3-4', 'e');
      state.setSubValue('cp3-3', 'sub1', 'c');
      state.setSubValue('cp3-3', 'sub2', 'd');
      state.setSubValue('cp3-3', 'sub3', 'f');

      return state.flush(5);
    }).then(function () {
      var restate = makeWorkerState();
      return restate.initialize(['cp3-1', 'cp3-2', 'cp3-3', 'cp3-4'], ['sub1', 'sub2', 'sub3']).then(function () {
        restate.checkpoint.should.equal(5);

        restate.setValue('cp3-1', 'i');
        restate.setValue('cp3-2', 'g');
        restate.setSubValue('cp3-3', 'sub1', 'j');
        restate.setSubValue('cp3-3', 'sub2', 'h');
        restate.deleteValue('cp3-4');
        restate.deleteSubValue('cp3-3', 'sub3');

        restate.simulateFailure = true;
        restate.state['cp3-2'].simulateFailure = true;
        restate.state['cp3-4'].simulateFailure = true;
        restate.substate['cp3-3'].sub2.simulateFailure = true;
        restate.substate['cp3-3'].sub3.simulateFailure = true;

        return restate.flush(10);
      });
    }).then(function () {
      var terstate = makeWorkerState();
      return terstate.initialize(['cp3-1', 'cp3-2', 'cp3-3', 'cp3-4'], ['sub1', 'sub2', 'sub3']).then(function () {
        terstate.checkpoint.should.equal(5);
        itemShouldMatch(terstate.state['cp3-1'], 'a', 'a', 5, false);
        itemShouldMatch(terstate.state['cp3-2'], 'b', 'b', 5, false);
        itemShouldMatch(terstate.state['cp3-4'], 'e', 'e', 5, false);
        itemShouldMatch(terstate.substate['cp3-3'].sub1, 'c', 'c', 5, false);
        itemShouldMatch(terstate.substate['cp3-3'].sub2, 'd', 'd', 5, false);
        itemShouldMatch(terstate.substate['cp3-3'].sub3, 'f', 'f', 5, false);
      });
    }).then(function () {
      return state.expungeAllKnownSavedState();
    });
  });
});

describe('Shard splitting', function () {
  it('Should split shard to 2 children and delete parent', function () {
    this.timeout(30000);
    var state1 = makeWorkerStateWithShard('shardId-000000000004');
    var state2 = makeWorkerStateWithShard('shardId-000000000005');
    var state3 = makeWorkerStateWithShard('shardId-000000000006');

    return state1.initialize().then(function () {
      state1.setValue('key1', 'a');
      state1.setValue('key2', 'b');
      state1.setSubValue('key3', 'sub1', 'c');
      state1.setSubValue('key3', 'sub2', 'd');
      return state1.flush(5);
    }).then(function () {
      return state2.initialize(['key1', 'key2', 'key3'], ['sub1', 'sub2']);
    }).then(function () {
      return state2.splitShard(state1.shard);
    }).then(function () {
      state2.checkpoint.should.equal(5);
      itemShouldMatch(state2.state.key1, 'a', 'a', 5, false);
      itemShouldMatch(state2.state.key2, 'b', 'b', 5, false);
      itemShouldMatch(state2.substate.key3.sub1, 'c', 'c', 5, false);
      itemShouldMatch(state2.substate.key3.sub2, 'd', 'd', 5, false);

      state1 = makeWorkerStateWithShard('shardId-000000000004');
      return state1.initialize(['key1', 'key2', 'key3'], ['sub1', 'sub2']);
    }).then(function () {
      state1.checkpoint.should.equal(5);
      itemShouldMatch(state1.state.key1, 'a', 'a', 5, false);
      itemShouldMatch(state1.state.key2, 'b', 'b', 5, false);
      itemShouldMatch(state1.substate.key3.sub1, 'c', 'c', 5, false);
      itemShouldMatch(state1.substate.key3.sub2, 'd', 'd', 5, false);

      return state3.initialize(['key1', 'key2', 'key3'], ['sub1', 'sub2']);
    }).then(function () {
      return state3.splitShard(state1.shard);
    }).then(function () {
      state3.checkpoint.should.equal(5);
      itemShouldMatch(state3.state.key1, 'a', 'a', 5, false);
      itemShouldMatch(state3.state.key2, 'b', 'b', 5, false);
      itemShouldMatch(state3.substate.key3.sub1, 'c', 'c', 5, false);
      itemShouldMatch(state3.substate.key3.sub2, 'd', 'd', 5, false);

      state1 = makeWorkerStateWithShard('shardId-000000000004');
      return state1.initialize(['key1', 'key2', 'key3'], ['sub1', 'sub2']);
    }).then(function () {
      state1.checkpoint.should.equal('');
      _.keys(state1.state).length.should.equal(0);
      _.keys(state1.substate).length.should.equal(0);

      return state2.expungeAllKnownSavedState();
    }).then(function () {
      return state3.expungeAllKnownSavedState();
    });
  });
});

describe('Shard merging', function () {
  it('Should merge and delete parent shards', function () {
    this.timeout(30000);
    var state1 = makeWorkerStateWithShard('shardId-000000000001');
    var state2 = makeWorkerStateWithShard('shardId-000000000002');
    var state3 = makeWorkerStateWithShard('shardId-000000000003');

    return state1.initialize().then(function () {
      state1.setValue('key1', 'a');
      state1.setValue('key2', 'b');
      state1.setSubValue('key3', 'sub1', 'c');
      state1.setSubValue('key4', 'sub1', 'd');
      state1.setSubValue('key4', 'sub2', 'e');
      return state1.flush(5);
    }).then(function () {
      return state2.initialize().then(function () {
        state2.setValue('key1', 'f');
        state2.setValue('key5', 'g');
        state2.setSubValue('key6', 'sub1', 'h');
        state2.setSubValue('key4', 'sub1', 'i');
        state2.setSubValue('key4', 'sub3', 'j');
        return state2.flush(10);
      });
    }).then(function () {
      return state3.initialize(['key1', 'key2', 'key3', 'key4', 'key5', 'key6'], ['sub1', 'sub2', 'sub3']);
    }).then(function () {
      return state3.mergeShards(state1.shard, state2.shard);
    }).then(function () {
      state3.checkpoint.should.equal(10);
      itemShouldMatch(state3.state.key1, 'f', 'f', 10, false);
      itemShouldMatch(state3.state.key2, 'b', 'b', 10, false);
      itemShouldMatch(state3.state.key5, 'g', 'g', 10, false);
      itemShouldMatch(state3.substate.key3.sub1, 'c', 'c', 10, false);
      itemShouldMatch(state3.substate.key4.sub1, 'i', 'i', 10, false);
      itemShouldMatch(state3.substate.key4.sub2, 'e', 'e', 10, false);
      itemShouldMatch(state3.substate.key4.sub3, 'j', 'j', 10, false);
      itemShouldMatch(state3.substate.key6.sub1, 'h', 'h', 10, false);

      state1 = makeWorkerStateWithShard('shardId-000000000001');
      return state1.initialize(['key1', 'key2', 'key3', 'key4'], ['sub1', 'sub2', 'sub3']);
    }).then(function () {
      state1.checkpoint.should.equal('');
      _.keys(state1.state).length.should.equal(0);
      _.keys(state1.substate).length.should.equal(0);

      state2 = makeWorkerStateWithShard('shardId-000000000002');
      return state2.initialize(['key1', 'key4', 'key5', 'key6'], ['sub1', 'sub2', 'sub3']);
    }).then(function () {
      state2.checkpoint.should.equal('');
      _.keys(state2.state).length.should.equal(0);
      _.keys(state2.substate).length.should.equal(0);

      return state3.expungeAllKnownSavedState();
    });
  });
});

function recordShouldMatch (item, value, previous, sequence) {
  undefEqual(item.value, value);
  undefEqual(item.previous, previous);
  undefEqual(item.sequence, sequence);
}

function itemShouldMatch (item, value, previous, sequence, modified) {
  undefEqual(item.value, value);
  undefEqual(item.previous, previous);
  undefEqual(item.sequence, sequence);
  undefEqual(item.modified, modified);
}

function undefEqual (a, b) {
  if (_.isUndefined(a) || _.isUndefined(b))
    return (_.isUndefined(a) && _.isUndefined(b));
  return should.equal(a, b);
}