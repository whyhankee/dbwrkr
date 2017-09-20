/* eslint no-console: 0 */
'use strict';
const debug = require('debug')('dbwrkr:tests');
const flw = require('flw');
const test = require('tape');
const randomstring = require('random-string');

const DBWrkr = require('./dbwrkr');
const middleware = require('./middleware');


//****************************************************************************
//  Globals
//****************************************************************************

let dbwrkr = null;

const queueOptions = {
  idleTimer: 100,
};


//****************************************************************************
//  The main testrunner
//****************************************************************************

function run(opt) {
  dbwrkr = DBWrkr({
    storage: opt.storage
  });

  dbwrkr.on('error', (type, err, event) => {
    if (err) {
      console.log('error processing event ' + event ? event.name : '');
      throw err;
    }
  });

  test('Connect', connect);
  test('Subscriptions', testSubscriptions);
  test('Publish', testPublish);
  test('processNext', testProcessNext);
  test('testEventFind', testEventFind);
  test('testEventFollowUp', testEventFollowUp);
  test('testEventRetry', testEventRetry);
  test('testMiddlewareOnce', testMiddlewareOnce);
  test('testFullQueueFlow', testFullFlow);
  test('benchmark', benchmark);
  test('Disconnect', disconnect);
}


function connect(t) {
  return dbwrkr.connect(t.end.bind(t));
}

function disconnect(t) {
  return dbwrkr.disconnect(t.end.bind(t));
}


function testSubscriptions(t) {
  const rndstr = randomstring();
  const eventName = 'event_'+rndstr;
  const queueName = 'queue_'+rndstr;

  const oldEventCacheSetting = dbwrkr.eventQueueCacheEnabled;

  t.test('disable eventCache', t => {
    dbwrkr.eventQueueCacheEnabled = false;
    t.end();
  });

  t.test(noSubscriptions, '> no subscriptions');
  t.test(subscribe, '> subscribe');
  t.test(unsubscribe, '> unsubscribe');

  t.test('reset eventCache', t => {
    dbwrkr.eventQueueCacheEnabled = oldEventCacheSetting;
    t.end();
  });

  function noSubscriptions(t) {
    return numSubscriptions(0, t);
  }

  function subscribe(t) {
    dbwrkr.subscribe(eventName, queueName, function (err) {
      t.is(err, null);
      return numSubscriptions(1, t);
    });
  }

  function unsubscribe(t) {
    dbwrkr.unsubscribe(eventName, queueName, function (err) {
      t.is(err, null);
      return numSubscriptions(0, t);
    });
  }

  function numSubscriptions(num, t) {
    dbwrkr.subscriptions(eventName, function (err, queues) {
      t.is(err, null, 'no error');
      t.ok(Array.isArray(queues), 'queues is an array');
      t.is(queues.length, num, 'num qitems '+num);
      t.end();
    });
  }
}


function testPublish(t) {
  const rnd = {};

  t.test('> Setup random event', t => {
    _setupRandomEvent(t, rnd);
  });

  t.test('> find & verify', t => {
    dbwrkr.find({name: rnd.event.name}, (err, events) => {
      t.is(err, null, '.find()');
      t.is(events.length, 1, 'found one event');

      const e = events[0];
      t.is(e.id, rnd.newEventIds[0], 'cmp id');
      t.is(e.name, rnd.event.name, 'cmp name');
      t.is(e.queue, rnd.queue, 'cmp queue');
      t.is(e.tid, rnd.event.tid, 'cmp tid');
      t.deepEqual(e.payload, rnd.event.payload, 'cmp payload');
      t.end();
    });
  });

  t.test('> remove', t => {
    dbwrkr.remove({name: rnd.event.name}, err => {
      t.is(err, null, '.remove()');

      dbwrkr.find({name: rnd.event.name}, function (err, items) {
        t.is(err, null, '.find()');
        t.is(items.length, 0, 'no events found');
        t.end();
      });
    });
  });

  t.test('> publish - missing name', t => {
    const event = {
      tid: 'blah'
    };
    dbwrkr.publish(event, (err, eventIds) => {
      t.is(err.toString(), 'Error: missingKey: name', 'correct error');
      t.is(eventIds, undefined, 'eventIds undefined');
      t.end();
    });
  });

  t.test('> publish - invalid property', t => {
    const event = {
      name: 'something',
      tidd: rnd.event.name,   // tid with a typo
    };
    dbwrkr.publish(event, (err, eventIds) => {
      t.is(err.toString(), 'Error: invalidKey:tidd', 'correct error');
      t.is(eventIds, undefined, 'eventIds undefined');
      t.end();
    });
  });


  t.test('> publish - blocked event', t => {
    const event = {
      name: rnd.event.name,
      __blocked: true,
    };
    dbwrkr.publish(event, (err, eventIds) => {
      t.is(err, null, 'no error');
      t.is(eventIds.length, 0, 'no events published');
      t.end();
    });
  });
}


function testProcessNext(t) {
  const queueName = randomstring({length: 20});
  const eventName = 'event_'+queueName;
  let testQueue;

  t.test('> getQueue', t => {
    dbwrkr.queue(queueName, queueOptions, (err, q) => {
      t.is(err, null, 'getQueue()');
      testQueue = q;
      t.end();
    });
  });

  t.test('> setupQueue', t => {
    dbwrkr.subscribe(eventName, queueName, err => {
      t.is(err, null, '.subscribe()');
      t.end();
    });
  });

  t.test('> processNext - no item', t => {
    dbwrkr.__processNext(testQueue, function (err, nextItem) {
      t.is(err, null, 'processNext()');
      t.is(nextItem, undefined, 'no item');
      t.end();
    });
  });

  t.test('> publish, processNext, verify', t => {
    const event = {
      name: eventName,
      tid: eventName,
      payload: {
        extra: 'string'
      }
    };
    dbwrkr.publish(event, (err, newEventIds) => {
      t.is(err, null, '.publish()');

      dbwrkr.__processNext(testQueue, function (err, nextItem) {
        t.is(err, null, '__processNext()');
        t.deepEqual(Object.keys(nextItem).sort(), [
          'created', 'done', 'id', 'name', 'parent',
          'payload','queue', 'retryCount', 'tid', 'when'
        ], 'correct properties');
        t.is(nextItem.created instanceof Date, true, 'cmp created');
        t.is(nextItem.done instanceof Date, true, 'cmp done');
        t.is(nextItem.id, newEventIds[0], 'cmp id');
        t.is(nextItem.name, eventName, 'cmp name');
        t.is(nextItem.parent, undefined, 'cmp parent');
        t.deepEqual(nextItem.payload, event.payload, 'cmp payload');
        t.is(nextItem.queue, queueName, 'cmp queuename');
        t.is(nextItem.retryCount, 0, 'cmp retryCount');
        t.is(nextItem.tid, eventName, 'cmp tid');
        t.is(nextItem.when, undefined, 'cmp when');
        t.end();
      });
    });
  });
}


function testEventFind(t) {
  const rnd = {};

  t.test('> Setup random event', t => {
    _setupRandomEvent(t, rnd);
  });

  t.test('> find by id', t => {
    _testFindSpec({id: rnd.newEventIds[0]}, 1, t);
  });

  t.test('> find by id (array)', t => {
    _testFindSpec({id: rnd.newEventIds}, 1, t);
  });

  t.test('> find by name', t => {
    _testFindSpec({name: rnd.event.name}, 1, t);
  });

  t.test('> find by tid', t => {
    _testFindSpec({tid: rnd.event.tid}, 1, t);
  });

  t.test('> find by queue', t => {
    _testFindSpec({queue: rnd.queue}, 1, t);
  });

  t.test('> find by name, tid, queue', t => {
    const findSpec = {
      name: rnd.event.name,
      tid: rnd.event.tid,
      queue: rnd.queue
    };
    _testFindSpec(findSpec, 1, t);
  });

  t.test('> find by tid (not found)', t => {
    _testFindSpec({tid: 'invalidValue'}, 0, t);
  });

  t.test('> find by invalid key', t => {
    const findSpec = {
      someKey: 'someValue'
    };
    dbwrkr.find(findSpec, (err) => {
      t.is(err.toString(), 'invalid key: someKey', 'correct error');
      t.end();
    });
  });

  t.test('> process qitem', t => {
    dbwrkr.__fetchNext(rnd.queue, (err, event) => {
      t.is(err, null, '__fetchNext()');
      t.is(event.id, rnd.newEventIds[0], 'processed correct event.id');
      t.end();
    });
  });

  t.test('find by name, tid (is processed, should *not* find items)', t => {
    const findSpec = {
      name: rnd.event.name,
      tid: rnd.event.tid,
    };
    _testFindSpec(findSpec, 0, t);
  });

  t.test('find by id (should find it, processed or not)', t => {
    const findSpec = {
      id: rnd.newEventIds[0],
    };
    _testFindSpec(findSpec, 1, t);
  });

  function _testFindSpec(findSpec, expected, t) {
    const findKeys = Object.keys(findSpec);
    const isMultiple = Array.isArray(findSpec[findKeys[0]]);

    dbwrkr.find(findSpec, function (err, foundEvents) {
      t.is(err, null, `.find(${findKeys}), multiple: ${isMultiple}`);
      t.is(foundEvents.length, expected, `found ${expected} events`);
      t.end();
    });
  }
}


function testEventFollowUp(t) {
  const rnd = {};

  t.test('> Setup random event', t => {
    _setupRandomEvent(t, rnd);
  });

  const followUpEvent = {
    name: 'followupEvent_'+randomstring(),
    tid: 12345
  };

  t.test('> subscribeFollowUpEvent', t => {
    dbwrkr.subscribe(followUpEvent.name, rnd.queue, err => {
      t.is(err, null, '.subscribe()');
      t.end();
    });
  });

  t.test('> fetchAndFollowUp', t => {
    dbwrkr.__fetchNext(rnd.queue, function (err, gotEvent) {
      t.is(err, null, '__fetchNext()');
      t.ok(gotEvent, 'gotEvent');

      dbwrkr.followUp(gotEvent, followUpEvent, (err, followUpIds) => {
        t.is(err, null, '.followUp()');

        dbwrkr.find({id: followUpIds}, function (err, foundEvents) {
          t.is(err, null, '.find()');

          const event = foundEvents[0];
          t.is(event.id, followUpIds[0], 'cmp eventId');
          t.is(event.name, followUpEvent.name, 'cmp name');
          t.is(event.tid, String(followUpEvent.tid), 'cmp tid');
          t.is(event.parent, rnd.newEventIds[0], 'cmp parent');
          t.end();
        });
      });
    });
  });
}


function testEventRetry(t) {
  const rnd = {};

  t.test('> Setup random event', t => {
    _setupRandomEvent(t, rnd);
  });

  t.test('> fetchAndRetry', t => {
    dbwrkr.__fetchNext(rnd.queue, function (err, gotEvent) {
      t.is(err, null, '__fetchNext');
      t.ok(gotEvent, 'gotEvent');

      dbwrkr.retry(gotEvent, new Date(), (err, retryIds) => {
        t.is(err, null, '.retry()');

        dbwrkr.find({name: rnd.event.name}, function (err, events) {
          t.is(err, null, '.find()');
          t.is(events.length, 1, 'one event');

          const event = events[0];
          t.is(event.id, retryIds[0], 'cmp id');
          t.is(event.name, rnd.event.name, 'cmp name');
          t.is(event.tid, rnd.event.tid, 'cmp tid');
          t.is(event.retryCount, 1, 'cmp retryCount');
          t.is(event.parent, rnd.newEventIds[0], 'cmp parent');
          t.ok(!event.done, 'cmp done');
          t.ok(event.when instanceof Date, 'cmd when');
          t.end();
        });
      });
    });
  });
}


function testFullFlow(t) {
  const queueName = randomstring({length: 20});
  const eventName = queueName;
  let ourEventReceived = false;
  let ourQueue;

  t.test('> createQueue', t => {
    dbwrkr.queue(queueName, queueOptions, (err, q) => {
      t.is(err, null, 'createQueue');
      ourQueue = q;
      t.end();
    });
  });

  t.test('> setupQueueHandler', t => {
    ourQueue.on(eventName, (event, eventDone) => {
      t.is(event.name, eventName, 'correct eventName');
      ourEventReceived = true;
      return eventDone();
    });
    t.end();
  });

  t.test('> subscribeEvent', t => {
    ourQueue.subscribe(eventName, t.end.bind(t));
  });

  t.test('> listen', t => {
    dbwrkr.listen(t.end.bind(t));
  });

  t.test('> publishEvent', t => {
    const ourEvent = {
      name: queueName,
      tid: queueName,
    };
    dbwrkr.publish(ourEvent, t.end.bind(t));
  });

  t.test('> receiveEvent', t => {
    const checkTimeMs = 50;

    setTimeout(nextCheck, checkTimeMs);
    function nextCheck() {
      if (ourEventReceived) return t.end();
      setTimeout(nextCheck, checkTimeMs);
    }
  });

  t.test('> unsubscribeEvent', t => {
    ourQueue.unsubscribe(eventName, t.end.bind(t));
  });

  t.test('> stopListen', t => {
    dbwrkr.stopListen(t.end.bind(t));
  });
}


function testMiddlewareOnce(t) {
  const onceMs = 30 * 60 * 1000;  // in 30 minutes
  const rnd = {};

  t.test('> setup', t => {
    dbwrkr.use(middleware.once);
    _setupRandomEvent(t, rnd, {publish: false});
  });

  t.test('> publish first event', t => {
    const publishEvent = {
      name: rnd.event.name,
      tid: rnd.event.tid,
      once: onceMs
    };
    dbwrkr.publish(publishEvent, (err, newIds) => {
      t.is(err, null, 'first .publish()');
      t.is(newIds.length, 1, 'one event published');
      t.end();
    });
  });

  t.test('> publish second event', t => {
    const publishEvent = {
      name: rnd.event.name,
      tid: rnd.event.tid,
      once: onceMs
    };
    dbwrkr.publish(publishEvent, (err, newIds) => {
      t.is(err, null, 'second .publish()');
      t.is(newIds.length, 0, 'no events published');
      t.end();
    });
  });

  t.test('> verify event', t => {
    const findSpec = {
      name: rnd.event.name,
      tid: rnd.event.tid,
    };
    dbwrkr.find(findSpec, (err, events) => {
      t.is(err, null, '.find()');
      t.is(events.length, 1, 'found one event');

      const timeStart = new Date(Date.now() + onceMs - 2000);
      const timeEnd = new Date(Date.now() + onceMs + 2000);
      const event = events[0];
      t.ok(
        event.when >= timeStart && event.when <= timeEnd,
        `cmp ${event.when} between ${timeStart} and ${timeEnd}`
      );
      t.end();
    });
  });

  t.test('> reset middleware', t => {
    dbwrkr.clearMiddleware();
    t.end();
  });
}


//****************************************************************************
//  Benchmark
//****************************************************************************

function benchmark(t) {
  if (!process.env.BENCHMARK) {
    t.comment('!> benchmark skipped, No BENCHMARK env');
    return t.end();
  }

  const queueName = 'benchmark_' + randomstring({length: 20});
  const eventName = queueName;

  const numEvents = 3000;
  let numEventsReceived = 0;
  let startTime, stopTime;

  const benchmarkQueueOptions = {
    idleTimer: 1,
    busyTimer: 1
  };

  return flw.series([
    reset,
    setupQueue,
    listenQueue,
    publishEvents,
    listen,
    receiveEvents,
    stopListen,
    reset
  ], err => {
    t.is(err, null, 'no errors');
    t.end();
  });

  function reset(c, cb) {
    dbwrkr.stopListen((err) => {
      if (err) return cb(err);

      dbwrkr.eventQueueCacheEnabled = true;
      dbwrkr.listenQueues = [];
      return cb();
    });
  }

  function setupQueue(c, cb) {
    t.comment('- setupQueue ' + queueName);
    dbwrkr.queue(queueName, benchmarkQueueOptions, c._store('ourQueue', cb));
  }

  function listenQueue(c, cb) {
    t.comment('> listenQueue');
    c.ourQueue.on(eventName, (event, eventDone) => {
      numEventsReceived += 1;
      return eventDone();
    });
    dbwrkr.subscribe(eventName, queueName, cb);
  }

  function publishEvents(c, cb) {
    startTime = new Date();
    t.comment(`> publishing ${numEvents} events`);
    const ourEvents = [{
      name: queueName,
      tid: queueName + 'odd',
    }, {
      name: queueName,
      tid: queueName + 'even',
    }];

    return flw.times(numEvents / 2, publish, (err) => {
      if (err) return cb(err);

      stopTime = new Date();
      t.comment(`> published ${numEvents} events in ${stopTime - startTime} ms`);
      return cb();
    });

    function publish(cb) {
      return dbwrkr.publish(ourEvents, cb);
    }
  }

  function listen(c, cb) {
    t.comment('> listen');

    startTime = new Date();
    dbwrkr.listen(cb);
  }

  function receiveEvents(c, cb) {
    t.comment('> receiving events');

    const checkTimeMs = 100;
    setTimeout(nextCheck, checkTimeMs);

    function nextCheck() {
      if (numEventsReceived === numEvents) {
        stopTime = new Date();
        return cb();
      }
      setTimeout(nextCheck, checkTimeMs);
    }
  }

  function stopListen(c, cb) {
    const totalMs = stopTime - startTime;
    const msPerItem = Math.round(totalMs * 100 / numEvents) / 100;
    t.comment(`> received ${numEvents} events in ${stopTime - startTime} ms, ${msPerItem}ms per item`);
    t.comment('> stop');

    dbwrkr.eventQueueCacheEnabled = false;
    dbwrkr.stopListen(cb);
  }
}


//****************************************************************************
//  helper functions
//****************************************************************************

function _setupRandomEvent(t, obj, opt) {
  if (opt === undefined) {
    opt = {publish: true};
  }

  const rndStr = randomstring();
  const queueName = 'queue_'+rndStr;
  const event = {
    name: 'event_'+rndStr,
    when: new Date,
    tid: 'tid_'+rndStr,
    payload: {
      data: rndStr
    }
  };

  return flw.series([
    subscribe,
    publish
  ], function (err, c) {
    t.is(err, null, `_setupRandomEvent ${event.name} on ${queueName}, publish: ${opt.publish}`);

    obj.event = event;
    obj.queue = queueName;
    obj.newEventIds = c.newEventIds;
    t.end();
  });

  function subscribe(c, cb) {
    dbwrkr.subscribe(event.name, queueName, cb);
    dbwrkr.on('event_'+event.name, function (event, done) {
      debug('got subscribed event', event);
      return done();
    });
  }

  function publish(c, cb) {
    if (!opt.publish) return cb();

    return dbwrkr.publish(event, c._store('newEventIds', cb));
  }
}


module.exports = run;
