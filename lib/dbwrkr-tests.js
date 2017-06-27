/* eslint no-console: 0 */
'use strict';
const expect = require('expect.js');
const flw = require('flw');
const randomstring = require('random-string');
const debug = require('debug')('dbwrkr:tests');

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

function run(opt, done) {
  expect(opt.storage).to.be.an(Object);

  dbwrkr = DBWrkr({
    storage: opt.storage
  });

  dbwrkr.on('error', (type, err, event) => {
    if (err) {
      console.log('error processing event ' + event ? event.name : '');
      throw err;
    }
  });

  return flw.series([
    setup,
    flw.make.series([
      testSubscriptions,
      testPublish,
      testProcessNext,
      testEventFind,
      testEventFollowUp,
      testEventRetry,
      testFullQueueFlow,
      testMiddlewareOnce,
    ]),
    benchmark,
    teardown,
    showDone
  ], done);

  function setup(c, cb) {
    console.log('\ndbwrkr tests - Setup');

    dbwrkr.eventQueueCacheEnabled = false;
    return dbwrkr.connect(cb);
  }

  function teardown(c, cb) {
    console.log('\ndbwrkr tests - Teardown');
    return dbwrkr.disconnect(cb);
  }

  function showDone(c, cb) {
    console.log('dbwrkr tests - Done');
    return cb();
  }
}


//****************************************************************************
//  testSubscriptions
//****************************************************************************

function testSubscriptions(c, done) {
  const rndstr = randomstring();
  const eventName = 'event_'+rndstr;
  const queueName = 'queue_'+rndstr;

  console.log('\nSubscriptions');
  return flw.series([
    noSubscriptions,
    subscribe,
    unsubscribe,
    noSubscriptions
  ], done);

  function subscribe(c, cb) {
    console.log('- subscribe');
    dbwrkr.subscribe(eventName, queueName, function (err) {
      expect(err).to.be(null);
      return numSubscriptions(1, cb);
    });
  }

  function unsubscribe(c, cb) {
    console.log('- unsubscribe');
    dbwrkr.unsubscribe(eventName, queueName, function (err) {
      expect(err).to.be(null);
      return numSubscriptions(0, cb);
    });
  }

  function noSubscriptions(c, cb) {
    return numSubscriptions(0, cb);
  }

  function numSubscriptions(num, cb) {
    dbwrkr.subscriptions(eventName, function (err, queues) {
      expect(err).to.be(null);
      expect(queues).to.be.an(Array);
      expect(queues.length).to.be(num);
      return cb();
    });
  }
}


//****************************************************************************
//  testPublish
//****************************************************************************

function testPublish(c, done) {
  console.log('\nPublish');

  return flw.series([
    flw.wrap(_setupRandomEvent, 'event'),
    find,
    verifyFind,
    remove,
    verifyRemove,
    publishNoName,
    publishInvalidKey,
    publishBlockedEvent,
  ], done);

  function find(c, cb) {
    console.log('- find');
    dbwrkr.find({name: c.event.event.name}, c._store('qitems', cb));
  }

  function verifyFind(c, cb) {
    console.log('- verifyFind');
    expect(c.qitems.length).to.be(1);

    const qitem = c.qitems[0];
    expect(qitem.id).to.be(c.event.newEventIds[0]);
    expect(qitem.name).to.be(c.event.event.name);
    expect(qitem.payload).to.eql(c.event.event.payload);
    expect(qitem.queue).to.be(c.event.queue);
    expect(qitem.tid).to.be(c.event.event.tid);
    return cb();
  }

  function remove(c, cb) {
    console.log('- remove');
    dbwrkr.remove({name: c.event.event.name}, cb);
  }

  function verifyRemove(c, cb) {
    console.log('- verifyRemove');
    dbwrkr.find({name: c.event.event.name}, function (err, items) {
      expect(err).to.be(null);
      expect(items.length).to.be(0);
      return cb();
    });
  }

  function publishNoName(c, cb) {
    console.log('- publishNoName');
    const event = {
    };
    dbwrkr.publish(event, (err, eventIds) => {
      expect(err.toString()).to.eql('Error: missingEventName');
      expect(eventIds).to.be(undefined);
      return cb();
    });
  }

  function publishInvalidKey(c, cb) {
    console.log('- publishInvalidKey');
    const event = {
      name: 'something',
      nane: c.event.event.name, // check typo :)
    };
    dbwrkr.publish(event, (err, eventIds) => {
      expect(err.toString()).to.eql('Error: invalidKey:nane');
      expect(eventIds).to.be(undefined);
      return cb();
    });
  }

  function publishBlockedEvent(c, cb) {
    console.log('- publishBlockedEvent');
    const event = {
      name: c.event.event.name,
      __blocked: true,
    };
    dbwrkr.publish(event, (err, eventIds) => {
      expect(err).to.be(null);
      expect(eventIds).to.have.length(0);
      return cb();
    });
  }
}


//****************************************************************************
//  testProcess
//****************************************************************************

function testProcessNext(c, done) {
  console.log('\nProcessNext');
  const queueName = randomstring({length: 20});
  const eventName = 'event_'+queueName;

  return flw.series([
    setupQueue,
    processNextNone,
    publishEvent,
    processNext,
    processNextNone,
  ], done);

  function setupQueue(c, cb) {
    c.ourQueue = dbwrkr.queue(queueName, queueOptions);
    dbwrkr.subscribe(eventName, queueName, cb);
  }

  function processNextNone(c, cb) {
    console.log('- processNext (no item)');
    dbwrkr.__processNext(c.ourQueue, function (err, nextItem) {
      expect(err).to.be(null);
      expect(nextItem).to.be(undefined);
      return cb();
    });
  }

  function publishEvent(c, cb) {
    const event = {
      name: eventName,
      tid: eventName,
      payload: {
        extra: 'string'
      }
    };
    dbwrkr.publish(event, c._store('newEventIds', cb));
  }

  function processNext(c, cb) {
    console.log('- processNext');

    dbwrkr.__processNext(c.ourQueue, function (err, nextItem) {
      expect(err).to.be(null);
      expect(nextItem.id).to.be(c.newEventIds[0]);
      expect(nextItem.name).to.be(eventName);
      expect(nextItem.queue).to.be(queueName);
      expect(nextItem.tid).to.be(eventName);
      expect(!nextItem.when);
      expect(nextItem.done).to.be.a(Date);
      expect(nextItem.retryCount).to.be(0);
      expect(nextItem.parent).to.be(undefined);
      expect(nextItem.payload).to.eql({extra: 'string'});
      return cb();
    });
  }
}


//****************************************************************************
//  testEventFind
//****************************************************************************

function testEventFind(c, done) {
  console.log('\nEvent Find');
  return flw.series([
    flw.wrap(_setupRandomEvent, 'event'),
    testFind
  ], done);

  function testFind(c, cb) {
    return flw.series([
      findById,
      findByIdArray,
      findByName,
      findByTid,
      findByQueue,
      findCombined,
      findInvalidValue,
      findInvalidKey,
      processQitem,
      findProcessed,
      findProcessedById,
    ], c, cb);

    function findById(c, cb) {
      return testFindSpec({id: c.event.newEventIds[0]}, c, 1, cb);
    }
    function findByIdArray(c, cb) {
      return testFindSpec({id: c.event.newEventIds}, c, 1, cb);
    }
    function findByName(c, cb) {
      return testFindSpec({name: c.event.event.name}, c, 1, cb);
    }
    function findByTid(c, cb) {
      return testFindSpec({tid: c.event.event.tid}, c, 1, cb);
    }
    function findByQueue(c, cb) {
      return testFindSpec({queue: c.event.queue}, c, 1, cb);
    }
    function findCombined(c, cb) {
      const findSpec = {
        name: c.event.event.name,
        tid: c.event.event.tid,
        queue: c.event.queue
      };
      return testFindSpec(findSpec, c, 1, cb);
    }
    function findInvalidValue(c, cb) {
      return testFindSpec({tid: 'invalidValue'}, c, 0, cb);
    }
    function findInvalidKey(c, cb) {
      console.log('- findBy (invalid key)');
      const findSpec = {
        someKey: 'someValue'
      };
      dbwrkr.find(findSpec, (err) => {
        expect(err.toString()).to.be('invalid key: someKey');
        return cb();
      });
    }

    function processQitem(c, cb) {
      console.log('- process qitem');

      // qitem should now not be findable (except by ID)
      dbwrkr.__fetchNext(c.event.queue, (err, event) => {
        expect(err).to.be(null);
        expect(event.id).to.be(c.event.newEventIds[0]);
        return cb();
      });
    }

    function findProcessed(c, cb) {
      console.log('- processed - findBy props, not findable)');
      const findSpec = {
        name: c.event.event.name,
        tid: c.event.event.tid,
      };
      dbwrkr.find(findSpec, (err, foundItems) => {
        expect(err).to.be(null);
        expect(foundItems).to.have.length(0);
        return cb();
      });
    }
    function findProcessedById(c, cb) {
      console.log('- processed - findByID, findable)');
      const findSpec = {
        id: c.event.newEventIds[0],
      };
      dbwrkr.find(findSpec, (err, foundItems) => {
        expect(err).to.be(null);
        expect(foundItems).to.have.length(1);
        return cb();
      });
    }
  }

  function testFindSpec(findSpec, c, expected, cb) {
    const findKeys = Object.keys(findSpec);
    const isMultiple = Array.isArray(findSpec[findKeys[0]]);
    console.log('- findBy', findKeys.join(','), isMultiple ? '(multiple)' : '');

    dbwrkr.find(findSpec, function (err, foundEvents) {
      expect(err).to.be(null);
      expect(foundEvents.length).to.be(expected);

      // We support 0 or 1 results.
      if (!expected) return cb();

      //  Check the content if there is a result
      const foundEvent = foundEvents[0];
      expect(foundEvent.id).to.be(c.event.newEventIds[0]);
      expect(foundEvent.queue).to.eql(c.event.queue);
      expect(foundEvent.name).to.eql(c.event.event.name);
      expect(foundEvent.tid).to.eql(c.event.event.tid);
      return cb();
    });
  }
}


//****************************************************************************
//  testEventFollowUp
//****************************************************************************

function testEventFollowUp(c, done) {
  const followUpEvent = {
    name: 'followupEvent_'+randomstring(),
    tid: 12345
  };

  console.log('\ntestEventFollowup');
  return flw.series([
    flw.wrap(_setupRandomEvent, 'event'),
    subscribeFollowUpEvent,
    fetchAndFollowUp,
    checkEvent
  ], done);

  function subscribeFollowUpEvent(c, cb) {
    console.log('- subscribeFollowUpEvent');
    dbwrkr.subscribe(followUpEvent.name, c.event.queue, cb);
  }

  function fetchAndFollowUp(c, cb) {
    console.log('- fetchAndFollowUp');
    dbwrkr.__fetchNext(c.event.queue, function (err, gotEvent) {
      if (err) return cb(err);
      if (!gotEvent) return cb(new Error('notFound'));

      dbwrkr.followUp(gotEvent, followUpEvent, c._store('newEventIds',cb));
    });
  }

  function checkEvent(c, cb) {
    console.log('- checkEvent');

    const newEventId = c.newEventIds[0];
    dbwrkr.find({id: newEventId}, function (err, foundEvents) {
      expect(err).to.be(null);
      const event = foundEvents[0];
      expect(event.idd);
      expect(event.name).to.be(followUpEvent.name);
      expect(event.tid).to.be(12345);
      expect(!event.payload);
      expect(event.parent).to.be(c.event.newEventIds[0]);
      return cb();
    });
  }
}


//****************************************************************************
//  testEventRetry
//****************************************************************************

function testEventRetry(c, done) {
  console.log('\ntestEventRetry');

  return flw.series([
    flw.wrap(_setupRandomEvent, 'event'),
    processAndRetry,
    verifyRetryEvent
  ], done);

  function processAndRetry(c, cb) {
    console.log('- processAndRetry');
    dbwrkr.__fetchNext(c.event.queue, function (err, gotEvent) {
      if (err) return cb(err);
      if (!gotEvent) return cb(new Error('notFound'));

      dbwrkr.retry(gotEvent, new Date(), c._store('newEventIds',cb));
    });
  }

  function verifyRetryEvent(c, cb) {
    console.log('- verifyRetryEvent');
    dbwrkr.find({name: c.event.event.name}, function (err, events) {
      expect(err).to.be(null);

      expect(events.length).to.be(1);
      expect(c.event.newEventIds[0]).not.to.be(c.newEventIds[0]);

      const retryEvents = events.filter(function (event) {
        return !!event.when;
      });
      expect(retryEvents[0].id).to.eql(c.newEventIds[0]);
      expect(retryEvents[0].tid).to.be(c.event.event.tid);
      expect(retryEvents[0].payload).to.eql(c.event.event.payload);
      expect(retryEvents[0].retryCount).to.be(1);
      expect(retryEvents[0].parent).to.be(c.event.newEventIds[0]);
      expect(retryEvents[0].when).to.be.a(Date);
      expect(!retryEvents[0].done);

      return cb();
    });
  }
}


//****************************************************************************
//  testPolling
//****************************************************************************

function testFullQueueFlow(c, done) {
  console.log('\ntestFullQueueFlow');

  const queueName = randomstring({length: 20});
  const eventName = queueName;
  let ourEventReceived = false;

  return flw.series([
    setupQueue,
    subscribeEvent,
    publishEvent,
    listen,
    receiveEvent,
    stopListen
  ], done);

  function setupQueue(c, cb) {
    console.log('- setupQueue '+queueName);
    const ourQueue = dbwrkr.queue(queueName, queueOptions);
    ourQueue.on(eventName, (event, eventDone) => {
      expect(event).to.have.property('name', eventName);
      ourEventReceived = true;
      return eventDone();
    });

    return cb();
  }

  function subscribeEvent(c, cb) {
    console.log(`- subscribe ${eventName} to ${queueName}`);
    dbwrkr.subscribe(eventName, queueName, cb);
  }

  function listen(c, cb) {
    console.log('- listen');
    dbwrkr.listen(cb);
  }

  function publishEvent(c, cb) {
    console.log('- publishEvent');
    const ourEvent = {
      name: queueName,
      tid: queueName,
    };
    dbwrkr.publish(ourEvent, cb);
  }

  function receiveEvent(c, cb) {
    console.log('- receiveEvent');
    const checkTimeMs = 50;

    setTimeout(nextCheck, checkTimeMs);
    function nextCheck() {
      if (ourEventReceived) return cb();
      setTimeout(nextCheck, checkTimeMs);
    }
  }

  function stopListen(c, cb) {
    console.log('- stopListen');
    dbwrkr.stopListen(cb);
  }
}


function testMiddlewareOnce(c, done) {
  console.log('\ntestMiddlewareOnce');
  const onceMs = 60 * 1000;  // in one minute

  dbwrkr.use(middleware.once);

  return flw.series([
    flw.wrap(_setupRandomEvent, [{publish: false}], 'event'),
    publish,
    verifyEvent,
    clearMiddleware,
  ], done);

  function publish(c, cb) {
    console.log('- publish (twice with .once set)');
    c.event.event.once = onceMs;
    return flw.times(2, doPublish, cb);

    function doPublish(cb) {
      dbwrkr.publish(c.event.event, cb);
    }
  }

  function verifyEvent(c, cb) {
    console.log('- verify (1 with date in future)');
    const findSpec = {
      name: c.event.event.name,
      tid: c.event.event.tid,
    };
    dbwrkr.find(findSpec, (err, events) => {
      expect(err).to.be(null);
      expect(events).to.have.length(1);

      const timeStart = new Date(Date.now() + onceMs - 1000);
      const timeEnd = new Date(Date.now() + onceMs + 1000);
      expect(events[0].when).to.be.within(timeStart, timeEnd);
      return cb();
    });
  }

  function clearMiddleware(c, cb) {
    dbwrkr.clearMiddleware();
    return cb();
  }
}

//****************************************************************************
//  helper functions
//****************************************************************************

function benchmark(c, done) {
  console.log('\nbenchmark');

  if (!process.env.BENCHMARK) {
    console.log('- skipped, No BENCHMARK env');
    return done();
  }

  const queueName = 'benchmark_'+randomstring({length: 20});
  const eventName = queueName;

  const numEvents = 5000;
  let numEventsReceived = 0;
  let startTime, stopTime;

  const benchmarkQueueOptions = {
    idleTimer: 1,
    busyTimer: 1
  };

  return flw.series([
    reset,
    setupQueue,
    publishEvents,
    listen,
    receiveEvents,
    stopListen
  ], done);

  function reset(c, cb) {
    dbwrkr.stopListen((err) => {
      if (err) return cb(err);

      dbwrkr.eventQueueCacheEnabled = true;
      dbwrkr.listenQueues = [];
      return cb();
    });
  }

  function setupQueue(c, cb) {
    console.log('- setupQueues ' + queueName);
    const ourQueue = dbwrkr.queue(queueName, benchmarkQueueOptions);
    ourQueue.on(eventName, (event, eventDone) => {
      numEventsReceived+=1;
      return eventDone();
    });

    dbwrkr.subscribe(eventName, queueName, cb);
  }

  function publishEvents(c, cb) {
    startTime = new Date();
    console.log(`- publishing ${numEvents} events`);
    const ourEvents = [{
      name: queueName,
      tid: queueName+'odd',
    }, {
      name: queueName,
      tid: queueName+'even',
    }];

    return flw.times(numEvents/2, publish, (err) => {
      if (err) return cb(err);

      stopTime = new Date();
      console.log(`- published ${numEvents} events in ${stopTime - startTime} ms`);
      return cb();
    });

    function publish(cb) {
      return dbwrkr.publish(ourEvents, cb);
    }
  }

  function listen(c, cb) {
    console.log('- listen');

    startTime = new Date();
    dbwrkr.listen(cb);
  }

  function receiveEvents(c, cb) {
    console.log('- receiving events');

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
    const msPerItem = Math.round(totalMs*100 / numEvents) / 100;
    console.log(`- received ${numEvents} events in ${stopTime - startTime} ms, ${msPerItem}ms per item`);
    console.log('- stop');

    dbwrkr.eventQueueCacheEnabled = false;
    dbwrkr.stopListen(cb);
  }
}


//****************************************************************************
//  helper functions
//****************************************************************************

function _setupRandomEvent(opt, done) {
  if (done === undefined && typeof(opt) === 'function') {
    done = opt;
    opt = {
      publish: true
    };
  }

  const rndStr = randomstring();
  const queueName = 'queue_'+rndStr;
  const event = {
    'name': 'event_'+rndStr,
    'when': new Date,
    'tid': 'tid_'+rndStr,
    'payload': {
      'data': rndStr
    }
  };

  return flw.series([
    subscribe,
    publish
  ], function (err, c) {
    expect(err).to.be(null);

    let logStr = '- create '+event.name+', subscribe to '+queueName;
    if (opt.publish) logStr += ', publish';
    console.log(logStr);

    return done(null, {
      event: event,
      queue: queueName,
      newEventIds: c.newEventIds
    });
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
