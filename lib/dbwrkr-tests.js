/* eslint no-console: 0 */
'use strict';
var expect = require('expect.js');
var flw = require('flw');
var randomstring = require('random-string');

var DBWrkr = require('./dbwrkr');


//****************************************************************************
//  Globals
//****************************************************************************

var dbwrkr = null;


//****************************************************************************
//  The main testrunner
//****************************************************************************

function run(opt, done) {
  expect(opt.storage).to.be.an(Object);

  dbwrkr = new DBWrkr({
    storage: opt.storage
  });

  return flw.series([
    setup,
    flw.make.series([
      testSubscriptions,
      testPublish,
      testProcess,
      testEventFind,
      testEventFollowUp,
      testEventRetry,
      testPolling
    ]),
    teardown,
    showDone
  ], done);

  function setup(c, cb) {
    console.log('\ndbwrkr tests - Setup');
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
  var rndstr = randomstring();
  var eventName = 'event_'+rndstr;
  var queueName = 'queue_'+rndstr;

  console.log('\nSubscriptions');
  return flw.series([
    noSubscriptions,
    subscribe,
    unsubscribe,
    noSubscriptions
  ], done);

  function subscribe(c, cb) {
    console.log('- subscribe');
    dbwrkr.subscribe(eventName, queueName, (err) => {
      expect(err).to.be(null);
      return numSubscriptions(1, cb);
    });
  }
  function unsubscribe(c, cb) {
    console.log('- unsubscribe');
    dbwrkr.unsubscribe(eventName, queueName, (err) => {
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
    verifyRemove
  ], done);

  function find(c, cb) {
    console.log('- find');
    dbwrkr.find({name: c.event.event.name}, c._store('qitems', cb));
  }

  function verifyFind(c, cb) {
    console.log('- verifyFind');
    expect(c.qitems.length).to.be(1);

    var qitem = c.qitems[0];
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
    dbwrkr.find({eventName: c.event.event.name}, (err, items) => {
      expect(err).to.be(null);
      expect(items.length).to.be(0);
      return cb();
    });
  }
}


//****************************************************************************
//  testProcess
//****************************************************************************

function testProcess(c, done) {
  console.log('\nEvent processing');
  return flw.series([
    flw.wrap(_setupRandomEvent, 'event'),
    processNext,
    verifyNext
  ], done);

  function processNext(c, cb) {
    console.log('- processNext');
    dbwrkr.processNext([c.event.queue], c._store('nextItem', cb));
  }

  function verifyNext(c, cb) {
    console.log('- verifyNext');
    expect(c.nextItem.id).to.be(c.event.newEventIds[0]);
    expect(c.nextItem.name).to.be(c.event.event.name);
    expect(c.nextItem.queue).to.be(c.event.queue);
    expect(c.nextItem.tid).to.be(c.event.event.tid);
    expect(c.nextItem.when).to.be(undefined);
    expect(c.nextItem.done).to.be.a(Date);
    expect(c.nextItem.retryCount).to.be(0);
    expect(c.nextItem.parent).to.be(null);
    expect(c.nextItem.payload).to.eql(c.event.event.payload);
    return cb();
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
      findByName,
      findByTid,
      findByQueue,
      findCombined,
      findInvalidValue,
      findInvalidKey
    ], c, cb);

    function findById(c, cb) {
      return testFindSpec({id: c.event.newEventIds[0]}, c, 1, cb);
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
      var findSpec = {
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
      return testFindSpec({invalidKey: 'invalidKey'}, c, 0, cb);
    }
  }

  function testFindSpec(findSpec, c, expected, cb) {
    console.log('- findBy', Object.keys(findSpec).join(','));
    dbwrkr.find(findSpec, function (err, foundEvents) {
      expect(err).to.be(null);
      expect(foundEvents.length).to.be(expected);

      // We support 0 or 1 results.
      if (!expected) return cb();

      //  Check the content if there is a result
      var foundEvent = foundEvents[0];
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
    name: 'followupEvent',
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
    dbwrkr.fetchNext([c.event.queue], (err, gotEvent) => {
      if (err) return cb(err);
      if (!gotEvent) return cb(new Error('notFound'));

      dbwrkr.followUp(gotEvent, followUpEvent, c._store('newEventIds',cb));
    });
  }

  function checkEvent(c, cb) {
    console.log('- checkEvent');

    const newEventId = c.newEventIds[0];
    dbwrkr.find({id: newEventId}, (err, foundEvents) => {
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
    prossesAndRetry,
    verifyRetryEvent
  ], done);

  function prossesAndRetry(c, cb) {
    console.log('- prossesAndRetry');
    dbwrkr.fetchNext([c.event.queue], (err, gotEvent) => {
      if (err) return cb(err);
      if (!gotEvent) return cb(new Error('notFound'));

      dbwrkr.retry(gotEvent, new Date(), c._store('newEventIds',cb));
    });
  }

  function verifyRetryEvent(c, cb) {
    console.log('- verifyRetryEvent');
    dbwrkr.find({name: c.event.event.name}, (err, events) => {
      expect(err).to.be(null);

      expect(events.length).to.be(2);
      expect(c.event.newEventIds[0]).not.to.be(c.newEventIds[0]);

      var doneEvents = events.filter( (event) => {
        return !!event.done;
      });
      expect(doneEvents[0].id).to.eql(c.event.newEventIds[0]);
      expect(doneEvents[0].retryCount).to.be(0);

      var retryEvents = events.filter( (event) => {
        return !!event.when;
      });
      expect(retryEvents[0].id).to.eql(c.newEventIds[0]);
      expect(retryEvents[0].tid).to.be(c.event.event.tid);
      expect(retryEvents[0].payload).to.eql(c.event.event.payload);
      expect(retryEvents[0].retryCount).to.be(1);
      expect(retryEvents[0].parent).to.be(doneEvents[0].id);
      expect(retryEvents[0].when).to.be.a(Date);
      expect(!retryEvents[0].done);

      return cb();
    });
  }
}


//****************************************************************************
//  testPolling
//****************************************************************************

function testPolling(c, done) {
  console.log('\nPolling');

  return flw.series([
    startPolling,
    stopPolling
  ], done);

  function startPolling(c, cb) {
    console.log('- startPolling');
    dbwrkr.startPolling(['test'], cb);
  }

  function stopPolling(c, cb) {
    console.log('- stopPolling');
    dbwrkr.stopPolling(cb);
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

  var rndStr = randomstring();
  var queueName = 'queue_'+rndStr;
  var event = {
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
  ], (err, c) => {
    expect(err).to.be(null);

    var logStr = `- create ${event.name}, subscribe to ${queueName}`;
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
  }

  function publish(c, cb) {
    if (!opt.publish) return cb();

    return dbwrkr.publish(event, c._store('newEventIds', cb));
  }
}


module.exports = run;
