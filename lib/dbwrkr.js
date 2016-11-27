'use strict';
var assert = require('assert');
var events = require('events');
var util = require('util');

var debug = require('debug')('dbwrkr');
var flw = require('flw');


function DBWrkr(opt) {
  assert(typeof opt === 'object', 'opt must be object');
  assert(opt.storage, 'need storage argument');

  this.storage = opt.storage;
  this.pollingStatus = 'stopped';   // started,stopping,stopped
}
util.inherits(DBWrkr, events.EventEmitter);


DBWrkr.prototype.connect = function connect(done) {
  assert(typeof done === 'function', 'done must be a function');

  debug('connecting to storage', {storage: this.storage.constructor.name});
  this.storage.connect(function(err) {
    if (err) return done(err);

    debug('connected to storage');
    return done(null);
  });
};


DBWrkr.prototype.disconnect = function disconnect(done) {
  assert(typeof done === 'function', 'done must be a function');

  debug('diconnecting from storage', {storage: this.storage.constructor.name});
  this.storage.disconnect(function (err) {
    if (err) return done(err);

    debug('disconnected from storage');
    return done(null);
  });
};



DBWrkr.prototype.subscribe = function subscribe(eventName, queueName, done) {
  assert(typeof eventName === 'string', 'eventName must be a string');
  assert(typeof queueName === 'string', 'queueName must be a string');
  assert(typeof done === 'function', 'done must be a function');

  debug('subscribing eventName ' +eventName+ 'to queue '+queueName);
  this.storage.subscribe(eventName, queueName, function (err) {
    if (err) return done(err);

    debug('subscribed eventName '+eventName+' to queue '+queueName);
    return done(null);
  });
};


DBWrkr.prototype.unsubscribe = function unsubscribe(eventName, queueName, done) {
  assert(typeof eventName === 'string', 'eventName must be a string');
  assert(typeof queueName === 'string', 'queueName must be a string');
  assert(typeof done === 'function', 'done must be a function');

  debug('unsubscribing eventName '+eventName+' from queue '+queueName);
  this.storage.unsubscribe(eventName, queueName, function (err) {
    if (err) return done(err);

    debug('unsubscribed eventName '+eventName+' from queue '+queueName);
    return done(null);
  });
};


DBWrkr.prototype.subscriptions = function subscriptions(eventName, done) {
  assert(typeof eventName === 'string', 'eventName must be a string');
  assert(typeof done === 'function', 'done must be a function');

  this.storage.subscriptions(eventName, function (err, queues) {
    debug('subscriptions for eventName '+eventName, queues);
    return done(err, queues);
  });
};



DBWrkr.prototype.publish = function publish(events, done) {
  assert(typeof done === 'function', 'done must be a function');

  var self = this;
  var eventsToStore = [];

  var arrEvents = Array.isArray(events) ? events : [events];
  debug('publish events', arrEvents);

  return flw.each(arrEvents, makeEventsForQueues, function (err) {
    if (err) return done(err);

    debug('publishing events', eventsToStore);

    // Nothing to publish?
    if (eventsToStore.length === 0) return done(null, []);

    return self.storage.publish(eventsToStore, function (err, createdIds) {
      if (err) return done(err);

      // Check for the return of createdIds
      assert(eventsToStore.length === createdIds.length, 'created eventIds returned');
      return done(null, createdIds);
    });
  });

  function makeEventsForQueues(event, cb) {
    assert(event.name, 'event must have a .name property');
    debug('makeQueueItems for event', event.name);

    self.subscriptions(event.name, function (err, qNames) {
      if (err) return cb(err);

      if (qNames.length === 0) {
        debug('no queues for event', event.name);
        return cb();
      }

      qNames.forEach(function (qName) {
        var newEvent = {
          name: event.name,
          queue: qName,
          tid: event.tid || null,
          payload: event.payload || {},
          parent: event.parent || null,
          created: new Date(),
          when: event.when || new Date(),
          retryCount: event.retryCount || 0
        };
        eventsToStore.push(newEvent);
      });
      return cb();
    });
  }
};


DBWrkr.prototype.find = function find(criteria, done) {
  assert(typeof criteria === 'object', 'criteria must be an object');
  assert(typeof done === 'function', 'done must be a function');
  
  this.storage.find(criteria, function (err, items) {
    if (err) return done(err);

    return done(null, items);
  });
};


DBWrkr.prototype.remove = function remove(criteria, done) {
  assert(typeof criteria === 'object', 'criteria must be an object');
  assert(typeof done === 'function', 'done must be a function');

  this.storage.remove(criteria, function (err) {
    return done(err || null);
  });
};


DBWrkr.prototype.followUp = function followUp(originalEvent, events, done) {
  assert(typeof originalEvent === 'object', 'originalEvent must be an object');
  assert(typeof done === 'function', 'done must be a function');

  var publishEvents = Array.isArray(events) ? events : [events];

  publishEvents.forEach(function (e) {
    e.parent = originalEvent.id;
  });
  return this.publish(publishEvents, done);
};


DBWrkr.prototype.retry = function retry(event, when, done) {
  assert(typeof event === 'object', 'event must be an object');
  assert(typeof done === 'function', 'done must be a function');

  if (done === undefined && typeof(when) === 'function') {
    done = when;
    when = null;
  }

  // get the next counter
  var nc = !isNaN(event.retryCount) ? event.retryCount+1 : 1;
  // 10 seconds, then increase seconds, reaches 57 hours in 20 attempts
  var nextSeconds = Math.round(10 + (nc*(nc/5))*(nc*150));
  var nextWhenMs = nextSeconds * 1000;

  // Come on, handle this!
  if (nc > 20) {
    return done(new Error('tooMuchRetries'));
  }

  // Re-publishes the events (new qitems are created)
  event.parent = event.id;
  event.id = undefined;
  event.when = when || new Date(nextWhenMs);
  event.retryCount = nc;
  return this.publish(event, done);
};


DBWrkr.prototype.processNext = function processext(queue, done) {
  assert(typeof queue === 'string', 'queue must be a string');
  assert(typeof done === 'function', 'done must be a function');

  var self = this;

  debug('processNext', {queue: queue});
  this.storage.fetchNext(queue, function (err, event) {
    if (err) return done(err);
    if (!event) return done(null, undefined);

    // First Try specific event_name handler
    var handled = self.emit('event_'+event.name, event, dispatchResult);
    // then, try the general 'event'
    if (!handled) {
      handled = self.emit('event', event, dispatchResult);
    }
    if (!handled) {
      self.emit('error', {
        event: event.name,
        error: 'noHandler',
        eventData: event
      });
      return done(null, event);
    }

    function dispatchResult(err) {
      if (err) {
        self.emit('error', {
          event: event.nane,
          error: err.toString(),
          eventData: event
        });
      }
      // don't return the error, that was related to the event
      //  we continue processing now
      return done(null, event);
    }
  });
};


DBWrkr.prototype.startPolling = function startPolling(queue, opt, done) {
  var self = this;

  if (done === undefined && typeof opt === 'function') {
    done = opt;
    opt = {};
  }

  assert(typeof qeueue === 'string', 'queue must be a string')
  assert(typeof opt === 'object', 'opt must be an object');
  assert(typeof done === 'function', 'done must be a function');

  // Timeout (ms) for checking if there's another event
  // - busyTimer: directly after processing one
  // - idleTimer: for when the previous check had no event
  if (!opt.busyTimer) opt.busyTimer = 10;
  if (!opt.idleTimer) opt.idleTimer = 250;

  debug('polling - start');
  self.pollingStatus = 'started';

  setImmediate(pollNext, opt.idleTimer);
  return done(null);

  function pollNext() {
    debug('polling - get nextItem for queue', queue);
    self.processNext(queue, function (err, event) {
      if (err) self.emit('error', {
        type: 'listen',
        error: err.toString()
      });

      var nextTimeOut = event ? opt.busyTimer : opt.idleTimer;
      debug('polling - next attempt in '+nextTimeOut+'ms');

      if (self.pollingStatus === 'started') {
        setTimeout(pollNext, nextTimeOut);
      }
      else if (self.pollingStatus === 'stopping') {
        self.pollingStatus = 'stopped';
      }
    });
  }
};


DBWrkr.prototype.stopPolling = function stopPolling(done) {
  assert(typeof done === 'function', 'done must be a function');

  var self = this;

  debug('polling - stop');

  if (self.pollingStatus === 'stopped') return done(null);
  if (self.pollingStatus === 'started') {
    self.pollingStatus = 'stopping';
  }

  setImmediate(checkStopped);

  function checkStopped() {
    if (self.pollingStatus === 'stopped') return done();

    debug('stopPolling - waiting for \'stopped\' state');
    setTimeout(checkStopped, 300);
  }
};


// Mostly for testing, fetches the next qitem but does not
//  send an event to the eventEmitter
DBWrkr.prototype.fetchNext = function fetchNext(queue, done) {
  assert(typeof queue === 'string', 'queue must be a string');
  assert(typeof done === 'function', 'done must be a function');

  debug('fetchNext', queue);
  this.storage.fetchNext(queue, done);
};




module.exports = DBWrkr;
