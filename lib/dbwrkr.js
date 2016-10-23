'use strict';
var assert = require('assert');
var events = require('events');
var util = require('util');

var debug = require('debug')('dbwrkr');
var flw = require('flw');


function DBWrkr(opt) {
  this.storage = opt.storage;
  assert(this.storage, 'storage argument missing');

  this.handlers = {};
  this.pollingStatus = 'stopped';   // started,stopping,stopped
}
util.inherits(DBWrkr, events.EventEmitter);


DBWrkr.prototype.connect = function connect(done) {
  debug('connecting to storage', {storage: this.storage.constructor.name});
  this.storage.connect((err) => {
    if (err) return done(err);

    debug('connected to storage');
    return done(null);
  });
};


DBWrkr.prototype.disconnect = function disconnect(done) {
  debug('diconnecting from storage', {storage: this.storage.constructor.name});
  this.storage.disconnect((err) => {
    if (err) return done(err);

    debug('disconnected from storage');
    return done(null);
  });
};



DBWrkr.prototype.subscribe = function subscribe(eventName, queueName, fn, done) {
  if (done === undefined) {
    done = fn;
    fn = null;
  }

  if (fn) {
    debug(`registering handler for event ${eventName} on queue ${queueName}`);
    if (this.handlers[queueName] === undefined) {
      this.handlers[queueName] = {};
    }
    this.handlers[queueName][eventName] = fn;
  }

  debug(`subscribing eventName ${eventName} to queue ${queueName}`);
  this.storage.subscribe(eventName, queueName, (err) => {
    if (err) return done(err);

    debug(`subscribed eventName ${eventName} to queue ${queueName}`);
    return done(null);
  });
};


DBWrkr.prototype.unsubscribe = function unsubscribe(eventName, queueName, done) {
  debug(`unsubscribing eventName ${eventName} from queue ${queueName}`);
  this.storage.unsubscribe(eventName, queueName, (err) => {
    if (err) return done(err);

    debug(`unsubscribed eventName ${eventName} from queue ${queueName}`);
    return done(null);
  });
};


DBWrkr.prototype.subscriptions = function subscriptions(eventName, done) {
  this.storage.subscriptions(eventName, (err, queues) => {
    debug(`subscriptions for eventName ${eventName}`, queues);
    return done(err, queues);
  });
};



DBWrkr.prototype.publish = function publish(events, done) {
  var self = this;
  var eventsToStore = [];

  var arrEvents = Array.isArray(events) ? events : [events];
  debug('publish events', arrEvents);

  return flw.each(arrEvents, makeEventsForQueues, (err) => {
    if (err) return done(err);

    debug('publishing events', eventsToStore);

    // Nothing to publish?
    if (eventsToStore.length === 0) return done(null, []);

    return this.storage.publish(eventsToStore, (err, createdIds) => {
      if (err) return done(err);

      // Check for the return of createdIds
      assert(eventsToStore.length === createdIds.length, 'created eventIds returned' );
      return done(null, createdIds);
    });
  });

  function makeEventsForQueues(event, cb) {
    debug('makeQueueItems for event', event.name);
    assert(event.name, 'event must have a .name property');

    self.subscriptions(event.name, (err, qNames) => {
      if (err) return cb(err);
      if (qNames.length === 0) {
        debug('no queues for event', event.name);
        return cb();
      }

      qNames.forEach(function (qName) {
        eventsToStore.push({
          name: event.name,
          queue: qName,
          tid: event.tid,
          payload: event.payload,
          parent: event.parent,
          created: new Date(),
          when: event.when || new Date(),
          retryCount: event.retryCount || 0
        });
      });
      return cb();
    });
  }
};


DBWrkr.prototype.find = function find(criteria, done) {
  this.storage.find(criteria, (err, items) => {
    if (err) return done(err);

    return done(null, items);
  });
};


DBWrkr.prototype.remove = function remove(criteria, done) {
  this.storage.remove(criteria, (err) => {
    return done(err || null);
  });
};


DBWrkr.prototype.followUp = function followUp(originalEvent, events, done) {
  var publishEvents = Array.isArray(events) ? events : [events];

  publishEvents.forEach(function (e) {
    e.parent = originalEvent.id;
  });
  return this.publish(publishEvents, done);
};


DBWrkr.prototype.retry = function retry(event, when, done) {
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
  event.done = null;
  event.when = when || new Date(nextWhenMs);
  event.retryCount = nc;
  return this.publish(event, done);
};


DBWrkr.prototype.fetchNext = function fetchNext(queues, done) {
  debug('fetchNext', queues);
  var ourQueues = Array.isArray(queues) ? queues : [queues];

  this.storage.fetchNext(ourQueues, done);
};


DBWrkr.prototype.processNext = function processext(queues, done) {
  var self = this;

  debug('processNext', queues);
  this.storage.fetchNext(queues, (err, event) => {
    if (err) return done(err);
    if (!event) return done(null, undefined);

    var fn;
    if (this.handlers[event.queue] && this.handlers[event.queue][event.name]) {
      fn = this.handlers[event.queue][event.name];
    }

    if (!fn) {
      self.emit('dispatchWarning', {
        event: event.name,
        error: 'No handler'
      });
      return done(null, event);
    }
    return setImmediate(fn, event, dispatchResult);

    function dispatchResult(err) {
      if (err) {
        self.emit('eventError', {
          event: event,
          error: err.toString()
        });
      }
      // don't return the error, that was related to the event
      //  we continue processing now
      return done(null, event);
    }
  });
};


DBWrkr.prototype.startPolling = function startPolling(opt, done) {
  var self = this;

  if (done === undefined && typeof opt === 'function') {
    done = opt;
    opt = {};
  }

  // Timeout (ms) for checking if there's another event
  // - busyTimer: directly after processing one
  // - idleTimer: for when the previous check had no event
  if (!opt.busyTimer) opt.busyTimer = 10;
  if (!opt.idleTimer) opt.idleTimer = 500;

  debug('polling - start');
  self.pollingStatus = 'started';

  setImmediate(pollNext, opt.idleTimer);
  return done(null);

  function pollNext() {
    var pollQueues = Object.keys(self.handlers);
    debug('polling - get nextItem for queues', pollQueues);
    self.processNext(pollQueues, (err, event) => {
      if (err) self.emit('listenError', {error: err.toString()});

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


module.exports = DBWrkr;
