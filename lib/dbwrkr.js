'use strict';
var assert = require('assert');
var events = require('events');
var util = require('util');

var debug = require('debug')('dbwrkr');
var flw = require('flw');


/** 
 * Create new DBWrkr object
 * @constructor
 * @param {Object} opt options
 * @property {Object} opt.storage - Storage engine to use
 */
function DBWrkr(opt) {
  assert(typeof opt === 'object', 'opt must be object');
  assert(opt.storage, 'need storage argument');

  this.storage = opt.storage;
  this.pollingStatus = 'stopped';   // started,stopping,stopped
}
util.inherits(DBWrkr, events.EventEmitter);


/** 
 * Connect to backend
 * @param {function} done callback
 * @see disconnect 
 */
DBWrkr.prototype.connect = function connect(done) {
  assert(typeof done === 'function', 'done must be a function');

  debug('connecting to storage', {storage: this.storage.constructor.name});
  this.storage.connect(function(err) {
    if (err) return done(err);

    debug('connected to storage');
    return done(null);
  });
};


/** 
 * Disconnect from the backend
 * @param {function} done callback
 * @see connect 
 */
DBWrkr.prototype.disconnect = function disconnect(done) {
  assert(typeof done === 'function', 'done must be a function');

  debug('diconnecting from storage', {storage: this.storage.constructor.name});
  this.storage.disconnect(function (err) {
    if (err) return done(err);

    debug('disconnected from storage');
    return done(null);
  });
};


/** 
 * Subscribe an event to a queue
 * @param {string} eventName name of the event to subscribe
 * @param {string} queueName name of the queue to receive the events
 * @param {function} done callback
 * @see unsubscribe subscriptions 
 */
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


/** 
 * Unsubscribe an event from a queue
 * @param {string} eventName event to unsubscribe
 * @param {string} queueName queue to remove
 * @param {function} done callback 
 * @see subscribe subscriptions 
 */
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


/** 
 * Get queueNames for given events
 * @param {string} eventName event to get the queueNames for
 * @param {function} done callback
 * @returns {Array} Array with queueNames
 * @see subscribe unsubscribe 
 */
DBWrkr.prototype.subscriptions = function subscriptions(eventName, done) {
  assert(typeof eventName === 'string', 'eventName must be a string');
  assert(typeof done === 'function', 'done must be a function');

  this.storage.subscriptions(eventName, function (err, queues) {
    debug('subscriptions for eventName '+eventName, queues);
    return done(err, queues);
  });
};


/** 
 * Publish event(s) to subscribes queues
 * @param {Object} events event(s) to publish
 * @property {string} event.name eventName (required)
 * @property {string} event.tid targetId (e.g. userId)
 * @property {Date} event.when when the event must be processed (default: direct)
 * @property {Object} event.payload payload to send with the event
 * @param {function} done callback
 * @returns {Array} Array with id's of created events
 * @see followUp retry 
 */
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


/** 
 * Find queueItems based on criteria
 * @param {Object} criteria Object with searchProperties (id:xxx, name:xxx, etc)
 * @param {function} done callback
 * @returns {Array} Array with found events 
 * @see remove
 */
DBWrkr.prototype.find = function find(criteria, done) {
  assert(typeof criteria === 'object', 'criteria must be an object');
  assert(typeof done === 'function', 'done must be a function');
  
  this.storage.find(criteria, function (err, items) {
    if (err) return done(err);

    return done(null, items);
  });
};


/** 
 * Remove queueItems based on criteria
 * @param {Object} criteria Object with searchProperties (id:xxx, name:xxx, etc)
 * @param {function} done callback
 * @see find
 */
DBWrkr.prototype.remove = function remove(criteria, done) {
  assert(typeof criteria === 'object', 'criteria must be an object');
  assert(typeof done === 'function', 'done must be a function');

  this.storage.remove(criteria, function (err) {
    return done(err || null);
  });
};


/** 
 * Followup event with a new event, will publish the new events with the .parent 
 * property set to the originalEvent. Useful for introspection. 
 * (show event-tree of generated events)
 * @param {Object} originalEvent originalEvent that is being processed
 * @param {Object} events Object with new event data
 * @param {function} done callback
 */
DBWrkr.prototype.followUp = function followUp(originalEvent, events, done) {
  assert(typeof originalEvent === 'object', 'originalEvent must be an object');
  assert(typeof done === 'function', 'done must be a function');

  var publishEvents = Array.isArray(events) ? events : [events];

  publishEvents.forEach(function (e) {
    e.parent = originalEvent.id;
  });
  return this.publish(publishEvents, done);
};


/** 
 * Retry an event, will publish a new event with an increased retry-counter
 * After 20 retries (approx 54 hour, an Error will be returned) 
 * @param {Object} event event that need to be retried
 * @param {Date} when when it should be retried (default: incremental retry-timer)
 * @param {function} done callback
 * @returns {Array} id's of generated events 
 */
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


/** 
 * Will be called by the polling mechanism to dispatch the next event
 * @private 
 * @param {string} Queue queue you want the next item processed for
 * @param {function} done callback
 * @returns {Object} the processed event 
 */
DBWrkr.prototype.processNext = function processext(queue, done) {
  assert(typeof queue === 'string', 'queue must be a string');
  assert(typeof done === 'function', 'done must be a function');

  var self = this;

  debug('processNext', {queue: queue});
  this.storage.fetchNext(queue, function (err, event) {
    if (err) return done(err);
    if (!event) return done(null, undefined);

    var handled = self.emit('event_'+event.name, event, dispatchResult);
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


/** 
 * Starts te polling mechanism for the given queue
 * @param {string} Queue queue you want start polling for
 * @param {Object} opt polling options
 * @property {number} opt.idleTimer pollTimer when idle (no events on last check) Default: 500ms
 * @property {number} opt.busyTimer pollTimer when busy (processed event on last check) Default: 10ms
 * @param {function} done callback
 * @see stopPolling
 */
DBWrkr.prototype.startPolling = function startPolling(queue, opt, done) {
  var self = this;

  if (done === undefined && typeof opt === 'function') {
    done = opt;
    opt = {};
  }

  assert(typeof qeueue === 'string', 'queue must be a string');
  assert(typeof opt === 'object', 'opt must be an object');
  assert(typeof done === 'function', 'done must be a function');

  if (!opt.idleTimer) opt.idleTimer = 500;
  if (!opt.busyTimer) opt.busyTimer = 10;

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


/** 
 * Stops the polling mechanism
 * @param {string} Queue queue you want start polling for
 * @param {function} done callback
 * @see startPolling
 */
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


/** 
 * For testing: Fetch the next qitem from the storage engine
 * @private
 * @param {string} Queue queue you want the next queueItem for
 * @param {function} done callback
 * @returns {Object} event (or undefined)
 */
DBWrkr.prototype.fetchNext = function fetchNext(queue, done) {
  assert(typeof queue === 'string', 'queue must be a string');
  assert(typeof done === 'function', 'done must be a function');

  debug('fetchNext', queue);
  this.storage.fetchNext(queue, done);
};




module.exports = DBWrkr;
