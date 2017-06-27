'use strict';
const assert = require('assert');
const events = require('events');
const util = require('util');

const debug = require('debug')('dbwrkr');
const flw = require('flw');
const lruCache = require('lru-cache');

// Valid event keys just before we store them
const validEventKeys = [
  'name', 'tid', 'payload',
  'parent',
  'when',
  'queue',
  'retryCount',
  '__blocked'
];


/**
 * Create new DBWrkr object
 * @constructor
 * @param {Object} opt options
 * @param {Object} opt.storage - Storage engine to use
 */
function DBWrkr(opt) {
  if (!(this instanceof DBWrkr)) {
    return new DBWrkr(opt);
  }
  assert(typeof opt === 'object', 'opt must be object');
  assert(opt.storage, 'need storage argument');

  this.storage = opt.storage;
  this.listenQueues = {};
  this.middleware = [];

  this.eventQueueCacheEnabled = true;
  this.eventQueueCache = new lruCache({
    max: 20,       // different eventNames
    maxAge: 2000,  // cache time in ms
  });
}
util.inherits(DBWrkr, events.EventEmitter);


/**
 * Install a middleware function
 * @param {function} fn function to use as middleware
 */
DBWrkr.prototype.use = function use(fn) {
  assert(typeof fn === 'function', 'fn must be a function');
  this.middleware.push(fn);
};


/**
 * Clear the current list of middleware
 */
DBWrkr.prototype.clearMiddleware = function () {
  this.middleware = [];
};


/**
 * Get a representation of a Queue to receive it's events
 */
DBWrkr.prototype.queue = function queue(queueName, opt) {
  assert(typeof queueName === 'string', 'queueName must be a string');
  if (!opt) opt = {};

  if (this.listenQueues[queueName]) {
    throw new Error('queueAlreadRegistered');
  }

  this.listenQueues[queueName] = new DBQueue(queueName, opt, this);
  return this.listenQueues[queueName];
};


/**
 * Connect to backend storage
 * @param {function} done callback
 * @see disconnect
 */
DBWrkr.prototype.connect = function connect(done) {
  assert(typeof done === 'function', 'done must be a function');

  debug('connecting to storage', {storage: this.storage.constructor.name});
  this.storage.connect(function (err) {
    if (err) return done(err);

    debug('connected to storage');
    return done(null);
  });
};


/**
 * Disconnect from the backend storage
 * @param {function} done callback
 * @see connect
 */
DBWrkr.prototype.disconnect = function disconnect(done) {
  assert(typeof done === 'function', 'done must be a function');
  const self = this;

  return flw.series([
    stopListen,
    disconnect,
  ], done);

  function stopListen(c, cb) {
    self.stopListen(cb);
  }
  function disconnect(c, cb) {
    debug('disconnecting from storage', {storage: self.storage.constructor.name});
    self.storage.disconnect(function (err) {
      if (err) return done(err);

      debug('disconnected from storage');
      return cb(null);
    });
  }
};


/**
 * Start processing events
 * @param {function} done callback
 */
DBWrkr.prototype.listen = function wrkrListen(done) {
  assert(typeof done === 'function', 'done must be a function');
  const self = this;

  const queues = Object.keys(this.listenQueues);
  return flw.each(queues, startQueue, done);

  function startQueue(queueName, cb) {
    const q = self.listenQueues[queueName];
    q.__listen(cb);
  }
};


/**
 * Stop processing events
 * @param {function} done callback
 */
DBWrkr.prototype.stopListen = function wrkrStopListen(done) {
  assert(typeof done === 'function', 'done must be a function');
  const self = this;

  const queues = Object.keys(this.listenQueues);
  return flw.each(queues, stopQueue, done);

  function stopQueue(queueName, cb) {
    const q = self.listenQueues[queueName];
    q.__stopListen(cb);
  }
};


/**
 * Subscribe an event to a queue
 * @param {string} eventName name of the event to subscribe
 * @param {string} queueName name of the queue to receive the events
 * @param {function} done callback
 * @see unsubscribe subscriptions
 */
DBWrkr.prototype.subscribe = function subscribe(eventName, queueName, done) {
  assert(eventName && typeof eventName === 'string', 'eventName must be a string');
  assert(queueName && typeof queueName === 'string', 'queueName must be a string');
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
  const self = this;

  if (self.eventQueueCacheEnabled) {
    const cachedQueues = self.eventQueueCache.get(eventName);
    if (cachedQueues) {
      return setImmediate(done, null, cachedQueues);
    }
  }

  this.storage.subscriptions(eventName, function (err, queues) {
    if (err) return done(err);

    if (self.eventQueueCacheEnabled) {
      self.eventQueueCache.set(eventName, queues);
    }

    debug('subscriptions for eventName ' + eventName, queues);
    return done(null, queues);
  });
};


/**
 * Publish event(s) to subscribes queues
 * @param {Object} events event(s) to publish
 * @param {string} event.name eventName (required)
 * @param {string} event.tid targetId, e.g. userId (optional)
 * @param {Date} event.when when the event must be processed (default: direct)
 * @param {Object} event.payload payload to send with the event (optional)
 * @param {function} done callback
 * @returns {Array} Array with id's of created events
 * @see followUp retry
 */
DBWrkr.prototype.publish = function publish(events, done) {
  assert(typeof done === 'function', 'done must be a function');
  const self = this;
  const eventsToStore = [];

  // note: make a shallow copy of the events,
  //  we might update or remove keys on the objects
  let eventsToProcess = Array.isArray(events) ? events : [events];
  eventsToProcess = eventsToProcess.map(e => Object.assign({}, e));

  return flw.series([
    processEvents,
    storeEvents,
  ], (err, c) => {
    if (err) return done(err);
    return done(null, c.createdIds);
  });

  function processEvents(c, cb) {
    debug('publish events', eventsToProcess);
    return flw.each(eventsToProcess, processEvent, cb);
  }

  function storeEvents(c, cb) {
    debug('publish - storing events', eventsToStore);
    if (eventsToStore.length === 0) {
      c.createdIds = [];
      return cb();
    }

    self.storage.publish(eventsToStore, (err, createdIds) => {
      if (err) return cb(err);

      assert(
        eventsToStore.length === createdIds.length,
        'created eventIds returned'
      );
      c.createdIds = createdIds;
      return cb();
    });
  }

  function processEvent(event, cb) {
    return flw.series([
      runMiddleware,
      checkValidEventKeys,
      makeQueueEvents
    ], cb);

    function runMiddleware(c, cb) {
      return flw.each(self.middleware, runFn, cb);
      function runFn(fn, cb) {
        return fn(event, self, cb);
      }
    }

    function checkValidEventKeys(c, cb) {
      const errs = [];

      if (!event.name) errs.push('missingEventName');

      Object.keys(event).forEach((k) => {
        if (validEventKeys.indexOf(k) === -1) {
          errs.push('invalidKey:' + k);
        }
      });
      return cb(errs.length ? new Error(errs.join(',')) : null);
    }

    function makeQueueEvents(c, cb) {
      if (event.__blocked) return cb();

      // get subscriptions for queue and create events
      self.subscriptions(event.name, function (err, qNames) {
        if (err) return cb(err);

        if (qNames.length === 0) {
          debug('publish - no queues for event', event.name);
          return cb();
        }

        qNames.forEach(function (qName) {
          const newEvent = {
            name: event.name,
            queue: qName,
            when: event.when || new Date(),
            created: new Date(),
            payload: event.payload || {},
            retryCount: event.retryCount || 0
          };
          if (event.tid) newEvent.tid = event.tid;
          if (event.parent) newEvent.parent = event.parent;
          eventsToStore.push(newEvent);
        });
        return cb();
      });
    }
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
  assert(typeof done === 'function', 'done must be a function');

  const criteriaErrors = __validCriteria(criteria);
  if (criteriaErrors) return done(criteriaErrors);

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
  assert(typeof done === 'function', 'done must be a function');

  const criteriaErrors = __validCriteria(criteria);
  if (criteriaErrors) return done(new Error(criteriaErrors));

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

  const publishEvents = Array.isArray(events) ? events : [events];

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
    when = undefined;
  }

  // get the next retryCounter
  const nc = !isNaN(event.retryCount) ? event.retryCount+1 : 1;
  if (nc > 20) {
    return done(new Error('tooMuchRetries'));
  }

  // Auto retryTimer mechanism
  // Start with 10 seconds, increase slowly, reaches 57 hours in 20 retries
  if (when === undefined) {
    const nextSeconds = Math.round(10 + (nc * (nc / 5)) * (nc * 150));
    when = nextSeconds * 1000;
  }

  // Re-publishes the events (new qitems are created)
  const newEvent = {
    name: event.name,
    queue: event.queue,
    parent: event.id,
    when: event.when,
    retryCount: nc,
  };
  if (event.tid) newEvent.tid = event.tid;
  if (event.payload) newEvent.payload = event.payload;
  return this.publish(newEvent, done);
};


/**
 * Will be called by the listen mechanism to dispatch the next event
 * @private
 * @param {DBQueue} Queue queue you want the next item processed for
 * @param {function} done callback
 * @returns {Object} the processed event
 */
DBWrkr.prototype.__processNext = function processext(q, done) {
  assert(q instanceof DBQueue, 'queue must be a DBQueue');
  assert(typeof done === 'function', 'done must be a function');
  const self = this;

  debug('processNext', {queue: q.queueName});
  self.__fetchNext(q.queueName, function (err, event) {
    if (err) return done(err);
    if (!event) return done(null, undefined);

    let handled = q.emit(event.name, event, dispatchDone);
    if (!handled) {
      handled = q.emit('*', event, dispatchDone);
    }
    if (!handled) {
      self.emit('error', 'noHandler', null, event);
      return done(null, event);
    }

    function dispatchDone(err) {
      if (err) {
        self.emit('error', 'dispatchError', err, event);
      }

      // don't return the error that was related to the event
      //  we continue processing now
      return done(null, event);
    }
  });
};


/**
 * Checks criteria object for valid options/arguments
 * @private
 * @param {Object} criteria object with find/remove spec
 */
function __validCriteria(criteria) {
  const validKeys = ['id', 'name', 'tid', 'queue'];
  const criteriaKeys = Object.keys(criteria);
  const errMsgs = [];

  if (typeof criteria !== 'object') {
    errMsgs.push('criteria must be an object');
  }

  if (!criteriaKeys.length) {
    errMsgs.push('no keys in criteria');
  }

  criteriaKeys.forEach(k => {
    if (validKeys.indexOf(k) === -1) {
      errMsgs.push(`invalid key: ${k}`);
    }

    if (k === 'id') {
      const ids = Array.isArray(criteria[k]) ? criteria[k] : [criteria[k]];
      ids.forEach(i => {
        if (typeof i !== 'string') {
          errMsgs.push('id(s) must be strings');
        }
      });
    } else {
      if (typeof criteria[k] !== 'string' && typeof criteria[k] !== 'number') {
        errMsgs.push(`${k} must be a string|number`);
      }
    }
  });

  if (errMsgs.length) {
    debug('criteria errors', criteria, errMsgs);
  }
  return errMsgs.join(',');
}


/**
 * Fetch the next qitem from the storage engine
 * (does not send events, mainly used for testing)
 * @private
 * @param {string} Queue queue you want the next queueItem for
 * @param {function} done callback
 * @returns {Object} event (or undefined)
 */
DBWrkr.prototype.__fetchNext = function fetchNext(queueName, done) {
  assert(typeof queueName === 'string', 'queue must be a string');
  assert(typeof done === 'function', 'done must be a function');

  debug('fetchNext', queueName);
  this.storage.fetchNext(queueName, done);
};


/**
 * Representation of a queue
 * @param {string} queueName name of the queue
 * @param {Object} opt misc options
 * @param {DBWrkr} wrkr object
 */
function DBQueue(queueName, opt, wrkr) {
  if (!(this instanceof DBQueue)) {
    return new DBQueue(queueName, opt, wrkr);
  }
  assert(typeof queueName === 'string', 'queueName must be a string');
  assert(typeof opt === 'object', 'opt must be an object');
  assert(wrkr instanceof DBWrkr, 'wrkr must be instance of DBWrkr');

  this.wrkr = wrkr;
  this.queueName = queueName;

  this.opt = opt;
  if (!opt.idleTimer) opt.idleTimer = 500;
  if (!opt.busyTimer) opt.busyTimer = 10;

  this.listenStatus = 'stopped';   // started,stopping,stopped
}
util.inherits(DBQueue, events.EventEmitter);


DBQueue.prototype.subscribe = function queueSubscribe(eventName, done) {
  assert(typeof eventName === 'string', 'eventName must be a string');
  this.wrkr.subscribe(eventName, this.queueName, done);
};

DBQueue.prototype.unsubscribe = function queueUnsubscribe(eventName, done) {
  assert(typeof eventName === 'string', 'eventName must be a string');
  this.wrkr.unsubscribe(eventName, this.queueName, done);
};


/**
 * Starts receiving qitems for the given queue
 * @private
 * @param {string} Queue queue you want start listen on
 * @param {Object} opt options
 * @param {number} opt.idleTimer pollTimer when idle (no events on last check) Default: 500ms
 * @param {number} opt.busyTimer pollTimer when busy (got event on last check) Default: 10ms
 * @param {function} done callback
 */
DBQueue.prototype.__listen = function queueListen(done) {
  assert(typeof done === 'function', 'done must be a function');
  const self = this;
  const logContext = {queue: this.queueName};

  if (self.listenStatus === 'started') {
    return done(new Error('alreadyStarted'));
  }

  debug('listen - started', logContext);
  self.listenStatus = 'started';

  setImmediate(fetchNext, this.opt.idleTimer);
  return done(null);

  function fetchNext() {
    debug('listen - get nextItem for queue', logContext);
    self.wrkr.__processNext(self, function (err, event) {
      if (err) self.wrkr.emit('error', {
        error: 'listen',
        description: err.toString()
      });

      if (self.listenStatus === 'started') {
        const nextTimeOut = event ? self.opt.busyTimer : self.opt.idleTimer;
        debug('listen - fetchNext in ' + nextTimeOut + 'ms', {logContext});
        setTimeout(fetchNext, nextTimeOut);
      }
      else if (self.listenStatus === 'stopping') {
        debug('listen - stopping', logContext);
        self.listenStatus = 'stopped';
      }
    });
  }
};


/**
 * Stops receiving qitems for the given queue
 * @param {DBQueue} Queue queue to stop receiving events
 * @param {function} done callback
 */
DBQueue.prototype.__stopListen = function queueStopListen(done) {
  assert(typeof done === 'function', 'done must be a function');
  const self = this;
  const logContext = {queue: this.queueName};

  if (self.listenStatus === 'stopped') {
    debug('stopListen - already stopped', logContext);
    return done(null);
  }
  if (self.listenStatus === 'started') {
    debug('stopListen - stopping', logContext);
    self.listenStatus = 'stopping';
  }

  setImmediate(checkStopped);

  function checkStopped() {
    if (self.listenStatus === 'stopped') {
      debug('stopListen - stopped', logContext);
      return done();
    }

    debug('stoplisten - waiting for \'stopped\' state', logContext);
    setTimeout(checkStopped, 250);
  }
};


// Main export
module.exports = DBWrkr;
