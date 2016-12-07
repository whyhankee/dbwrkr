/* eslint no-console: 0 */
var DBWrkr = require('../index').DBWrkr;
var DBWrkrMongo = require('dbwrkr-mongodb');
var flw = require('flw');


// Setup DBWorker
var storage = new DBWrkrMongo({
  dbName: 'dbwrkr-example'
});
var wrkr = new DBWrkr({
  storage: storage
});

wrkr.on('error', function (error) {
  console.log('****** - error', error);
});

wrkr.on('event', function (event, done) {
  var delayMs = new Date() - event.created;
  console.log(
    'received event', event.payload.counter, 'delay: ', delayMs,
    event.name, 'from queue', event.queue
  );
  return done();
});


// Start
//
console.log('starting');
return flw.series([
  connect,
  subscribeEvent,
  sendEvents,
  startPolling,
], function (err) {
  if (err) throw err;
});


function connect(c, cb) {
  console.log('connect');
  return wrkr.connect(cb);
}


function subscribeEvent(c, cb) {
  console.log('subscribeEvent');
  return wrkr.subscribe('example_event', 'example_queue', cb);
}


function sendEvents(c, cb) {
  console.log('sendEvents');
  var intervalMs = 333;
  var counter = 0;

  setTimeout(sendEvent, intervalMs);
  return cb();

  function sendEvent() {
    wrkr.publish({
      name: 'example_event',
      payload: {counter: counter}
    }, function (err, resultIds) {
      if (err) throw err;

      console.log('published id : ', counter++, resultIds[0]);
      return setTimeout(sendEvent, intervalMs);
    });
  }
}


function startPolling(c, cb) {
  console.log('startPolling');
  return wrkr.startPolling('example_queue', {
    idleTimer: 500,
    busyTimer: 0
  }, cb);
}
