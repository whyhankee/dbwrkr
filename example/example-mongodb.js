/* eslint no-console: 0 */
var DBWrkr = require('dbwrkr').DBWrkr;
var DBWrkrMongo = require('dbwrkr-mongodb');
var flw = require('flw');


// Setup DBWorker
//
var wrkr = new DBWrkr({
  storage: new DBWrkrMongo({
    dbName: 'dbwrkr-example'
  })
});
wrkr.on('error', function (error) {
  console.log('****** - error', error);
});
wrkr.on('dispatchWarning', function (warning) {
  console.log('****** - warning', warning);
});
wrkr.on('eventError', function (eventError) {
  console.log('****** - eventError', eventError);
});



// Start
//
return flw.series([
  connect,
  subscribeEvent,
  sendEvents,
  startPolling,
  // disconnect, -> will exit the eventLoop  ^c to abort
], function (err) {
  if (err) throw err;
});


function connect(c, cb) {
  console.log('connect');
  return wrkr.connect(cb);
}
// function disconnect(c, cb) {
//   console.log('disconnect');
//   return wrkr.disconnect(cb);
// }


function subscribeEvent(c, cb) {
  console.log('subscribeEvent');
  return wrkr.subscribe('example_event', 'example_queue', exampleEventHandler, cb);

  function exampleEventHandler(event, done) {
    console.log('got event', event.id, event.name, event.created);
    return done();
  }
}


function sendEvents(c, cb) {
  console.log('sendEvents');
  var intervalMs = 500;

  setTimeout(sendEvents, intervalMs);
  return cb();

  function sendEvents() {
    // publish our example event
    wrkr.publish({name: 'example_event'}, function (err, resultIds) {
      console.log('published id : ', resultIds[0]);

      return setTimeout(sendEvents, intervalMs);
    });
  }
}


function startPolling(c, cb) {
  console.log('startPolling');
  return wrkr.startPolling(cb);
}
