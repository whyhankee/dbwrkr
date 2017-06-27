'use strict';


/**
 * Once middleware will block an event if there is already a scheduled event
 * @param {Event} event the event to check
 * @param {DBWrkr} wrkr wrkr instance to work with
 * @param {function} done callback
 */
function once(event, wrkr, done) {
  // This middleware works on the 'once' property
  if (!event.once) return done();

  const findSpec = {
    name: event.name
  };
  if (event.tid) findSpec.tid = event.tid;

  wrkr.find(findSpec, (err, foundEvents) => {
    if (err) return done(err);

    // Mark event as Blocked, DBWrkr.publish will not store it
    if (foundEvents.length) {
      event.__blocked = true;
    }

    event.when = event.once;
    if (typeof event.when === 'number') {   // ms
      event.when = new Date(Date.now()+event.when);
    }

    delete event.once;
    return done();
  });
}


// Exports
module.exports = {
  once: once
};
