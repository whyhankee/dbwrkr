## DBWrkr - A general use API for an easy to use pub-sub scheduler.

### What?

* A pub-sub system would make it easy for multiple processes to communicate with each other by sending events. Other processes can pickup the events, do what need to be done (and followUp by sending another event).

* This way, by reacting to events you create loose coupling, little components that are easy to maintain, deploy and removed.

* Easy storage-plugin system for any storage devices like MongoDB, Postgres, Redis, etc.

* It's not build for performance, it's for flexibility and introspection.

### Current state

* **Disclaimer: This is *very alpha* everything could happen**.

* DBWrkr currently only has one backend: <https://github.com/whyhankee/dbwrkr-mongodb>.

## How does it work ?

* Setup a wrkr object, see the API documentation

### To publish events you need to

* wrkr.connect() to connect to the DBWorker storage
* wrkr.publish() an event just because you can

### To process events to you need to:

* wrkr.connect() to connect to the DBWorker storage
* wrkr.subscribe() to the events so the events will be collected in the designated queue
* wrkr.startPolling() to start receiving your events
* optional - wrkr.publish() an event just because you also can
* optional - wrkr.retry() an event if something went wrong
* optional - wrkr.followUp() with events of your own in reply to other events
* wrkr.stopPolling() when you are done (signal handler?)

See the `example/` directory for an example


## API

### Setup

* Require the modules (DBWrkr and storage module),
* Create a storage object
* Create the worker with the storage object

```
var wrkr = require('dbwrkr');
var DBWrkrMongodb = require('dbwrkr-mongodb');

var wrkrBackend = new DBWrkrMongodb({
  dbName: 'wrkr_blah'
});

var wrkr = new wrkr.DBWrkr({
	storage: wrkrBackend
});
```

### connect()

Connect to the backend storage engine

```
wrkr.connect(options, callback);
```

options:
* opt.idleTimer (default: 10 ms)
* opt.busyTimer (default: 500 ms)


### disconnect()

Disconnect from the backend storage engine

```
wrkr.disconnect(callback);
```

### subscribe()

Subscribe an event to a queue. When polling the handler will be called when this event arrives.

```
wrkr.subscribe(eventName, queueName, handler, callback)
```

### unsubscribe()

Unsubscribe and event from a queue. The handler will still be called for all the events are already in the queue. New events will no longer be queued.

```
wrkr.unsubscribe(eventName, queueName, handler, callback)
```

Notes:
* A handler is still required as there may still be events arriving.
* Remove the unsubscribe line when all the remaining events are processed.


### subscriptions()

Get a list of queues that are subscribed to the event.

```
wrkr.subscriptions(eventName, (err, queues) => {
  // queues is an array with the names of the queues
})
```

### publish()

Publish a new event. Events will we created for each queue that is subscribed to the event.

```
var events = [{
  name: 'yourapp.user.signup',
  tid: user.id,
}];

wrkr.publish(events, (err, eventIds) => {
  // eventIds is an array with the ids the created events
})
```

optional event properties:
* when: Date object when the event should be processed
* payload: object with extra information


### followUp()

FollowUp one event with another event. This will publish new event(s) with the parent set to the current event. This will help with the introspection system.

```
var newEvent = {
  name: 'yourapp.user.sentWelcomeMessage',
  tid: user.id,
};
wrkr.followUp(event, newEvent, (err, eventIds) => {
  // eventIds is an array with the created events
})
```

### retry()

Create a new retry event with the data of the current event, Will increase the retryCount on the new event.

```
wrkr.retry(event, when, (err, eventIds) => {
  // eventIds is an array with the created events
})
```

Notes:
* the `when` argument is optional. The default (crappy) algorithm will increase the retry-seconds until it reaches 20 (in about 57 hours)
* retry() will callback an error when retryCount reaches 20

### find()

Find events in the system.

```
var criteria = {
  name: eventName,
  tid: user.id,
};
wrkr.find(criteria, (err, events) => {
  // events is an array with matched events
})
```

### remove()

Remove events in the system.

```
var criteria = {
  name: eventName,
  tid: user.id,
};
wrkr.remove(criteria, (err, events) => {
  // events is an array with matched events
})
```

### startPolling()

Starts the polling mechanism.
* Will get all events from all queues the are subscribed to.


```
wrkr.startPolling(callback);
```

Note:
* Even when a process has subscribed to one event in a queue it will still receive *all* events from that qeuue
* Processing of events in queues should be in order (on the 'when' field).

### stopPolling()

Stops the polling mechanism. The callback will be called when the current event is processed.

```
wrkr.stopPolling(callback);
```


## Testing, developing and debugging

Notes:

* The examples directory contains an example for use with mongodb. You might need to link the dbwrkr-mongodb package though.

* The dbwrk pcakage contains the tests. They are called from the storage engine, see the mongodb storage engine for more info.


### Debugging

  DEBUG=wrkr* node ./example/example-mongodb.js


### Todo

* middleware (once & cron)
* cleanup system (remove/archive old events)
* in-memory-storage engine for running the tests
* storage-engines (Postgres & Rethinkdb)
* Promise callbacks?


### Links

Storage engines:

* MongoDB <https://github.com/whyhankee/dbwrkr-mongodb>


## Changelog

v0.0.1
* Fix devDependency issue

v0.0.0
* Initial commit
