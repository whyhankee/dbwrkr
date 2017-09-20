## DBWrkr - A general use API for an easy to use pub-sub scheduler.


## What?

* A pub-sub system makes it easy for multiple processes to communicate with each other by sending events. Subscribed processes will receive the events at the appropriate time.

* This way, by creating (and reacting) to events you create loose coupling, little components that are easy to maintain, deploy and remove.

* A storage-plugin system to store your events in your used technology like MongoDB, RethinkDB, Postgres, Redis, etc.

* Although it could work pretty speedy, it's not build for performance, it's for ease of use, flexibility and introspection.


## Status

* Currently this is in an early beta version. I'm using it in a production system and it seems to work pretty stable however, it could contain unplanned side-effects.

[![Build Status](https://travis-ci.org/whyhankee/dbwrkr.svg?branch=master)](https://travis-ci.org/whyhankee/dbwrkr)
[![Coverage Status](https://coveralls.io/repos/github/whyhankee/dbwrkr/badge.svg?branch=master)](https://coveralls.io/github/whyhankee/dbwrkr?branch=master)
[![David](https://david-dm.org/whyhankee/dbwrkr.svg)](https://david-dm.org)
[![Gitter](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/whyhankee/dbwrkr?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)


## Storage engines

Backend | Link | Status
:------- | :------ | :------------------
MongoDB | <https://github.com/whyhankee/dbwrkr-mongodb> | [![Build Status](https://travis-ci.org/whyhankee/dbwrkr-mongodb.svg?branch=master)](https://travis-ci.org/whyhankee/dbwrkr-mongodb)
RethinkDB | <https://github.com/whyhankee/dbwrkr-rethinkdb> | [![Build Status](https://travis-ci.org/whyhankee/dbwrkr-rethinkdb.svg?branch=master)](https://travis-ci.org/whyhankee/dbwrkr-rethinkdb)


## Installation

	npm install dbwrkr --save
	npm install dbwrkr-<storage> --save

### To publish events you need to

* `wrkr.connect()` to connect to the DBWorker storage
* `wrkr.publish()` an event
* `wrkr.disconnect()` when your are done and want to exit

### To process events to you need to:

* `wrkr.connect()` to connect to the DBWorker storage
* Get your self a queue from `wrkr.queue()`
* subscribe to events using `queue.subscribe()`
* use `queue.on('event', fn)` to setup your eventHandler (receives the events)
* use `wrkr.listen()` to start receiving events
* `wrkr.disconnect()` when your are done and want to exit


## API - Setup

* Require the modules (DBWrkr and storage module),
* Create a storage object
* Create the worker with the storage object

```
var wrkr = require('dbwrkr');
var DBWrkrMongodb = require('dbwrkr-mongodb');

var wrkr = new wrkr.DBWrkr({
  storage: new DBWrkrMongodb({
    dbName: 'dbwrkr'
  })
});

wrkr.on('error', (errType, err, event) => {
	// do something with the error
});
```

## Wrkr API

The Wrkr API is the main interface, it provides the main API but is also an EventEmitter.

It will Emit errors using the `error` event



### wrkr.connect(callback)

Connect to the backend storage engine

```
wrkr.connect(callback);
```

### wrkr.disconnect(callback)

Disconnect from the backend storage engine

```
wrkr.disconnect(callback);
```


### wrkr.queue(queueName, callback)

Get a queue object representing a queue in the system.

```
wrkr.queue('queueName`, (err, q) => {
	// q = the Queue interface
});
```


### wrkr.subscriptions(eventName)

Get a list of queues that are subscribed to the event.

```
wrkr.subscriptions(eventName, (err, queues) => {
  // queues is an array with the names of the queues
})
```

### wrkr.publish(events, callback)

Publish new event(s). Events can be a single object or an array of objects.
Events will we created for each queue that is subscribed to the event.

properties:

* `name` - name of the event (required)
* `tid` - target ID of the event (optional)
* `payload` - Object with additional info (optional)
* `when` - `Date()` when the event should be processed (optional, default immediate)


```
var events = [{
  name: 'yourapp.user.signup',
  tid: user.id,
}];

wrkr.publish(events, (err, eventIds) => {
  // eventIds is an array with the ids the created events
})
```

### wrkr.followUp(originalEvent, newEvent, callback)

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

### wrkr.retry(originalEvent, newEvent, callback)

Create a new retry event with the data of the current event also increasing the retryCount on the new event.
You can set the `when` property to specify when the event should be retried, `when` defaults to an algorithm that should slowly increase to about 50 hours in 20 attempts.

When the retryCount becomes >= 20, an error will be returned.

```
wrkr.retry(event, when, (err, eventIds) => {
  // eventIds is an array with the created events
})
```

### wrkr.find(findSpec, callback)

Find unprocessed events in the system.

```
var criteria = {
  name: eventName,
  tid: user.id,
};
wrkr.find(criteria, (err, events) => {
  // events is an array with matched events
})
```

### wrkr.remove(eventSpec, callback)

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


## Queue API

You will receive a queue interface using the `wrkr.queue(queueName)` method.

### Listening for events


Events will be emitted to the Queue interace. You can handle the events by listening for the name of the event. Listening for `*` will receive the event if a specific event-listener was not available.

Listen to specific events
```
queue.on('eventName', function (event, callback) {
	console.log(event) 	// event is the processed event
	return callback(); 	// always call the callback the mark the event as done
})
```


### queue.subscribe(eventName, callback)

Subscribe an event to a queue.
The queue will emit the eventName 

```
queue.subscribe(eventName, callback)
```

### queue.unsubscribe(eventName, callback)

Unsubscribe an event from a queue.
New events will no longer be created for this queue, although previously created qitems could still be received.

```
queue.unsubscribe(eventName, callback)
```



## Testing, developing and debugging

Notes:

* The dbwrk package contains the tests. They are currently called from the storage engine, see the mongodb storage engine for more info.


### Debugging

  DEBUG=wrkr* node ./example/example-mongodb.js


### Event properties

```
id          Unique id of the event                          String  (indexed)
name        Name of the event                               String  (indexed)
queue       Name of the queue                               String  (indexed)
tid         Target id of the event (eg. userId/orderId)     String
payload     Object with extra properties for the event
parent      Id of the parent event (in case of a followUp)  String
created     Date created                                    Date
when        Date when the event should be processed         Date (sparse indexed)
done        Date when the event was processed               Date (sparse indexed)
retryCount  in case of an error followUp, the retryCount    Number
```


## Changelog

v0.0.8
* fix: Invalid ‘error’ emit from __processNext

v0.0.7
* Update dependencies (debug, tape, eslint)
* Map field (id, parent, tid) types to string, gives cleaner cross-db compatibility.
* Fixed benchmarks in tests
* DBWrkr.queue() - fix argument assert
* Tinkering on debug() output

v0.0.6
* dbwrkr.queue() - opt argument is now optional

v0.0.5
* Update documentation
* Use coveralls for coverage testing
* Some API changes, mostly around wrkr.queue() method so we can use more than 1 queue per process
* Rewrite tests using tape

v0.0.4
* Add to Travis-CI
* Use mongodb storage for self-testing
* Support for finding multiple ids in one call

v0.0.3
* Removed some es6 code

v0.0.2
* events are now emitted on the dbwrkr eventEmitter

v0.0.1
* Fix devDependency issue

v0.0.0
* Initial commit
