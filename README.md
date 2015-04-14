
```javascript
var JobStore = require('fs-job-store');
var store = new JobStore({
  dir: tmpdir
});
store.create({
  calculate: '3+3'
}, function (e, v) {
  // ...
}); 
```

## General workflow

The job store create Jobs which transition from a couple of states:

In terms of resolution (being finished):

```
PENDING - job has been queued and workers can perform work on it
FULFILLED - job has had a worker (or multiple) resolve it to a value
```

You should think about job stores as having 3 workflows.

* job creation
* job fulfillment
* fulfillment notification

Generally for recovery of unfinished jobs / unfinished notifications we want to take care of each separately.

### job createion

```javascript
//create job store
var store = new JobStore({dir:$dir});

//accept multiple jobs
function addJob(json_value_representing_request_for_work) {
  store.create(json_value, function created(creation_err, job_description) {
    // this does not lock our job, use pullJob
  });
}
```

### job fulfillment

```javascript
//create job store
var store = new JobStore({dir:$dir});

function getAJob() {
  store.pullJob(function (pull_error, job) {
    // if there is a problem, retry in a couple seconds
    if (pull_error) {
      console.log(pull_error);
      setTimeout(getAJob, 1000 * 10);
      return;
    }
    // if job === undefined we did not get a job
    if (job === undefined) {
      setTimeout(getAJob, 1000 * 10);
      return;
    }
    // otherwise we have a job to do

    // we are using locks to assure that we are the exclusive worker for a job
    // this is *optional* but *recommended*
    job.oncompromised = function () {
      // stop work
    };
    
    // do work ...
    // make your own work function
    var result_string_or_buffer = work(job.value);

    // values for results are stored as Buffers, so you should pass in a string or buffer
    store.fulfill(job.id, result_string_or_buffer, function (fulfillment_error) {
i     if (fulfillment_error) console.log(fulfillment_error);
      // release our lock on trying to do the job
      // we should do this regardless if we get a fulfillment error
      job.release();
      // queue up another notification attempt
      setTimeout(getAJob, 1000 * 10);
    });
  });
}
getAJob();
```

### fulfillment notification

```javascript
//create job store
var store = new JobStore({dir:$dir});

function getANotification() {
  store.pullNotification(function (pull_error, notification) {
    // if there is a problem, retry in a couple seconds
    if (pull_error) {
      console.log(pull_error);
      setTimeout(getANotification, 1000 * 10);
      return;
    }
    // if job === undefined we did not get a job
    if (job === undefined) {
      setTimeout(getANotification, 1000 * 10);
      return;
    }
    // otherwise we have notifications to do

    // we are using locks to assure that we are the exclusive worker for a notification
    // this is *optional* but *recommended*
    notification.oncompromised = function () {
      // stop working
    };
    
    // notify things
    var to_remove = [];
    for (var i = 0; i < notification.waiting.length; i++) {
      var target = notification.waiting[i];
      // make your own notification fn
      var success = notify(target);
      if (success) {
        to_remove.push(target);
      }
    }

    // tell the store to remove any notifications that were successful (to_remove)
    // stores will ignore error arguments generally
    notification.done(null, to_remove, function (unqueue_error) {
      if (unqueue_error) console.log(unqueue_error);
      // queue up another notification attempt
      setTimeout(getANotification, 1000 * 10);
    });
  });
}
getANotification();
```

## JobStore = require('fs-job-store');

A constructor 

### options

#### dir

Place to put all the work we are going to store.
This can be shared amongst job stores of the same type.

## store.create(request, callback)

Creates a Job using the JSON representation of `request`.

### callback(err, job)

Used to confirm the request was fully commited.
If there are enough locks contending for job creation you might need to retry, this is unlikely though.

## store.fulfill(id, value, callback)

Given a Job's id it will fulfill the job with the Buffer representation of `value`.
This will attempt to flush any pending notification before completing.

### callback(err)

Used to confirm a job is in the `fulfilled` state.

## store.queue(id, target, callback)

Used to inform the store that a target needs to be notified of the value of the Job with `id`.
This is asynchronous to the callback and may not occur before the callback is invoked.

### callback

Used to confirm that a Job has had a notification appended to its waiting queue.
This does not mean that the notification was successful.

## store.unqueue(id, target, callback)

Used to inform the store that a target no longer needs to be notified of a Job's value.

### callback

Used to confirm that a notification has been removed from a Job's waiting queue.
This does not mean that no workers are attempting to fulfill the notification already.

## store.unqueueAll(id, target[], callback)

A batch form of `store.unqueue`

## store.pullNotification(callback)

Used to pull some request for notification off of the store.
You MUST use `.done` when you are finished unless you will leak both locks and memory.

### callback(err, notification)

Used to obtain an exclusive lock for a notification to perform.
These notifications may have multiple target destinations (see `notification.waiting`).

#### notification.id

The Job id the notificaiton is associated with.

#### notification.oncompromised

This will be called if the notification loses its exclusive lock.
You can use this to stop performing work so that fewer duplicate notifications are possible.

#### notification.value

The Job's fulfillment value. This is a Buffer.

#### notification.waiting

The list of targets provided to the `store.queue` method, these can be any JSON value.

#### notification.done(err, target[], callback)

Used to notify the store that you are done attempting to perform notifications.
It will release the notification lock and unqueue the `target[]`.
You MUST call `job.release` if you do not want to leak locks and memory.

##### callback(err)

Used to notify that the store has successfully removed the lock and unqueued the targets.

## store.pullWork(callback)

Use to pull an exclusive lock to perform work for this Job Store.

### callback(err, job)

#### job.id

Id of the Job we are working on, use this so you can call `store.fulfill(job.id, value, ...)`

#### job.request

JSON value representing the work that we need to perform.

#### job.release(callback)

Releases the lock on this Job.
Call this *AFTER* you are done working and fulfilling.
Call this even in the case of an error.

#### job.oncompromised

This will be called if the Job loses its exclusive lock.
You can use this to stop performing work so that fewer duplicate Job fulfillments are possible.

##### callback(err)

Used to notify that a job has successfully been released.

