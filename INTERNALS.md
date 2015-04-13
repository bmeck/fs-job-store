# Assumptions

* File locking works

# Directories

## $DIR/pending

Used by Workers, this is how you:

* Show that a Job still requires work (by creating a symlink to the Job)
* Show that a Worker is currently working on a Job (by locking)

This is created prior to the Job itself, since pointing to a non-existant Job is fairly easy to detect.
It would be troublesome if to look for Jobs to do we had to look inside of a Job directory/file for the state of the Job.

## $DIR/waiting

Used by Notifiers

* Show that a Job has notification points still unfulfilled

We keep this separate from Jobs so they can easily be found by Notifiers, without needing to look inside of a Job directory or file.
We do not keep the list of values for notification here since we ant to ensure that you cannot perform an update while queueing a value for notification.

## $DIR/jobs

Used mostly by Workers for storing changes to Jobs.

* Show the request for a Job
* Store the results of a Job

