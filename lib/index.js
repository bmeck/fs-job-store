//
//TODO AUDIT TMP CLEANUP CHECKS
// WARNING: bmeck paranoia about write consistency
//
const debug = false ? console.error : Function.prototype; 
const path = require('path');
const task = require('generator-runner');
const FinallyAggregator = require('finally-aggregator');
const tmp = require('tmp');
const lockfile = require('proper-lockfile');
const deepEqual = require('deep-equal');
const _fs = require('fs');
const shuffle = require('knuth-shuffle').knuthShuffle;

const TMP_OPTIONS = {
  unsafeCleanup: true
};

const STATES = {
  RESOLVING: 1,
  FULFILLED: 2
}

function* ensure_dir(fs, dir) {
  try {
    yield _ => fs.mkdir(dir, _);
  }
  catch (e) {
    if (e.code !== 'EEXIST') throw e;
  }
}

function* mkdir_check_parent(fs, dir) {
  try {
    yield _ => fs.mkdir(dir, _);
  }
  catch (e) {
    if (e.code === 'ENOENT') {
      yield ensure_dir(fs, path.dirname(dir));
      yield _ => fs.mkdir(dir, _);
    }
  }
}

function* rename_check_parent(fs, src, dst) {
  try {
    yield _ => fs.rename(src, dst, _);
  }
  catch (e) {
    if (e.code === 'ENOENT') {
      yield ensure_dir(fs, path.dirname(dst));
      yield _ => fs.rename(src, dst, _);
    }
    else {
      throw e;
    }
  }
}

function _jobdir_raw(__dirname) {
  return path.join(__dirname, 'jobs');
}

function _jobdir(__dirname, id) {
  return path.join(__dirname, 'jobs', id);
}

function _waitdir_raw(__dirname) {
  return path.join(__dirname, 'waiting');
}

function _waitdir(__dirname, id) {
  return path.join(__dirname, 'waiting', id);
}

function _penddir_raw(__dirname) {
  return path.join(__dirname, 'pending');
}

function _penddir(__dirname, id) {
  return path.join(__dirname, 'pending', id);
}

function _statefile(__dirname) {
  return path.join(__dirname, 'state.json');
}

// shared for rejection and fulfillment
function _valuefile(__dirname) {
  return path.join(__dirname, 'value');
}

function* getlock(fs, file, oncompromised, msg) {
  debug('locking', file, msg);
  let r = yield _ => lockfile.lock(file, {
    fs,
    retries: 4,
    realpath: false
  }, function (e) {
    oncompromised(e);
  }, _);
  debug('locked', file);
  return (fn) => {
    debug('releasing', file);
    r(fn);
  };
}

class JobStore {
  constructor(options) {
    this.__dirname = options.dir;
    this.__respond = options.respond;
    this.__fs = options.fs || _fs;
    this._toflush = Object.create(null);
  }

  unqueue(id, target, fn) {
    this.unqueueAll(id, [target], fn);
  }

  unqueueAll(id, targets, fn) {
    debug('starting unquue');
    const self = this;
    const jobdir = _jobdir(self.__dirname, id);
    const waitdir = _waitdir(self.__dirname, id);
    const fs = self.__fs;
    task(function* (next, abort) {
      const cleanup = new FinallyAggregator();
      try {
        cleanup.add(yield getlock(fs, jobdir, abort, 'from unqueue'));
        cleanup.add(yield getlock(fs, waitdir, abort, 'from unqueue'));
        const statefile = _statefile(jobdir);
        // use this to write files so we can get atomic
        const state = JSON.parse(yield _ => fs.readFile(statefile, _));
        let removed = 0;
        for (let target of targets) {
          let to_remove = -1;
          for (let i = 0; i < state.waiting.length; i++) {
            let wait = state.waiting[i];
            if (deepEqual(target, wait, {strict:true})) {
              to_remove = i;
              break;
            }
          }
          if (to_remove !== -1) {
            state.waiting.splice(to_remove, 1);
            removed++;
          }
        }
        if (removed === 0) return;
        const tmpdir = yield _ => tmp.dir(TMP_OPTIONS, (err, tmpdir, release) => {
          cleanup.add(done => {
            release();
            done(null);
          });
          _(err, tmpdir);
        }); 
        const tmpstatefile = _statefile(tmpdir);
        const txt = JSON.stringify(state);
        if (state.waiting.length === 0) {
          debug('removing waitdir');
          yield _ => fs.unlink(_waitdir(self.__dirname, id), _)
        }
        // do this separate so we cannot get a partial write
        yield _ => fs.writeFile(tmpstatefile, txt, _);
        // don't check parent
        yield _ => fs.rename(tmpstatefile, statefile, _);
      }
      finally {
        yield _ => cleanup.finish(()=>_(null,null));
        debug('unqueue finished');
      } 
    }, fn || Function.prototype);
  }

  queue(id, target, fn) {
    debug('starting quue');
    const self = this;
    const jobdir = _jobdir(self.__dirname, id);
    const fs = self.__fs;
    let success = false;
    task(function* (next, abort) {
      const cleanup = new FinallyAggregator();
      try {
        cleanup.add(yield getlock(fs, jobdir, abort, 'from queue'));
        const statefile = _statefile(jobdir);
        // use this to write files so we can get atomic
        const state = JSON.parse(yield _ => fs.readFile(statefile, _));
        const tmpdir = yield _ => tmp.dir(TMP_OPTIONS, (err, tmpdir, release) => {
          cleanup.add(done=>{
            release();
            done(null);
          });
          _(err, tmpdir);
        }); 
        const tmpstatefile = _statefile(tmpdir);
        state.waiting.push(target);
        const txt = JSON.stringify(state);
        const waitdir_raw = _waitdir_raw(self.__dirname);
        yield ensure_dir(fs, waitdir_raw);
        try {
          let waitdir = _waitdir(self.__dirname, id);
          yield _ => fs.symlink(jobdir, waitdir, _);
        }
        catch (e) {
          if (e.code !== 'EEXIST') throw e;
        }
        // do this separate so we cannot get a partial write
        yield _ => fs.writeFile(tmpstatefile, txt, _);
        // don't check parent
        yield _ => fs.rename(tmpstatefile, statefile, _);
        success = true;
      }
      finally {
        yield _ => cleanup.finish(()=>_(null,null));
        // we have already commited at this point
        yield _ => {
          self.flush(id, () => _(null));
        }
        debug('queue finished');
      } 
    }, function (err, ret) {
      if (!success) err = err || new Error('unable to queue wait');
      if (fn) fn(err, ret);
    }) 
  }

  flush(id, fn) {
    const self = this;
    if (self._toflush[id]) {
      debug('already flushing that id')
      if (fn) self._toflush[id].push(fn);
      return;
    }
    const toflush = self._toflush[id] = fn ? [fn] : [];
    setImmediate(()=>{
      const jobdir = _jobdir(self.__dirname, id);
      const fs = self.__fs;
      task(function* (next, abort) {
        let to_remove;
        const cleanup = new FinallyAggregator();
        try {
          cleanup.add(yield getlock(fs, jobdir, abort, 'from flush'));
          const statefile = _statefile(jobdir);
          // use this to write files so we can get atomic
          const state = JSON.parse(yield _ => fs.readFile(statefile, _));
          if (state.state === STATES.FULFILLED) {
            to_remove = [];
            cleanup.add(_ => {
              self.unqueue(id, to_remove, _);
            });
            const errors = [];
            const value = yield _ => {
              fs.readFile(_valuefile(jobdir), _);
            }
            // failure to respond is silent
            if (typeof self.__respond === 'function') {
              let respond = self.__respond;
              yield state.waiting.map((target) => {
                return _ => {
                  respond(target, {
                    id,
                    value
                  }, (err) => {
                    if (!err) {
                      to_remove.push(target);
                    }
                    _(null, null);
                  });
                }
              });
            }
            if (errors.length) {
              if (typeof self.onresponsefailure === 'function') {
                self.onresponsefailure({
                  id,
                  errors
                }); 
              }
            }
          }
        }
        finally {
          debug('finishing flush()')
          yield _ => cleanup.finish(()=>_(null, null));
          debug('flush finished')
        } 
      }, (e) => {
        let errs;
        if (this._toflush[id] === toflush) {
          delete this._toflush[id];
        }
        for (let fn of toflush) {
          try {
            fn(e)
          }
          catch (e) {
            errs = errs || [];
            errs.push(e);
          }
        }
        if (errs && errs.length) {
          throw errs.length > 1 ? errs : errs[0];
        }
      });
    });
  }

  create(request, fn) {
    const fs = this.__fs;
    const self = this;
    task(function* (next, abort) {
      const cleanup = new FinallyAggregator();
      try {
        let id;
        let jobdir;
        let penddir;
        // we want a marker if we are not completed, thus
        // penddir should be removed
        // once we are complete we do not want to remove
        // penddir
        let should_remove_pending = false;
        // TODO allow limited tries
        // this will find a job id not in jobdir
        // try to create a penddir
        //   if the penddir exists, it will grab a new id
        while (true) {
          id = path.basename(yield _ => tmp.tmpName({
            prefix: 'job-',
            postfix: '',
            dir: _jobdir_raw(self.__dirname)
          }, _));
          jobdir = _jobdir(self.__dirname, id);
          penddir = _penddir(self.__dirname, id);
          let release_penddir;
          yield ensure_dir(fs, _penddir_raw(self.__dirname));
          release_penddir = yield getlock(fs, penddir, abort, 'from create');
          try {
            yield _ => fs.symlink(jobdir, penddir, _);
          }
          catch (e) {
            debug(2,e);
            if (e.code !== 'EEXIST') throw e;
            // already exists, try again
            else {
              yield _ => release_penddir(_);
              continue;
            }
          }
          should_remove_pending = true;
          cleanup.add((done)=> {
            release_penddir((e) => {
              if (should_remove_pending) {
                fs.unlink(penddir, done);
              }
              else done(null);
            });
          });
          break;
        }
        // use this to write files so we can get atomic 
        const tmpdir = yield _ => tmp.dir(TMP_OPTIONS, (err, tmpdir, release) => {
          cleanup.add((done)=> {
            if (should_remove_pending) release();
            done(null);
          });
          _(err, tmpdir);
        }); 
        const tmpstatefile = _statefile(tmpdir);
        const txt = JSON.stringify({
          version: 0,
          state: STATES.RESOLVING,
          waiting: [],
          request
        });
        // do this separate so we cannot get a partial write
        yield _ => fs.writeFile(tmpstatefile, txt, _);
        yield rename_check_parent(fs, tmpdir, jobdir);
        should_remove_pending = false;
        return {
          id
        };
      }
      finally {
        yield _ => cleanup.finish(() => _(null, null));
      } 
    }, fn);
  }

  /**
   * Grab a job to perform,
   * YOU NEED TO CALL .release if you are finished
   * YOU SHOULD WATCH .oncompromised to avoid duplicate work
   */
  // TODO MAKE THESE UNIFIED
  // CACHE ALL REQUESTS THAT COME IN PRIOR TO readdir
  pullJob(fn) {
    const self = this;
    const fs = self.__fs;
    if (typeof fn !== 'function') {
      throw new Error('callback must be a function');
    }
    task(function* (next, abort) {
      const cleanup = new FinallyAggregator();
      try {
        const potential_job_ids = shuffle(yield _ => fs.readdir(_penddir_raw(self.__dirname), _));
        let id;
        let release;
        let ret;
        // we need a marker for if we are still pulling the job
        // but we get compromised
        let should_release = false;
        for (let potential_job_id of potential_job_ids) {
          if (/\.lock$/.test(potential_job_id)) {
            continue;
          }
          try {
            let penddir = _penddir(self.__dirname, potential_job_id);
            release = yield getlock(fs, penddir, (e) => {
              if (should_release) {
                abort(e);
              }
              else {
                if (typeof ret.oncompromised === 'function') ret.oncompromised();
              }
            }, 'from pullJob');
            try {
              yield _ => fs.stat(penddir, _);
              yield _ => fs.stat(_jobdir(self.__dirname, potential_job_id), _);
            }
            catch (e) {
              continue;
            }
            should_release = true;
            cleanup.add((done) => {
              if (should_release && release) release(done);
              else done();
            })
            id = potential_job_id;
            break;
          }
          catch (e) {
            if (e.code !== 'ELOCKED') throw e; 
          }
        }
        const jobdir = _jobdir(self.__dirname, id);
        const statefile = _statefile(jobdir);
        // use this to write files so we can get atomic 
        const state = JSON.parse(yield _ => fs.readFile(statefile, _));
        should_release = false;
        ret = {
          id,
          version: state.version,
          request: state.request,
          oncompromised: null,
          release: (fn) => {
            release((e)=> {
              console.log('TODO RELEASING JOB', id, e);
              fn(e);
            });
          }
        };
        return ret;
      }
      finally {
        yield _ => cleanup.finish(()=>_(null, null));
      } 
    }, fn);
  }
  /**
   * Grab a notification to perform,
   * YOU NEED TO CALL .release if you are finished
   * YOU SHOULD WATCH .oncompromised to avoid duplicate work
   */
  pullNotification(fn) {
    const self = this;
    const fs = self.__fs;
    if (typeof fn !== 'function') {
      throw new Error('callback must be a function');
    }
    task(function* (next, abort) {
      const cleanup = new FinallyAggregator();
      try {
        const potential_notification_ids = shuffle(yield _ => fs.readdir(_waitdir_raw(self.__dirname), _));
        let id;
        let release;
        let ret;
        // we need a marker for if we are still pulling the notification
        // but we get compromised
        let should_release = false;
        for (let potential_notification_id of potential_notification_ids) {
          if (/\.lock$/.test(potential_notification_id)) {
            continue;
          }
          try {
            let waitdir = _waitdir(self.__dirname, potential_notification_id);
            release = yield getlock(fs, waitdir, (e) => {
              if (should_release) {
                abort(e);
              }
              else {
                if (typeof ret.oncompromised === 'function') ret.oncompromised();
              }
            }, 'from pullNotification');
            should_release = true;
            cleanup.add((done) => {
              if (should_release && release) release(done);
              else done();
            })
            id = potential_notification_id;
          }
          catch (e) {
            if (e.code !== 'ELOCKED') throw e; 
            else continue;
          }
          const jobdir = _jobdir(self.__dirname, id);
          const statefile = _statefile(jobdir);
          // use this to write files so we can get atomic 
          const state = JSON.parse(yield _ => fs.readFile(statefile, _));
          if (state.state !== STATES.FULFILLED) {
            continue;
          }
          const value = yield _ => {
            fs.readFile(_valuefile(jobdir), _);
          }
          should_release = false;
          ret = {
            id,
            waiting: state.waiting,
            value,
            oncompromised: null,
            // ignore errors, YOLO
            done: (err, to_remove, fn) => {
              release((release_err) => {
                self.unqueueAll(id, to_remove, (err) => {
                    fn(err || release_err);
                });
              });
            },
          };
          return ret;
        }
      }
      finally {
        yield _ => cleanup.finish(()=>_(null, null));
      } 
    }, fn);
  }

  update(id, old_version, new_request, fn) {
    const self = this;
    const jobdir = _jobdir(self.__dirname, id);
    const fs = self.__fs;
    task(function* (next, abort) {
      const cleanup = new FinallyAggregator();
      let should_remove_pending = false;
      cleanup.add((done)=> {
        // TODO should failures propagate?
        if (should_remove_pending) {
          fs.unlink(_penddir(self.__dirname, id), done);
        }
        else done(null);
      });
      try {
        cleanup.add(yield getlock(fs, jobdir, abort, 'from update'));
        const statefile = _statefile(jobdir);
        // use this to write files so we can get atomic fs.rename
        const state = JSON.parse(yield _ => fs.readFile(statefile, _));
        if (state.state !== STATES.RESOLVING) {
          should_remove_pending = true;
          throw new Error('already resolved');
        }
        if (state.version !== old_version) {
          throw new Error('version mismatch');
        }
        state.version++;
        state.request = new_request;
        const tmpdir = yield _ => tmp.dir(TMP_OPTIONS, (err, tmpdir, release) => {
          cleanup.add(done=> {
            release();
            done(null);
          });
          _(err, tmpdir);
        }); 
        const tmpstatefile = _statefile(tmpdir);
        const txt = JSON.stringify(state);
        // do this separate so we cannot get a partial write
        yield _ => fs.writeFile(tmpstatefile, txt, _);
        yield _ => fs.rename(tmpstatefile, statefile, _);
      }
      finally {
        yield _ => cleanup.finish(()=>_(null, null));
      } 
    }, fn || Function.prototype);
  }

  // DOES NOT LOCK PENDING
  //   this does not check versioning
  //   this is because we want to allow concurrent work not to completely fail
  fulfill(id, value, fn) {
    return this._setValueAndState(id, value, STATES.FULFILLED, fn);
  }

  // DOES NOT LOCK PENDING
  //   this is because we want to allow concurrent work not to completely fail
  _setValueAndState(id, value, new_state, fn) {
    debug('starting setvalue')
    const self = this;
    const jobdir = _jobdir(self.__dirname, id);
    const penddir = _penddir(self.__dirname, id);
    const fs = self.__fs;
    let state;
    task(function* (next, abort) {
      const cleanup = new FinallyAggregator();
      let should_remove_pending = false;
      cleanup.add((done)=> {
        // TODO should failures propagate?
        if (should_remove_pending) {
          fs.unlink(penddir, (e) => {
            done(e);
          });
        }
        else done(null);
      });
      try {
        debug('starting setValue');
        cleanup.add(yield getlock(fs, jobdir, abort, 'from setvalue ' + new_state));
        // use this to write files so we can get atomic 
        const statefile = _statefile(jobdir, id);
        state = JSON.parse(yield _ => fs.readFile(statefile, _));
        if (state.state !== STATES.RESOLVING) {
          should_remove_pending = true;
          throw new Error('already resolved');
        }
        state.state = new_state;
        const tmpdir = yield _ => tmp.dir(TMP_OPTIONS, (err, tmpdir, release) => {
          cleanup.add(function (done) {
            release();
            done(null);
          });
          _(err, tmpdir);
        }); 
        const valuefile = _valuefile(jobdir);
        const tmpstatefile = _statefile(tmpdir);
        const tmpvaluefile = _valuefile(tmpdir);
        const txt = JSON.stringify(state);
        // do this separate so we cannot get a partial write
        yield _ => fs.writeFile(tmpvaluefile, value, _);
        yield _ => fs.writeFile(tmpstatefile, txt, _);
        yield _ => fs.rename(tmpvaluefile, valuefile, _);
        yield _ => fs.rename(tmpstatefile, statefile, _);
        should_remove_pending = true;

        // we have already commited at this point
      }
      finally {
        console.log('TODO, REMOVPEND', id, should_remove_pending);
        yield _ => cleanup.finish(() => _(null, null));
        if (state && state.waiting && state.waiting.length) {
          yield _ => {
            self.flush(id, () => {
              _(null);
            });
          }
        }
        debug('setvalue done');
      } 
    }, fn || Function.prototype);
  }
}

module.exports = JobStore; 
