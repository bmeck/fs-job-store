//
//TODO AUDIT TMP CLEANUP CHECKS
// WARNING: bmeck paranoia about write consistency
//
const debug = Function.prototype;console.error;
const path = require('path');
const task = require('generator-runner');
const FinallyAggregator = require('finally-aggregator');
const tmp = require('tmp');
const lockfile = require('proper-lockfile');
const _fs = require('fs');

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
  }, oncompromised, _)
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

  /**/
  queue(id, target, fn) {
    debug('starting quue');
    const self = this;
    const jobdir = _jobdir(self.__dirname, id);
    const fs = self.__fs;
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
        const waitdir = _waitdir_raw(self.__dirname);
        debug('making waitdir');
        yield ensure_dir(fs, _waitdir_raw(self.__dirname));
        try {
          yield _ => fs.symlink(_waitdir(self.__dirname, id), jobdir, _);
        }
        catch (e) {
          if (e.code !== 'EEXIST') throw e;
        }
        // do this separate so we cannot get a partial write
        yield _ => fs.writeFile(tmpstatefile, txt, _);
        // don't check parent
        yield _ => fs.rename(tmpstatefile, statefile, _);
      }
      finally {
        yield _ => cleanup.finish(()=>_(null,null));
        // we have already commited at this point
        yield _ => {
          self.flush(id, () => _(null));
        }
        debug('queue finished');
      } 
    }, fn);
  }

  flush(id, fn) {
    const self = this;
    if (self._toflush[id]) {
      debug('already flushing that id')
      self._toflush[id].push(fn);
      return;
    }
    const toflush = self._toflush[id] = [fn];
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
              self.cancel(id, to_remove);
            });
            const errors = [];
            const value = yield _ => {
              fs.readFile(_valuefile(jobdir), _);
            }
            yield state.waiting.map((target) => {
              return _ => {
                self.__respond(target, {
                  fulfilled: state.state === STATES.FULFILLED,
                  value
                }, (err) => {
                  if (!err) {
                    to_remove.push(target);
                  }
                  _(null, null);
                });
              }
            });
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
          release_penddir = yield getlock(fs, penddir, abort);
          debug(1);
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
                fs.rmdir(penddir, done);
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
  pullJob(fn) {
    const self = this;
    const fs = self.__fs;
    task(function* (next, abort) {
      const cleanup = new FinallyAggregator();
      try {
        const potential_job_id = fs.readdir(_penddir_raw(self.__dirname));
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
            let release = yield getlock(fs, penddir, (e) => {
              if (should_release) {
                abort(e);
              }
              else {
                if (typeof ret.oncompromised === 'function') ret.oncompromised();
              }
            });
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
          release
        };
        return ret;
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
          fs.rmdir(_penddir(self.__dirname, id), done);
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
    }, fn);
  }

  // DOES NOT LOCK PENDING
  //   this does not check versioning
  //   this is because we want to allow concurrent work not to completely fail
  fulfill(id, value, fn) {
    this._setValueAndState(id, value, STATES.FULFILLED, fn);
  }

  // DOES NOT LOCK PENDING
  //   this is because we want to allow concurrent work not to completely fail
  _setValueAndState(id, value, new_state, fn) {
    debug('starting setvalue')
    const self = this;
    const jobdir = _jobdir(self.__dirname, id);
    const fs = self.__fs;
    task(function* (next, abort) {
      const cleanup = new FinallyAggregator();
      let should_remove_pending = false;
      cleanup.add((done)=> {
        // TODO should failures propagate?
        if (should_remove_pending) {
          fs.rmdir(_penddir(self.__dirname, id), done);
        }
        else done(null);
      });
      try {
        cleanup.add(yield getlock(fs, jobdir, abort, 'from setvalue ' + new_state));
        // use this to write files so we can get atomic 
        const statefile = _statefile(jobdir);
        const state = JSON.parse(yield _ => fs.readFile(statefile, _));
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
        yield _ => cleanup.finish(()=>_(null, null));
        yield _ => {
          self.flush(id, () => _(null));
        }
        debug('setvalue done');
      } 
    }, fn);
  }
}

module.exports = JobStore; 
