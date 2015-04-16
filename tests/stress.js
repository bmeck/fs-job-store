var test = require('tape');
var tmp = require('tmp');
var JOB_COUNT = 20;
var JobStore = require('../');
test('stress', function (t) {
  tmp.dir({unsafeCleanup:true}, function (err, tmpdir) {
    console.log('TODO', tmpdir);
    t.notOk(err, 'should grab a tmpdir');
    var notifications = {};
    var store = new JobStore({
      dir: tmpdir,
      respond: function (target, v, done) {
        notifications[target] = String(v.value);
        done(null);
      }
    });
    t.ok(store, 'should create a JobStore');
    var todo = JOB_COUNT;
    function tick(n, e) {
      todo--;
      console.log('TODO', todo, n, e);
      if (todo === 0) {
        t.end();
      }
    }
    for (var i = 0; i < JOB_COUNT; i++) {
      store.create({
        service: 'bundler'
      }, function (e, v) {
        t.notOk(e, 'should not have errors creating jobs');
        t.ok(v, 'job creation should return job info');
        if (e) {
          tick('create', e);
          return;
        }
        store.queue(v.id, 'queued-before', function (e) {
          if (e) {
            tick('queue', e);
            return;
          }
          t.notOk(e, 'queueing notifications before job completion should work');
          store.pullJob(function (e, v) {
            console.log('TODO pulled', v && v.id);
            t.notOk(e);
            if (e) {
              tick('pull', e);
              return;
            }
            store.fulfill(v.id, 'no', function (e) {
              if (e) {
                tick('fulfill:'+v.id, e);
                v.release(Function.prototype);
                return;
              }
              t.notOk(e, 'fulfillment should not have errors');
              t.equal(notifications['queued-before'], 'no', 'notifications should flush when resolution occurs');
              v.release(function (e) {
                console.log('TODO RELEASE()', e);
                t.notOk(e, 'releasing a job should not cause errors');
                store.queue(v.id, 'queued-after', function (e) {
                  t.notOk(e, 'queueing notifications after job completion should work');
                  t.equal(notifications['queued-after'], 'no', 'notifications after resolution should fire immediately');
                  tick('done', e);
                });
              });
            });
          });
        });
      }); 
    }
  });
});
