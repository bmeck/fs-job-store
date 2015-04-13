var test = require('tape');
var tmp = require('tmp');
var JobStore = require('../');
test('queued after', function (t) {
  tmp.dir({unsafeCleanup:true}, function (err, tmpdir) {
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
    store.create({
      service: 'bundler'
    }, function (e, v) {
      t.notOk(e, 'should not have errors creating jobs');
      t.ok(v, 'job creation should return job info');
      store.queue(v.id, 'queued-before', function (e) {
        t.notOk(e, 'queueing notifications before job completion should work');
        store.fulfill(v.id, 'no', function (e) {
          t.notOk(e, 'fulfillment should not have errors');
          t.equal(notifications['queued-before'], 'no', 'notifications should flush when resolution occurs');
          store.queue(v.id, 'queued-after', function (e) {
            t.notOk(e, 'queueing notifications after job completion should work');
            t.equal(notifications['queued-after'], 'no', 'notifications after resolution should fire immediately');
            t.end();
          });
        });
      });
    }); 
  });
});
