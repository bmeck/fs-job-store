
```javascript
var JobStore = require('job-store');
var store = new JobStore({
  dir: tmpdir,
  respond: function (target, v, done) {
    console.log('Eureka! the answer is', v.value);
    done(null);
  }
});
store.create({
  calculate: '3+3'
}, function (e, v) {
  store.queue(v.id, 'queued-before', function (e) {
    store.fulfill(v.id, 6, function (e) {
    });
  });
}); 
```
