/* eslint no-console: 0 */
const DBWrkrMongoDb = require('dbwrkr-mongodb');
const dbWrkrTests = require('../index').tests;

const testOptions = {
  storage: new DBWrkrMongoDb({
    dbName: 'dbwrkr_tests'
  })
};


dbWrkrTests(testOptions, function (err) {
  if (err) throw err;
});
