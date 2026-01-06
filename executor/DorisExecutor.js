var debug = require('debug')('database-executor:mysql-executor');
function executeQuery(connection, rawQuery, cb) {
  if(typeof rawQuery === "string" ){
    if (rawQuery.length <= 100000000) {
      if(connection?.debug !== false){
        debug('query: %s', rawQuery);
      }
    } else {
        if(connection?.debug !== false){
          debug('query: %s', rawQuery.substring(0, 500) + "\n...\n" + rawQuery.substring(rawQuery.length - 500, rawQuery.length));
        }
    }
  } else {
    if (rawQuery.sql && rawQuery.sql.length <= 100000000) {
      if(connection?.debug !== false){
        debug('query: %s', rawQuery.sql);
      }
    } else {
        if(connection?.debug !== false && rawQuery.sql && rawQuery.sql.length){
          debug('query: %s', rawQuery.sql.substring(0, 500) + "\n...\n" + rawQuery.sql.substring(rawQuery.sql.length - 500, rawQuery.sql.length));
        }
    }
  }
  connection.query(rawQuery, function(err, results) {
    if (err) {
      debug("query", err);
      var e = err;
      cb({
        status: false,
        error: e
      });
    } else {
      cb({
        status: true,
        content: results
      });
    }
  });
}

function executeQueryStream(connection, query, onResultFunction, cb){
  var queryExecutor = connection.query(query);
      queryExecutor
        .on('error', function(err) {
          cb({
            status: false,
            error: err
          });
          // Handle error, an 'end' event will be emitted after this as well
        })
        .on('fields', function(fields) {
          // the field packets for the rows to follow
        })
        .on('result', function(row) {
          // Pausing the connnection is useful if your processing involves I/O
          connection.pause();

          onResultFunction(row, function() {
            connection.resume();
          });
        })
        .on('end', function() {
          cb({
            status: true
          });

        });
}

module.exports = {
  executeQuery: executeQuery,
  executeQueryStream: executeQueryStream
}
