var { prepareQuery } = require('./MySQLConnector');
var db = require('mysql2');
var debug = require('debug')('node-database-connectors:node-database-connectors');

exports.prepareQuery = (query, dbConfig) => {
  const res = prepareQuery(query);
  return res;
}

exports.connect = (json, cb) => {
  let connectionObject = {
    host: json.host,
    port: json.port,
    user: json.user,
    database: json.database,
    password: json.password,
    multipleStatements: json.connectionLimit === false ? false : true,
    decimalNumbers: json.decimalNumbers === false ? false : true
  }
  if (json.ssl) {
    let extraparamSSL = JSON.parse(json.extraparam);
    if (extraparamSSL.authenticationType == 'user-assigned-managed-identity') {
      connectionObject['ssl'] = { ca: azCAData }
    }
    else {
      connectionObject['ssl'] = { ca: caData }
    }
  }
  if (json.ssl) {
    connectionObject['ssl'] = { ca: caData }
    //Overwrite the key file for specific authentication type
    if (json.extraparam) {
      let extraparamSSL = JSON.parse(json.extraparam);
      if (extraparamSSL.authenticationType == 'user-assigned-managed-identity') {
        connectionObject['ssl'] = { ca: azCAData }
      }
    }
  }
  var connection = db.createConnection(connectionObject);
  // console.log("CONNECTION CREATED...", connection.state, connection.threadId);
  connection.connect(function (err) {
    if (err) {
      debug('error-A');
      debug(['c.connect', err]);
    } else {
      connection.on('error', function (e) {
        debug('error-B');
        debug(['error', e]);
      });
    }
    if (cb)
      cb(err, connection);
  });
  return connection;
}

exports.connectPool = (json, cb) => {
  var numConnections = json.connectionLimit || 0;
  let connectionObject = {
    acquireTimeout: json.acquireTimeout || 2 * 1000,
    connectionLimit: numConnections,
    host: json.host,
    port: json.port,
    user: json.user,
    database: json.database,
    password: json.password,
    multipleStatements: json.connectionLimit === false ? false : true,
    decimalNumbers: json.decimalNumbers === false ? false : true,
    expiresOnTimestamp: json.expiresOnTimestamp
  }
  if (json.ssl) {
    connectionObject['ssl'] = { ca: caData }
    if (json.extraparam) {
      let extraparamSSL = JSON.parse(json.extraparam);
      if (extraparamSSL.authenticationType == 'user-assigned-managed-identity') {
        connectionObject['ssl'] = { ca: azCAData }
      }
    }
  }
  var pool = db.createPool(connectionObject);
  pool.config.expiresOnTimestamp = json.expiresOnTimestamp;

  if (cb)
    cb(null, pool);

  acquireConnection(0);

  function acquireConnection(index) {
    if (index >= numConnections) {
      return;
    }
    debug("acquiring connection ", index, json);
    pool.getConnection(function (err, connection) {
      if (err) {
        debug("error in acquiring connection", index, err, json);
      } else {
        debug("releasing connection ", index, json);
        connection.release();
      }
      acquireConnection(index + 1);
    });
  }
  return pool;
}

exports.disconnect = function () {
  return disconnect(arguments[0]);
}

function disconnect(connection) {
  connection.end();
}