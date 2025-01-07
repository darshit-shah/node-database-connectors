﻿var debug = require('debug')('connector:indentifier');
exports.identify = function() {
  return identifyConnection(arguments[0]);
}

function identifyConnection(json) {
  var objConnection = null;
  if (json.type == 'database') {
    if (json.databaseType == "mysql") {
      objConnection = require('./MySQLConnector.js');
    } else if (json.databaseType == "mssql") {
      objConnection = require('./MSSQLConnector.js');
    } else if (json.databaseType == "postgres") {
      objConnection = require('./PostgresConnector.js');
    }
    /*else if (json.databaseType == "mssql") {
        objConnection = require('./MSSQLConnector.js');
    }
    else if (json.databaseType == "elasticsearch") {
        objConnection = require('./ElasticSearchConnector.js');
    }*/
    else if (json.databaseType == "cassandra") {
      objConnection = require('./CassandraConnector.js');
    } else if (json.databaseType == "clickhouse") {
      objConnection = require('./ClickhouseConnector.js');
    }  else if (json.databaseType == "redshift") {
      objConnection = require('./RedShiftConnector.js');
    }else if (json.databaseType == "json") {
      objConnection = require('./JSONConnector.js');
    } else if (json.databaseType === "influx") {
      objConnection = require('./InfluxConnector.js')
    } else if (json.databaseType === "bigquery"){ 
      objConnection = require('./BigQueryConnector.js')
    } else if (json.databaseType === "snowflake"){ 
      objConnection = require('./SnowflakeConnector.js')
    }
  } else {

  }
  return objConnection;
}
