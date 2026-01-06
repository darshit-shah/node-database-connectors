var debug = require('debug')('executor:indentifier');

exports.identify = function() {
  return identifyConnection(arguments[0]);
}

function identifyConnection(json) {
        debug("CLICKHOUSE IDENTIFIED------------------------",json)

  var objConnection = null;
  if (json.type == 'database') {
    if (json.databaseType == "mysql") {
      objConnection = require('./MySQLExecutor.js');
    } else if (json.databaseType == "mssql") {
      objConnection = require('./MSSQLExecutor.js');
    }else if (json.databaseType == "postgres") {
      objConnection = require('./PostgresExecutor.js');
    } else if (json.databaseType == "cassandra") {
      objConnection = require('./CassandraExecutor.js');
    } else if (json.databaseType == "clickhouse") {
      objConnection = require('./ClickhouseExecutor.js');
    } else if (json.databaseType == "redshift") {
      objConnection = require('./RedShiftExecutor.js');
    } else if (json.databaseType == "json") {
      objConnection = require('./JSONExecutor.js');
    } else if (json.databaseType === "influx") {
      objConnection = require('./InfluxExecutor.js');
    } else if (json.databaseType === "bigquery"){ 
      objConnection = require('./BigQueryExecutor.js')
    } else if (json.databaseType === "snowflake"){ 
      objConnection = require('./SnowflakeExecutor.js')
    }
    else if(json.databaseType === "doris"){ 
      objConnection = require('./DorisExecutor.js')
    }
    /*else if (json.databaseType == "mssql") {
        objConnection = require('./MSSQLConnector.js');
    }
    else if (json.databaseType == "elasticsearch") {
        objConnection = require('./ElasticSearchConnector.js');
    }*/
  } else {

  }
  return objConnection;
}
