exports.identify = function () {
    return identifyConnection(arguments[0]);
}

function identifyConnection(json) {
    var objConnection = null;
    if (json.type == 'database') {
        if (json.databaseType == "mysql") {
            objConnection = require('./MySQLConnector.js');
        }
        else if (json.databaseType == "mssql") {
            objConnection = require('./MSSQLConnector.js');
        }
        /*else if (json.databaseType == "mssql") {
            objConnection = require('./MSSQLConnector.js');
        }
        else if (json.databaseType == "elasticsearch") {
            objConnection = require('./ElasticSearchConnector.js');
        }*/
        else if (json.databaseType == "cassandra") {
            objConnection = require('./CassandraConnector.js');
        }
    }
    else {

    }
    return objConnection;
}
