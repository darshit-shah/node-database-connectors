const debug = require('debug')('node-database-connectors:node-database-connectors');
const { Pool, Client } = require('pg');
const utils = require("./utils.js");
var fieldIdentifier_left = '"',
  fieldIdentifier_right = '"';
// Connect
exports.connectPool = async function (json, cb) {
  return connectPool(json, cb);
}

exports.connect = async function (json, cb) {
  return connect(json, cb);
}

function connectPool(json, cb) {
  var numConnections = json.connectionLimit || 0;
  let poolConfig = {
    host: json.host,
    port: json.port,
    user: json.user,
    database: json.database,
    password: json.password,
    ssl: json.ssl,
    idleTimeoutMillis: 30000
  };

  const pool = new Pool(poolConfig);
  pool.expiresOnTimestamp = json.expiresOnTimestamp;

  if (cb) cb(null, pool);

  acquireConnection(0);

  function acquireConnection(index) {
    if (index >= numConnections) {
      return;
    }
    debug("Acquiring connection ", index, json);
    pool.connect((err, client, release) => {
      if (err) {
        debug("Error in acquiring connection", index, err, json);
      } else {
        debug("Releasing connection ", index, json);
        release();
      }
      acquireConnection(index + 1);
    });
  }
  return pool;
}

function connect(json, cb) {
  let connectionConfig = {
    host: json.host,
    port: json.port,
    user: json.user,
    database: json.database,
    password: json.password,
    ssl: json.ssl || false
  };

  const client = new Client(connectionConfig);
  console.log("CONNECTION CREATED...", client._connected);
  client.connect((err) => {
    if (err) {
      console.error('Connection error:', err.stack);
      if(cb) cb('Could not connect')
    }
    console.log('Connected to the PostgreSQL database successfully');
    if(cb) cb(err,client)
  })
}

// Disconnect
exports.disconnect = function () {
  return disconnect(arguments[0]);
}

function disconnect(connection) {
  connection.end();
}

// Prepare query
exports.prepareQuery = function () {
  return prepareQuery(arguments[0]);
}

function createInsertQuery(json) {
  const table = json.table ? json.table : null;
  const vInsert = json.insert && 
  typeof json.insert === 'object' && 
  (Array.isArray(json.insert) ? json.insert.length > 0 : Object.keys(json.insert).length > 0) 
    ? json.insert 
    : null;
  let arrInsert = [];
  arrInsert = createInsert(vInsert); 
  let query = '';
  if(!vInsert || !arrInsert){
    return query;
  }
  if (!Array.isArray(vInsert)) {
    if (!Array.isArray(vInsert.fValue[0])) {
      query = `INSERT INTO ${table} (${arrInsert.fieldArr.join(', ')}) VALUES (${arrInsert.valueArr.join(', ')})`;
    } else {
      const valueRows = arrInsert.valueArr.join('');
      query = `INSERT INTO ${table} (${arrInsert.fieldArr.join(', ')}) VALUES ${valueRows}`;
    }
  } else {
    //single row insert
    query = `INSERT INTO ${table} (${arrInsert.fieldArr.join(', ')}) VALUES (${arrInsert.valueArr.join(', ')})`;
  }

  return query + ';';
}


function createUpdateQuery(json) {
  var arrUpdate = [],
    arrFilter = [],
    strJOIN = '';
  var table = json.table ? json.table : null;
  var vUpdate = json.update ? json.update : null,
    vFilter = json.filter ? json.filter : null,
    join = json.join ? json.join : null;

  if (vFilter != null)
    arrFilter = createFilter(vFilter);
  if (join != null) {
    strJOIN = createJOIN(join);
    if (strJOIN.length > 0) {
      table = strJOIN;
    } else {
      table = encloseField(table) + (fromTblAlias ? (' as ' + fromTblAlias) : '');
    }
  }

  arrUpdate = createUpdate(vUpdate);
  query = 'UPDATE ' + table + ' SET ' + arrUpdate.join() + '';
  if (arrFilter.length > 0) {
    query += ' WHERE ' + arrFilter.join('');
  }
  return query + ';';
}

function createSelectQuery(json, selectAll) {
  var arrSelect = [],
    arrSortBy = [],
    arrFilter = [],
    arrGroupBy = [],
    arrHaving = [],
    strJOIN = '';
  var table = json.table ? json.table : null;
  var fromTblAlias = json.alias ? json.alias : json.table;
  var sortby = json.sortby ? json.sortby : null,
    limit = json.limit ? json.limit : null,
    join = json.join ? json.join : null;
  var vSelect = json.select ? json.select : null,
    vFilter = json.filter ? json.filter : null,
    vGroupby = json.groupby ? json.groupby : null,
    vHaving = json.having ? json.having : null;

  if (vHaving != null)
    arrHaving = createAggregationFilter(vHaving);
  if (vGroupby != null)
    arrGroupBy = createSelect(vGroupby, false);
  if (vFilter != null)
    arrFilter = createFilter(vFilter);

  arrSelect = createSelect(vSelect, true);

  //from/join
  strJOIN = createJOIN(join);
  if (strJOIN.length > 0) {
    table = strJOIN;
  } else {
    table = encloseField(table) + (fromTblAlias ? (' as ' + fromTblAlias) : '');
  }

  //order by
  if (sortby != null) {
    for (var s = 0; s < sortby.length; s++) {
      var encloseFieldFlag = (sortby[s].encloseField != undefined) ? sortby[s].encloseField : true;
      var sortField = encloseField(sortby[s].field, encloseFieldFlag);
      var sortTable = sortby[s].table != undefined ? encloseField(sortby[s].table) : null;
      var sortOrder = sortby[s].order ? sortby[s].order : 'ASC';
      if (sortTable == null)
        arrSortBy.push(sortField + ' ' + sortOrder);
      else
        arrSortBy.push(sortTable + '.' + sortField + ' ' + sortOrder);
    }
  }

  var query = 'SELECT ' + arrSelect.join();
  if (table != '') {
    query += ' FROM ' + table + '';
  }
  if (arrFilter.length > 0) {
    query += ' WHERE ' + arrFilter.join('');
  }
  if (arrGroupBy.length > 0) {
    query += ' GROUP BY ' + arrGroupBy.join();
  }
  if (arrHaving.length > 0) {
    query += ' HAVING ' + arrHaving.join('');
  }
  if (arrSortBy.length > 0) {
    query += ' ORDER BY ' + arrSortBy.join();
  }
  if (limit != null) {
    query += ' LIMIT ' + limit;
  }
  return query + ';';
}

function createDeleteQuery(json) {
  var table = json.table ? json.table : null;
  var arrFilter = [];
  var vFilter = json.filter ? json.filter : null;;
  if (vFilter != null) {
    arrFilter = createFilter(vFilter);
  }
  var query = '';
  if (arrFilter.length > 0) {
    query = 'DELETE FROM ' + table + ' WHERE' + arrFilter.join('');
  } else {
    query = 'DELETE FROM ' + table + ' WHERE 1=1';
  }
  return query + ';';
}

function validateJson(json) {
  return utils.validateJson(json);
}

function prepareQuery(json) {
  var validate = validateJson(json);

  if (validate !== '') {
    return validate;
  } else {
    var query = '';
    vInsert = json.insert ? json.insert : null,
      vSelect = json.select ? json.select : null,
      vUpdate = json.update ? json.update : null,
      vDelete = json.delete ? json.delete : null;

    //INSERT
    if (vInsert != null) {
      return createInsertQuery(json);
    }
    //UPDATE
    else if (vUpdate != null) {
      return createUpdateQuery(json);
    }
    //DELETE
    else if (vDelete != null) {
      return createDeleteQuery(json);
    }
    //SELECT
    else if (vSelect != null) {
      return createSelectQuery(json);
    }
  }
}

//Create select expression
function createSelect(arr, selectAll) {
  var tempArr = [];
  if (arr != null) {
    if (arr.length == 0 && selectAll == true) {
      tempArr.push('*');
    } else {
      for (var s = 0; s < arr.length; s++) {
        var obj = arr[s];
        if (typeof obj === 'string') {
          obj = { field: obj };
        }
        if (obj.encloseField != undefined && typeof obj.encloseField != "boolean") {
          obj.encloseField = obj.encloseField == "false" ? false : true;
        }
        var encloseFieldFlag = (obj.encloseField != undefined) ? obj.encloseField : true;
        var field = encloseField(obj.field, encloseFieldFlag);
        var table = encloseField((obj.table ? obj.table : ''));
        var hasAlias = (obj.alias ? true : false);
        var alias = encloseField((obj.alias ? obj.alias : obj.field));
        var expression = obj.expression ? obj.expression : null;
        var aggregation = obj.aggregation ? obj.aggregation : null;
        var dataType = obj.dataType ? obj.dataType : null;
        var format = obj.format ? obj.format : null;
        var selectText = '';
        if (expression != null) {
          selectText = '(CASE ';
          var cases = expression.cases;
          var defaultCase = expression['default'];
          var defaultValue = '';
          for (var e = 0; e < cases.length; e++) {
            var operator = cases[e].operator;
            var value = cases[e].value;
            var out = cases[e].out;
            var outVal = '';
            if (out.hasOwnProperty('value')) {
              outVal = out.value;
            } else {
              outVal = encloseField(out.table) + '.' + encloseField(out.field);
            }
            var strOperatorSign = '';
            strOperatorSign = operatorSign(operator, value);
            if (strOperatorSign.indexOf('IN') > -1) { //IN condition has different format
              selectText += ' WHEN ' + table + '.' + field + ' ' + strOperatorSign + ' ("' + value.join('","') + '") THEN ' + outVal;
            } else {
              selectText += ' WHEN ' + table + '.' + field + ' ' + strOperatorSign + ' "' + value + '" THEN ' + outVal;
            }
          }
          if (defaultCase.hasOwnProperty('value')) {
            defaultValue = defaultCase.value;
          } else {
            defaultValue = encloseField(defaultCase.table) + '.' + encloseField(defaultCase.field);
          }
          selectText += ' ELSE ' + defaultValue + ' END)';
        } else {
          if (dataType != null) {
            if (dataType.toString().toLowerCase() == 'datetime') {
              selectText = ' DATE_FORMAT(' + table + '.' + field + ',\'' + format + '\') ';
            } else {
              if (encloseFieldFlag == false || encloseFieldFlag == 'false')
                selectText = field;
              else
                selectText = table + '.' + field;
            }
          } else {
            if (encloseFieldFlag == false || encloseFieldFlag == 'false') {
              selectText = field;
            } else {
              selectText = table + '.' + field;
            }
          }
        }

        if (aggregation != null) {
          //CBT:this is for nested aggregation if aggregation key contains Array
          if (Object.prototype.toString.call(aggregation).toLowerCase() === "[object array]") {
            var aggregationText = "";
            aggregation.forEach(function (d) {
              aggregationText = aggregationText + d + "("
            });
            selectText = aggregationText + selectText;
            aggregationText = "";
            aggregation.forEach(function (d) {
              aggregationText = aggregationText + ")"
            });
            selectText = selectText + aggregationText;

          } else {
            selectText = aggregation + '(' + selectText + ')';

          }
        }
        if (hasAlias) selectText += ' as ' + alias;
        tempArr.push(selectText);
        selectText = null;
      };
    }
  }
  return tempArr;
}

function createInsert(arr) {
  var tempJson = {
    fieldArr: [],
    valueArr: []
  };
  if (arr == null) {
    console.error('createInsert -> blank arr found');
  } else {
    if (!Array.isArray(arr)) {
      if(arr.field && Array.isArray(arr.field) && arr.field.length>0 && arr.fValue &&Array.isArray(arr.fValue) && arr.fValue.length>0){
      for (var y = 0; y < arr.field.length; y++) {
        var obj = arr.field[y];
        var field = encloseField(obj, encloseFieldFlag)
        tempJson.fieldArr.push(field);
      }
      for (var z = 0; z < arr.fValue.length; z++) {
        var obj = arr.fValue[z];
        if (Array.isArray(obj)) {
          subValueArr = [];
          for (var k = 0; k < obj.length; k++) {
            var objSub = obj[k]
            var fValue = objSub
            fValue = (replaceSingleQuote(fValue));
            if (fValue != null) {
              subValueArr.push('\'' + fValue + '\'');
            } else {
              subValueArr.push("null");
            }
          }
          if (tempJson.valueArr.length == 0) {
            tempJson.valueArr.push('(' + subValueArr.join() + ')');
          } else {
            tempJson.valueArr.push(', (' + subValueArr.join() + ')');
          }
        } else {
          var fValue = (replaceSingleQuote(obj));
          tempJson.valueArr.push('\'' + fValue + '\'');
        }
      }}else{
        return tempJson;
      }
      
    } else {
      for (var s = 0; s < arr.length; s++) {
        var obj = arr[s];
        if (obj.encloseField != undefined && typeof obj.encloseField != "boolean") {
          obj.encloseField = obj.encloseField == "false" ? false : true;
        }
        var encloseFieldFlag = (obj.encloseField != undefined) ? obj.encloseField : true;
        var field = encloseField(obj.field, encloseFieldFlag)
        var table = encloseField(obj.table ? obj.table : '');
        var fValue = obj.fValue;// ? obj.fValue : '';
        fValue = (fValue == null ? fValue : replaceSingleQuote(fValue));
        tempJson.fieldArr.push(field);
        if (fValue != null) {
          tempJson.valueArr.push('\'' + fValue + '\'');
        } else {
          tempJson.valueArr.push("null");
        }
      }
    }
    return tempJson;
  }
}

function replaceSingleQuote(aValue) {
  if (aValue != undefined && typeof aValue === 'string') {
    aValue = aValue.replace(/\'/ig, "\\\'");
    return aValue;
  } else {
    return aValue;
  }
}

function createUpdate(arr) {
  var tempArr = [];
  if (arr != null) {
    for (var s = 0; s < arr.length; s++) {
      var obj = arr[s];
      if (obj.encloseField != undefined && typeof obj.encloseField != "boolean") {
        obj.encloseField = obj.encloseField == "false" ? false : true;
      }
      var encloseFieldFlag = (obj.encloseField != undefined) ? obj.encloseField : true;
      var field = encloseField(obj.field, encloseFieldFlag)
      var fValue = obj.fValue;// ? obj.fValue : '';
      fValue = (fValue == null ? fValue : replaceSingleQuote(fValue));
      var selectText = '';
      if (fValue != null) {
        selectText = (obj.table?(encloseField(obj.table) + '.'):'') + field + '=' + '\'' + fValue + '\'';
      } else {
        if (encloseFieldFlag == true) {
          selectText = (obj.table?(encloseField(obj.table) + '.'):'') + field + '=null';
        } else {
          selectText = field;
        }
      }
      tempArr.push(selectText);
    }
    return tempArr;
  }
}
//Create select expression
function createAggregationFilter(obj) {
  var tempHaving = [];
  if (obj != null) {
    tempHaving = createFilter(obj);
  }
  return tempHaving;
}

exports.createFilter = function (arr) {
  return createFilter(arr);
}

//Create filter conditions set
function createFilter(arr) {
  var tempArrFilter = [];
  if (arr != null) {
    if (arr.hasOwnProperty('and') || arr.hasOwnProperty('AND') || arr.hasOwnProperty('or') || arr.hasOwnProperty('OR')) { //multiple conditions
      tempArrFilter = createMultipleConditions(arr);
    } else { //single condition
      var conditiontext = createSingleCondition(arr);
      tempArrFilter.push(conditiontext);
    }
  }
  return tempArrFilter;
}

function createMultipleConditions(obj) {
  var tempArrFilters = [];
  var conditionType = Object.keys(obj)[0]; //AND/OR/NONE
  var listOfConditions = obj[conditionType]; //all conditions
  if (conditionType.toString().toLowerCase() != 'none') {
    for (var c = 0; c < listOfConditions.length; c++) {
      var tempConditionType = Object.keys(listOfConditions[c])[0];
      //console.log('*************' + tempConditionType + '*******************');
      if (tempConditionType.toString().toLowerCase() == 'and' || tempConditionType.toString().toLowerCase() == 'or') {
        tempArrFilters.push(createMultipleConditions(listOfConditions[c]));
      } else if (tempConditionType.toString().toLowerCase() == 'none') {
        var conditiontext = createSingleCondition(listOfConditions[c].none);
        tempArrFilters.push(conditiontext);
      } else {
        var conditiontext = createSingleCondition(listOfConditions[c]);
        tempArrFilters.push(conditiontext);
      }
    }
  } else { //single condition
    if (listOfConditions.length > 0) {
      var conditiontext = createSingleCondition(listOfConditions.none);
      tempArrFilters.push(conditiontext);
    }
  }
  var tempConditionSet = '(' + tempArrFilters.join(' ' + conditionType + ' ') + ')';
  tempArrFilters = [];
  tempArrFilters.push(tempConditionSet);
  return tempArrFilters;
}

function encloseField(a, flag) {
  if (flag == undefined || (flag != undefined && flag == true))
    return fieldIdentifier_left + a + fieldIdentifier_right;
  else
    return a;
}

function operatorSign(operator, value) {
  var sign = '';
  if (operator.toString().toLowerCase() == 'eq') {
    if (Object.prototype.toString.call(value) === '[object Array]') {
      sign = 'IN';
    } else if (typeof value === 'undefined' || value == null) {
      sign = 'IS';
    } else if (typeof value == 'string') {
      sign = '=';
    } else {
      sign = '=';
    }
  } else if (operator.toString().toLowerCase() == 'noteq') {
    if (Object.prototype.toString.call(value) === '[object Array]') {
      sign = 'NOT IN';
    } else if (typeof value === 'undefined' || value == null) {
      sign = 'IS NOT';
    } else if (typeof value == 'string') {
      sign = '<>';
    } else {
      sign = '<>';
    }
  } else if (operator.toString().toLowerCase() == 'match') {
    sign = 'LIKE';
  } else if (operator.toString().toLowerCase() == 'notmatch') {
    sign = 'NOT LIKE';
  } else if (operator.toString().toLowerCase() == 'gt') {
    sign = '>';
  } else if (operator.toString().toLowerCase() == 'lt') {
    sign = '<';
  } else if (operator.toString().toLowerCase() == 'gteq') {
    sign = '>=';
  } else if (operator.toString().toLowerCase() == 'lteq') {
    sign = '<=';
  } else {
    throw Error("Unknow operator '%s'", operator);
  }
  return sign;
}

function createSingleCondition(obj) {
  var field = obj.field,
    table = obj.table ? obj.table : '',
    aggregation = obj.aggregation ? obj.aggregation : null,
    operator = obj.operator,
    value = obj.value,
    encloseFieldFlag = obj.encloseField;
  if (encloseFieldFlag != undefined && typeof encloseFieldFlag != "boolean") {
    encloseFieldFlag = encloseFieldFlag == "false" ? false : true;
  }
  var conditiontext = '';
  if (aggregation != null) {
    if (encloseFieldFlag == false) {
      //CBT:this is for nested aggregation if aggregation key contains Array
      if (Object.prototype.toString.call(aggregation).toLowerCase() === "[object array]") {
        var aggregationText = "";
        aggregation.forEach(function (d) {
          aggregationText = aggregationText + d + "("
        });
        conditiontext = aggregationText + field;
        aggregationText = "";
        aggregation.forEach(function (d) {
          aggregationText = aggregationText + ")"
        });
        conditiontext = conditiontext + aggregationText;

      } else {
        conditiontext = aggregation + '(' + field + ')';

      }
    } else {
      if (Object.prototype.toString.call(aggregation).toLowerCase() === "[object array]") {
        var aggregationText = "";
        aggregation.forEach(function (d) {
          aggregationText = aggregationText + d + "("
        });
        conditiontext = aggregationText +(table?( encloseField(table) + '.'):'') + encloseField(field);
        aggregationText = "";
        aggregation.forEach(function (d) {
          aggregationText = aggregationText + ")"
        });
        conditiontext = conditiontext + aggregationText;

      } else {
        conditiontext = aggregation + '(' + (table?(encloseField(table) + '.'):'') + encloseField(field) + ')';

      }
    }
  } else {
    if (encloseFieldFlag == false) {
      conditiontext = field;
    } else {
      conditiontext = '' + (table?(encloseField(table) + '.'):'') + encloseField(field) + '';
    }
  }

  if (operator != undefined) {
    if (Array.isArray(value) && value.length == 1) {
      const updatedValue = value[0];
      value = updatedValue;
    }
    var sign = operatorSign(operator, value);
    if (sign.indexOf('IN') > -1) { //IN condition has different format
      var tempValue = value.map(d => d != null ? d.toString().replace(/\'/ig, "\\\'") : d).join("','");
      conditiontext += " " + sign + " ('" + tempValue + "')";
    } else {
      var tempValue = '';
      if (typeof value === 'undefined' || value == null) {
        tempValue = 'null';
      } else if (typeof value === 'object') {
        sign = operatorSign(operator, '');
        if (value.hasOwnProperty('field')) {
          var rTable = value.table ? value.table : '';
          tempValue = (rTable?(encloseField(rTable) + '.'):'') + encloseField(value.field);
        }
      } else {
        tempValue = '\'' + replaceSingleQuote(value) + '\'';
      }
      conditiontext += ' ' + sign + ' ' + tempValue;
    }
  }
  return conditiontext;
}

//create join conditions
function createJOIN(join) {
  var joinText = '';
  if (join != null) {
    var fromTbl = join.table;
    var fromTblAlias = join.alias;
    var joinwith = join.joinwith;
    // var strJoinConditions = '';
    joinText += encloseField(fromTbl) + (fromTblAlias ? (' as ' + fromTblAlias) : '');
    for (var j = 0; j < joinwith.length; j++) {
      var table = joinwith[j].table,
        tableAlias = joinwith[j].alias,
        type = joinwith[j].type ? joinwith[j].type : 'INNER',
        joincondition = joinwith[j].joincondition;
      joinText += ' ' + type.toString().toUpperCase() + ' JOIN ' + encloseField(table) + (tableAlias ? (' as ' + tableAlias) : '') + ' ON ' + createFilter(joincondition).join('');
    }
  }
  return joinText;
}


//run query
exports.execQuery = function (query, connection, cb) {
  return execQuery(query, connection, cb);
}

function execQuery(query, connection, cb) {
  connection.query(query, function (err, result, fields) {
    cb(err, result, fields);
  });
}
