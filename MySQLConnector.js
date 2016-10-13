var debug = require('debug')('node-database-connectors:node-database-connectors');
var db = require('mysql');

//connect
var fieldIdentifier_left = '`',
  fieldIdentifier_right = '`';

exports.connectPool = function(json, cb) {
  connectPool(json, cb);
}

exports.connect = function(json, cb) {
  connect(json, cb);
}

function connectPool(json, cb){
  var numConnections = json.connectionLimit || 10;
  var pool  = db.createPool({
    acquireTimeout: json.acquireTimeout || 30 * 1000,
    connectionLimit : numConnections,
    host: json.host,
    port: json.port,
    user: json.user,
    database: json.database,
    password: json.password
  });
  cb(null, pool);

  acquireConnection(0);
  function acquireConnection(index){
    if(index >= numConnections){
      return;
    }
    debug("acquiring connection ", index, json);
    pool.getConnection(function(err, connection){
      if(err){
        debug("error in acquiring connection", index, err, json);
      } else {
        debug("releasing connection ", index, json);
        connection.release();
      }
      acquireConnection(index+1);
    });
  }
}

function connect(json, cb) {
  var connection = db.createConnection({
    host: json.host,
    port: json.port,
    user: json.user,
    database: json.database,
    password: json.password
  });
  connection.connect(function(err) {
    if (err) {
      debug('error-A');
      debug(['c.connect', err]);
    } else {
      connection.on('error', function(e) {
        debug('error-B');
        debug(['error', e]);
      });
    }
    cb(err, connection);
  });
}

//disconnect
exports.disconnect = function() {
  return disconnect(arguments[0]);
}

function disconnect(connection) {
  connection.end();
}

//prepare query
exports.prepareQuery = function() {
  return prepareQuery(arguments[0]);
}

function createInsertQuery(json) {
  var table = json.table ? json.table : null;
  var vInsert = json.insert ? json.insert : null;
  var arrInsert = [];
  arrInsert = createInsert(vInsert);
  var query = '';
  if (!Array.isArray(vInsert)) {
    if (!Array.isArray(vInsert.fValue[0])) {
      query = 'INSERT INTO ' + table + '(' + arrInsert.fieldArr.join() + ') VALUES(' + arrInsert.valueArr.join() + ')';
    } else {
      query = 'INSERT INTO ' + table + '(' + arrInsert.fieldArr.join() + ') VALUES ' + arrInsert.valueArr.join() + '';
    }

  } else {
    query = 'INSERT INTO ' + table + '(' + arrInsert.fieldArr.join() + ') VALUES(' + arrInsert.valueArr.join() + ')';
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
  if (!json.hasOwnProperty('insert') && !json.hasOwnProperty('update') && !json.hasOwnProperty('delete') && !json.hasOwnProperty('select')) {
    return 'J2Q_INVALID_JSON';
  }
  if (json.hasOwnProperty('filter') && json.hasOwnProperty('insert')) {
    return 'J2Q_INVALID_INSERTJOSN';
  }

  if (json.hasOwnProperty('limit') || json.hasOwnProperty('having') || json.hasOwnProperty('groupby')) {
    if (!json.hasOwnProperty('select')) {
      return 'J2Q_ONLYSELECT_JSON';
    }
  }

  if (json.hasOwnProperty('insert') && (json.hasOwnProperty('update') || json.hasOwnProperty('delete') || json.hasOwnProperty('select'))) {
    return 'J2Q_INSERT_UPDATE_MERGED';
  }
  if (json.hasOwnProperty('udpate') && (json.hasOwnProperty('insert') || json.hasOwnProperty('delete') || json.hasOwnProperty('select'))) {
    return 'J2Q_INSERT_UPDATE_MERGED';
  }
  if (json.hasOwnProperty('delete') && (json.hasOwnProperty('update') || json.hasOwnProperty('insert') || json.hasOwnProperty('select'))) {
    return 'J2Q_INSERT_UPDATE_MERGED';
  }
  if (json.hasOwnProperty('select') && (json.hasOwnProperty('update') || json.hasOwnProperty('delete') || json.hasOwnProperty('insert'))) {
    return 'J2Q_INSERT_UPDATE_MERGED';
  } else {
    return '';
  }
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
            if(Object.prototype.toString.call(aggregation).toLowerCase()==="[object array]"){
              var aggregationText="";
              aggregation.forEach(function(d){
               aggregationText=aggregationText+d+"("
              });
              selectText=aggregationText+selectText;
              aggregationText="";
              aggregation.forEach(function(d){
               aggregationText=aggregationText+")"
              });
              selectText=selectText+aggregationText;

           }else{
            selectText = aggregation + '(' + selectText + ')';

           }
        }
        if (hasAlias) selectText += ' as ' + alias;
        tempArr.push(selectText);
        selectText = null;
      };
    }
  }

  // else {
  //   tempArr.push('*');
  // }
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
      for (var y = 0; y < arr.field.length; y++) {
        var obj = arr.field[y];
        var field = encloseField(obj, encloseFieldFlag)
        tempJson.fieldArr.push(field);
      }
      // arr.forEach(function(obj){
      //   var field = encloseField(obj, encloseFieldFlag)
      //   tempJson.fieldArr.push(field);
      // });
      for (var z = 0; z < arr.fValue.length; z++) {
        var obj = arr.fValue[z];
        if (Array.isArray(obj)) {
          subValueArr = [];
          for (var k = 0; k < obj.length; k++) {
            var objSub = obj[k]
            var fValue = objSub
            fValue = (replaceSingleQuote(fValue));
            if(fValue !== "NULL"){
              subValueArr.push('\'' + fValue + '\'');
            } else{
              subValueArr.push(fValue);
            }
          }
          if (tempJson.valueArr !== []) {
            tempJson.valueArr.push('(' + subValueArr.join() + ')');
          } else {
            tempJson.valueArr.push(', (' + subValueArr.join() + ')');
          }
        } else {
          var fValue = (replaceSingleQuote(obj));
          tempJson.valueArr.push('\'' + fValue + '\'');
        }
      }
    } else {
      for (var s = 0; s < arr.length; s++) {
        var obj = arr[s];
        var encloseFieldFlag = (obj.encloseField != undefined) ? obj.encloseField : true;
        var field = encloseField(obj.field, encloseFieldFlag)
        var table = encloseField(obj.table ? obj.table : '');
        var fValue = obj.fValue ? obj.fValue : '';
        fValue = (replaceSingleQuote(fValue));
        tempJson.fieldArr.push(field);
        tempJson.valueArr.push('\'' + fValue + '\'');
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
      var encloseFieldFlag = (obj.encloseField != undefined) ? obj.encloseField : true;
      var field = encloseField(obj.field, encloseFieldFlag)
      var table = encloseField(obj.table ? obj.table : '');
      var fValue = obj.fValue ? obj.fValue : '';
      fValue = (replaceSingleQuote(fValue));
      var selectText = '';
      selectText = table + '.' + field + '=' + '\'' + fValue + '\'';
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

exports.createFilter = function(arr) {
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
  if (conditionType.toString().toLowerCase() != 'none') {
    var listOfConditions = obj[conditionType]; //all conditions
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
    } else if (typeof value === 'undefined') {
      sign = 'IS';
    } else if (typeof value == 'string') {
      sign = '=';
    } else {
      sign = '=';
    }
  } else if (operator.toString().toLowerCase() == 'noteq') {
    if (Object.prototype.toString.call(value) === '[object Array]') {
      sign = 'NOT IN';
    } else if (typeof value === 'undefined') {
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

  var conditiontext = '';
  if (aggregation != null) {
    if (encloseFieldFlag == false) {
     //CBT:this is for nested aggregation if aggregation key contains Array
     if(Object.prototype.toString.call(aggregation).toLowerCase()==="[object array]"){
          var aggregationText="";
          aggregation.forEach(function(d){
           aggregationText=aggregationText+d+"("
          });
          conditiontext=aggregationText+field;
          aggregationText="";
          aggregation.forEach(function(d){
           aggregationText=aggregationText+")"
          });
          conditiontext=conditiontext+aggregationText;

       }else{
        conditiontext = aggregation + '(' + field + ')';

       }
    } else {
      if(Object.prototype.toString.call(aggregation).toLowerCase()==="[object array]"){
           var aggregationText="";
           aggregation.forEach(function(d){
            aggregationText=aggregationText+d+"("
           });
           conditiontext=aggregationText+encloseField(table) + '.' + encloseField(field);
           aggregationText="";
           aggregation.forEach(function(d){
            aggregationText=aggregationText+")"
           });
           conditiontext=conditiontext+aggregationText;

        }else{
         conditiontext = aggregation + '(' + encloseField(table) + '.' + encloseField(field) + ')';

        }
    }
  } else {
    if (encloseFieldFlag == false) {
      conditiontext = field;
    } else {
      conditiontext = '' + encloseField(table) + '.' + encloseField(field) + '';
    }
  }

  if (operator != undefined) {
    var sign = operatorSign(operator, value);
    if (sign.indexOf('IN') > -1) { //IN condition has different format
      conditiontext += ' ' + sign + ' ("' + value.join('","') + '")';
    } else {
      var tempValue = '';
      if (typeof value === 'undefined') {
        tempValue = 'null';
      } else if (typeof value === 'object') {
        sign = operatorSign(operator, '');
        if (value.hasOwnProperty('field')) {
          var rTable = value.table ? value.table : '';
          tempValue = encloseField(rTable) + '.' + encloseField(value.field);
        }
      } else {
        tempValue = '\'' + value + '\'';
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

      //            for (var jc = 0; jc < joincondition.length; jc++) {
      //                strJoinConditions += joincondition[jc].on + ' ' + operatorSign(joincondition[jc].operator, joincondition[jc].value) + ' ' + joincondition[jc].value + ' ';
      //            }
      joinText += ' ' + type.toString().toUpperCase() + ' JOIN ' + encloseField(table) + (tableAlias ? (' as ' + tableAlias) : '') + ' ON ' + createFilter(joincondition).join('');
    }
  }
  return joinText;
}


//run query
exports.execQuery = function() {
  return execQuery(arguments);
}

function execQuery(cb) {
  var query = arguments[0][0];
  var connection = null;
  var format = null;
  if (arguments[0].length > 1) {
    format = arguments[0][2];
  }
  if (arguments[0].length > 0) {
    connection = arguments[0][1];
    //Commenting pipe and returning full JSON;
    //return connection.query(query).stream({ highWaterMark: 5 }).pipe(objectToCSV(format));
    connection.query(query, function(err, result, fields) {
      cb(arguments[0][3]);
    });
  } else {
    return {
      status: false,
      content: {
        result: 'Connection not specified.'
      }
    };
  }
}
/*
function objectToCSV(format) {
var stream = require('stream')
var liner = new stream.Transform({ objectMode: true })
var csv = [];
var isFirstChunk = true;
liner._transform = function (chunk, encoding, done) {
if (format == 'csv') {
var keys = Object.keys(chunk);
csv = [];
if (isFirstChunk == true) {
for (var i = 0; i < keys.length; i++) {
csv.push(keys[i]);
}
}
else {
for (var i = 0; i < keys.length; i++) {
csv.push(chunk[keys[i]]);
}
}
this.push(csv.join());
}
else if (format == 'jsonArray') {
var strChunk = '';
if (isFirstChunk == true) {
strChunk += '[' + JSON.stringify(chunk);
}
else {
strChunk += ',' + JSON.stringify(chunk);
}
this.push(strChunk);
}
else {
this.push(chunk);
}
isFirstChunk = false;
done()
}

liner._flush = function (done) {
if (format == 'jsonArray') {
this.push(']');
}
done()
}

return liner;
}
*/
