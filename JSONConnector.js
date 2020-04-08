var debug = require('debug')('node-database-connectors:node-database-connectors');
const utils=require("./utils.js");
//connect
var fieldIdentifier_left = '`',
  fieldIdentifier_right = '`';

exports.connectPool = function(json, cb) {
  return connectPool(json, cb);
}

exports.connect = function(json, cb) {
  return connect(json, cb);
}

function connectPool(json, cb) {
 cb(null,json);
}

function connect(json, cb) {
  cb(null,json);
}

//disconnect
exports.disconnect = function() {
  return disconnect(arguments[0]);
}

function disconnect(connection) {
//   connection.end();
return "";
}

//prepare query
exports.prepareQuery = function() {
  return prepareQuery(arguments[0]);
}

function createInsertQuery(json) {
  return query;
}

function createUpdateQuery(json) {
  return query;
}

function createSelectQuery(json, selectAll) {
  return query ;
}

function createDeleteQuery(json) {
  return query;
}

function validateJson(json) {
  const result=utils.validateJson(json);
  if (!json.hasOwnProperty('join') && !json.hasOwnProperty('table')) {
    throw new Error('table or join key missing');
  }
  if (json.hasOwnProperty('join') && json.hasOwnProperty('table')) {
    throw new Error('Either join or table allowed at a time');
  }
  if (json.hasOwnProperty('insert') || json.hasOwnProperty('update') || json.hasOwnProperty('delete') ) {
    throw new Error('insert,update and delete operations are not supported now');
  }
 return result;
}

function prepareQuery(json) {
  var validate = validateJson(json);
  if (validate !== '') {
    return validate;
  } else {
    return json;
  }
}

//Create select expression
function createSelect(arr, selectAll) {
  return arr;
}

function createInsert(arr) {
  return arr;
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
  return arr;
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
        aggregation.forEach(function(d) {
          aggregationText = aggregationText + d + "("
        });
        conditiontext = aggregationText + field;
        aggregationText = "";
        aggregation.forEach(function(d) {
          aggregationText = aggregationText + ")"
        });
        conditiontext = conditiontext + aggregationText;

      } else {
        conditiontext = aggregation + '(' + field + ')';

      }
    } else {
      if (Object.prototype.toString.call(aggregation).toLowerCase() === "[object array]") {
        var aggregationText = "";
        aggregation.forEach(function(d) {
          aggregationText = aggregationText + d + "("
        });
        conditiontext = aggregationText + encloseField(table) + '.' + encloseField(field);
        aggregationText = "";
        aggregation.forEach(function(d) {
          aggregationText = aggregationText + ")"
        });
        conditiontext = conditiontext + aggregationText;

      } else {
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
          tempValue = encloseField(rTable) + '.' + encloseField(value.field);
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

      //            for (var jc = 0; jc < joincondition.length; jc++) {
      //                strJoinConditions += joincondition[jc].on + ' ' + operatorSign(joincondition[jc].operator, joincondition[jc].value) + ' ' + joincondition[jc].value + ' ';
      //            }
      joinText += ' ' + type.toString().toUpperCase() + ' JOIN ' + encloseField(table) + (tableAlias ? (' as ' + tableAlias) : '') + ' ON ' + createFilter(joincondition).join('');
    }
  }
  return joinText;
}


//run query
exports.execQuery = function(query, connection, cb) {
  return execQuery(query, connection, cb);
}

function execQuery(query, connection, cb) {
  // var query = arguments[0][0];
  // var connection = null;
  // var format = null;
  // if (arguments[0].length > 1) {
  //   format = arguments[0][2];
  // }
  // if (arguments[0].length > 0) {
    // connection = arguments[0][1];
    //Commenting pipe and returning full JSON;
    //return connection.query(query).stream({ highWaterMark: 5 }).pipe(objectToCSV(format));
    connection.query(query, function(err, result, fields) {
      cb(err, result, fields);
    });
  // } else {
  //   return {
  //     status: false,
  //     content: {
  //       result: 'Connection not specified.'
  //     }
  //   };
  // }
}