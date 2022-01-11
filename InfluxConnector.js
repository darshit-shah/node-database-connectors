const debug = require('debug')('node-database-connectors:node-database-connectors');
const db = require('influx');
const utils=require("./utils.js");

// return influxDB connection
function connect(json, cb) {
  const connection = new db.InfluxDB({
    host: json.host,
    port: json.port || 8086,
    username: json.user,
    database: json.database,
    password: json.password
  });
  cb(null, connection)
}

function createSelectQuery(json, selectAll) {
  if(json.groupby && json.groupby.length > 0){
    json.groupby = json.groupby.map(d => {
      d["oldfield"] = d["field"] ? d["field"].replace(/`/gi, '') : d["field"]
      d["field"] = json.select.find(sel => sel.alias === d.oldfield)["field"]
      return d;
    });
    const groupbyKeys = json.groupby.map(d => d["oldfield"]);
    if(groupbyKeys.length > 0){
      json.select = json.select.filter(d => groupbyKeys.indexOf(d.alias) === -1)
    }
  }
  let arrSelect = [],
    arrSortBy = [],
    arrFilter = [],
    arrGroupBy = [],
    arrHaving = []
  let table = json.table ? json.table : null;
  let sortby = json.sortby ? json.sortby : null,
    limit = json.limit ? json.limit : null,
    join = json.join ? json.join : null;
  let vSelect = json.select ? json.select : null,
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

  //order by
  if (sortby != null) {
    for (let s = 0; s < sortby.length; s++) {
      let encloseFieldFlag = (sortby[s].encloseField != undefined) ? sortby[s].encloseField : true;
      let sortField = encloseField(sortby[s].field, encloseFieldFlag);
      let sortTable = sortby[s].table != undefined ? encloseField(sortby[s].table) : null;
      let sortOrder = sortby[s].order ? sortby[s].order : 'ASC';
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

function validateJson(json) {
  return utils.validateJson(json);
}

function prepareQuery(json) {
  const validate = validateJson(json);
  if (validate !== '') {
    return validate;
  } else {
    let query = '';
    let vSelect = json.select ? json.select : null;
    //SELECT
    if (vSelect != null) {
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
            aggregation.forEach(function(d) {
              aggregationText = aggregationText + d + "("
            });
            selectText = aggregationText + selectText;
            aggregationText = "";
            aggregation.forEach(function(d) {
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

function replaceSingleQuote(aValue) {
  if (aValue != undefined && typeof aValue === 'string') {
    aValue = aValue.replace(/\'/ig, "\\\'");
    return aValue;
  } else {
    return aValue;
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
  return a;
}

// get operator sign based on operation
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

// create single filter condition
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
    conditiontext = field;
  }

  if (operator != undefined) {
    if(Array.isArray(value) && value.length ==1)
     {
       const updatedValue = value[0];
       value = updatedValue;
     }
    var sign = operatorSign(operator, value);
    if (sign.indexOf('IN') > -1) {
      //IN condition has different format for Influx
      var tempValue = value.map(d => {
        const filterVal = d != null ? d.toString().replace(/\'/ig, "\\\'") : d
        return `${conditiontext} = '${filterVal}'`
      }).join(" or ");
      conditiontext = `( ${tempValue} )`;
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
      } else if (typeof value === "number") {
        tempValue = replaceSingleQuote(value);
      } else {
        tempValue = '\'' + replaceSingleQuote(value) + '\'';
      }
      conditiontext += ' ' + sign + ' ' + tempValue;
    }
  }
  return conditiontext;
}

//prepare raw query
exports.prepareQuery = function() {
  return prepareQuery(arguments[0]);
}

//disconnect
exports.disconnect = function() {
  return true;
}

// connect
exports.connect = function(json, cb) {
  return connect(json, cb);
}
