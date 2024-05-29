var debug = require('debug')('node-database-connectors:node-database-connectors');
const fs = require('fs');
var db = require('snowflake-sdk');
const utils = require("./utils.js");
const { DefaultAzureCredential } = require("@azure/identity");
const { SecretClient } = require("@azure/keyvault-secrets");
//connect
var fieldIdentifier_left = '"',
  fieldIdentifier_right = '"',
  clientCredentials={
    clientSecret:''
  }

exports.connectPool = function (json, cb) {
  return connectPool(json, cb);
}

exports.connect = function (json, cb) {
  return connect(json, cb);
}

async function connectPool(json, cb) {
  let connectionObject = {
    account: json.extraparam.account,
    username: json.user,
    password: json.password,
    database: json.database,
    warehouse: json.extraparam.warehouse,
    authenticator: json.extraparam.authenticator,
  };
  if (json.extraparam.insecureConnect) {
    db.configure({ insecureConnect: true });
  }
  var pool = db.createConnection(connectionObject);
  if (json.extraparam.authenticator?.toUpperCase() === "EXTERNALBROWSER") {
    pool.connectAsync(function (err, conn) {
      if (err) {
        console.error("Unable to connect: " + err.message); //conn, connection0
        cb(err, null);
      } else {
        cb(null, conn);
      }
    });/*
    json.extraparam.authenticator - EXTERNALBROWSER,SNOWFLAKE,OAUTH
    json.extraparam.account - xxxxx.xxxx-xxxx.azure
    json.database - XXXX
    */
  } else if (json.extraparam.authenticator?.toUpperCase() === "OAUTH") {
    var clientSecret=await getClientSecret(json.extraparam);
    const accessToken = await utils.getAccessToken(json,clientSecret);
    var connectionSF = db.createConnection({
      account: json.extraparam.account,
      authenticator: json.extraparam.authenticator,
      database: json.database,
      token: accessToken,
    });
    connectionSF.connect(function (err, conn) {
      if (err) {
        if(err.code=='390303'){
          getClientSecret(json.extraparam);
        }
        console.error("Unable to connect: " + err.message); 
        cb(err, null);
      } else {
        cb(null, conn);
      }
    });
  } else {
    pool.connect(function (err, conn) {
      if (err) {
        console.error("Unable to connect: " + err.message); 
        cb(err, null);
      } else {
        cb(null, conn);
      }
    });
  }
}

async function connect(json, cb) {
  let connectionObject = {
    account: json.extraparam.account,
    username: json.user,
    password: json.password,
    database: json.database,
    warehouse: json.extraparam.warehouse,
    authenticator: json.extraparam.authenticator,
  };

  if (json.extraparam.insecureConnect) {
    db.configure({ insecureConnect: true });
  }
  var connection = db.createConnection(connectionObject);
  if (json.extraparam.authenticator?.toUpperCase() === "EXTERNALBROWSER") {
    connection.connectAsync(function (err, conn) {
      if (err) {
        console.error("Unable to connect: " + err.message); 
        cb(err, null);
      } else {
        cb(null, conn);
      }
    });/*
    json.extraparam.authenticator - EXTERNALBROWSER,SNOWFLAKE,OAUTH
    json.extraparam.account - xxxxx.xxxx-xxxx.azure
    json.database - XXXX
    */
  } else if (json.extraparam.authenticator?.toUpperCase() === "OAUTH") {
    const clientSecret=await getClientSecret(json.extraparam);
    const accessToken = await utils.getAccessToken(json,clientSecret);
    var connectionSF = db.createConnection({
      account: json.extraparam.account,
      authenticator: json.extraparam.authenticator,
      database: json.database,
      token: accessToken,
    });
    connectionSF.connect(function (err, conn) {
      if (err) {
        if(err.code=='390303'){
          getClientSecret(json.extraparam);
        }
        console.error("Unable to connect: " + err.message); 
        cb(err, null)
      } else {
        cb(null, conn);
      }
    });
  } else {
    connection.connect(function (err, conn) {
      if (err) {
        console.error("Unable to connect: " + err.message); 
        cb(err, null);
      } else {
        cb(null, conn);
      }
    });
  }

  return connection;
}
async function getClientSecret(extraparam){
  if(extraparam.keyVault!=undefined){
  if(extraparam)
 if(extraparam.clientSecretKey==undefined){
  if(clientCredentials.clientSecret!=''){
    return clientCredentials.clientSecret;
  }else{
    let clientSecret = await fetchClientSecret(extraparam);
    clientCredentials.clientSecret=clientSecret;
    return clientSecret
  }
 }else{
  if(clientCredentials[extraparam.clientSecretKey]!=undefined && clientCredentials[extraparam.clientSecretKey]!=''){
    return clientCredentials[extraparam.clientSecretKey];
  }else{
    let clientSecretOthers = await fetchClientSecret(extraparam);
    clientCredentials[extraparam.clientSecretKey]=clientSecretOthers;
    return clientSecretOthers
  }
 }
}else{
  return extraparam.clientSecret;
}
}
async function fetchClientSecret(extraparam){
  return new Promise((resolve, reject) => {
    const credential = new DefaultAzureCredential();
    const url = `https://${extraparam.keyVault.vaultName}.vault.azure.net`;
    const client = new SecretClient(url, credential);
    client
    .getSecret(extraparam.keyVault.secretName)
    .then((latestSecret) => {
      resolve(latestSecret.value);

    }).catch((err) => {
      reject('Fetching Secret key Failed',err);
    });
  });
}
//disconnect
exports.disconnect = function () {
  return disconnect(arguments[0]);
}

function disconnect(connection) {
  connection.destroy();
}

//prepare query
exports.prepareQuery = function() {
  return prepareQuery(arguments[0]);
}

function createInsertQuery(json) {
  var table = json.table ? json.table : null;
  var schema = json.schema ? encloseField(json.schema) : null;
  var vInsert = json.insert ? json.insert : null;
  var arrInsert = [];
  arrInsert = createInsert(vInsert);
  var query = '';
  if (!Array.isArray(vInsert)) {
    if (!Array.isArray(vInsert.fValue[0])) {
      query = 'INSERT INTO ' + (schema ? schema + '.' : '') + table + '(' + arrInsert.fieldArr.join() + ') VALUES(' + arrInsert.valueArr.join() + ')';
    } else {
      query = 'INSERT INTO ' + (schema ? schema + '.' : '') + table + '(' + arrInsert.fieldArr.join() + ') VALUES ' + arrInsert.valueArr.join() + '';
    }

  } else {
    query = 'INSERT INTO ' + (schema ? schema + '.' : '') + table + '(' + arrInsert.fieldArr.join() + ') VALUES(' + arrInsert.valueArr.join() + ')';
  }
  return query + ';';
}

function createUpdateQuery(json) {
  var arrUpdate = [],
    arrFilter = [],
    strJOIN = '';
  var table = json.table ? json.table : null;
  var schema = json.schema ? encloseField(json.schema) : null;
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
      table =  encloseField(table, json.encloseField) + (fromTblAlias ? (' as ' + fromTblAlias) : '');
    }
  }

  arrUpdate = createUpdate(vUpdate);
  query = 'UPDATE ' + (schema ? schema + '.' : '') + table + ' SET ' + arrUpdate.join() + '';
  if (arrFilter.length > 0) {
    query += ' WHERE ' + arrFilter.join('');
  }
  return query + ';';
}

function createSelectQuery(json, selectAll) {
  // console.log("json", json)
  var arrSelect = [],
    arrSortBy = [],
    arrFilter = [],
    arrGroupBy = [],
    arrHaving = [],
    strJOIN = '';
  var table = json.table ? json.table : null;
  var schema = json.schema ? encloseField(json.schema) : null;
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
    table = encloseField(table, json.encloseField) + (fromTblAlias ? (' as ' + fromTblAlias) : '');
  }

  //order by
  if (sortby != null) {
    for (var s = 0; s < sortby.length; s++) {
      var sortField = encloseField(sortby[s].field, sortby[s].encloseField)
      var sortTable = sortby[s].table != undefined ? encloseField(sortby[s].table) : null;
      var sortSchema = sortby[s].schema ? encloseField(sortby[s].schema) : null;
      var sortOrder = sortby[s].order ? sortby[s].order : 'ASC';
      if (sortTable == null)
        arrSortBy.push(sortField + ' ' + sortOrder);
      else
        arrSortBy.push((sortSchema ? sortSchema + '.' : '') + sortTable + '.' + sortField + ' ' + sortOrder);
    }
  }

  var query = 'SELECT ' + arrSelect.join();
  if (table != '') {
    query += ' FROM ' + (schema ? schema + '.' : '') + table + '';
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

  console.log(query);
  return query + ';';
}

function createDeleteQuery(json) {
  var table = json.table ? json.table : null;
  var schema = json.schema ? encloseField(json.schema) : null;
  var arrFilter = [];
  var vFilter = json.filter ? json.filter : null;;
  if (vFilter != null) {
    arrFilter = createFilter(vFilter);
  }
  var query = '';
  if (arrFilter.length > 0) {
    query = 'DELETE FROM ' + (schema ? schema + '.' : '') + table + ' WHERE' + arrFilter.join('');
  } else {
    query = 'DELETE FROM ' + (schema ? schema + '.' : '') + table + ' WHERE 1=1';
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
        if (obj.encloseField != undefined && typeof obj.encloseField != "boolean") {
          obj.encloseField = obj.encloseField == "false" ? false : true;
        }
        var encloseFieldFlag = (obj.encloseField != undefined) ? obj.encloseField : true;
        var field = encloseField(obj.field, encloseFieldFlag);
        var table = encloseField((obj.table ? obj.table : ''));
        var schema = obj.schema ? encloseField(obj.schema) : null;
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
              selectText += " WHEN " + (schema ? schema + '.' : '') + table + "." + field + " " + strOperatorSign + " ('" + value.join("','") + "') THEN " + outVal;
            } else {
              selectText += "WHEN " + (schema ? schema + '.' : '') + table + "." + field + " " + strOperatorSign + " '" + value + "' THEN " + outVal;
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
              selectText = ' DATE_FORMAT(' + (schema ? schema + '.' : '') + table + '.' + field + ',\'' + format + '\') ';
            } else {
              if (encloseFieldFlag == false || encloseFieldFlag == 'false')
                selectText = field;
              else
                selectText = (schema ? schema + '.' : '') + table + '.' + field;
            }
          } else {
            if (encloseFieldFlag == false || encloseFieldFlag == 'false') {
              selectText = field;
            } else {
              if (table == fieldIdentifier_left + fieldIdentifier_right)
                selectText = field;
              else
                selectText = (schema ? schema + '.' : '') + table + '.' + field;
            }
          }
        }

        if (aggregation != null) {
          //CBT:this is for nested aggregation if aggregation key contains Array
          if (Object.prototype.toString.call(aggregation).toLowerCase() === '[object array]') {
            var aggregationText = "";
            aggregation.forEach(function(d) {
              aggregationText = aggregationText + d + '('
            });
            selectText = aggregationText + selectText;
            aggregationText = "";
            aggregation.forEach(function(d) {
              aggregationText = aggregationText + ')'
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
            subValueArr.push('\'' + fValue + '\'');

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
        if (obj.encloseField != undefined && typeof obj.encloseField != "boolean") {
          obj.encloseField = obj.encloseField == "false" ? false : true;
        }
        var encloseFieldFlag = (obj.encloseField != undefined) ? obj.encloseField : true;
        var field = encloseField(obj.field, encloseFieldFlag)
        var table = encloseField(obj.table ? obj.table : '');
        var fValue = obj.fValue;// ? obj.fValue : '';
        fValue = (fValue == null ? fValue : replaceSingleQuote(fValue));
        tempJson.fieldArr.push(field);
        if(fValue != null) {
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
      var table = encloseField(obj.table ? obj.table : '');
      var schema = obj.schema ? encloseField(obj.schema) : null;
      var fValue = obj.fValue;// ? obj.fValue : '';
      fValue = (fValue == null ? fValue : replaceSingleQuote(fValue));
      var selectText = '';
      if(fValue != null) {
        if (table == fieldIdentifier_left + fieldIdentifier_right)
          selectText = field + '=' + '\'' + fValue + '\'';
        else
          selectText = (schema ? schema + '.' : '') + table + '.' + field + '=' + '\'' + fValue + '\'';
      } else {
       if(encloseFieldFlag==true){
          if (table == fieldIdentifier_left + fieldIdentifier_right)
            selectText = field + '=null';
          else
            selectText = (schema ? schema + '.' : '') + table + '.' + field + '=null';
        }else{
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
    schema = obj.schema ? encloseField(obj.schema) : null,
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
          aggregationText = aggregationText + d + '('
        });
        conditiontext = aggregationText + field;
        aggregationText = "";
        aggregation.forEach(function(d) {
          aggregationText = aggregationText + ')'
        });
        conditiontext = conditiontext + aggregationText;

      } else {
        conditiontext = aggregation + '(' + field + ')';

      }
    } else {
      if (Object.prototype.toString.call(aggregation).toLowerCase() === "[object array]") {
        var aggregationText = "";
        aggregation.forEach(function(d) {
          aggregationText = aggregationText + d + '('
        });
        conditiontext = aggregationText + (schema ? schema + '.' : '') + encloseField(table) + '.' + encloseField(field);
        aggregationText = "";
        aggregation.forEach(function(d) {
          aggregationText = aggregationText + ')'
        });
        conditiontext = conditiontext + aggregationText;

      } else {
        conditiontext = aggregation + '(' + (schema ? schema + '.' : '') + encloseField(table) + '.' + encloseField(field) + ')';

      }
    }
  } else {
    if (encloseFieldFlag == false) {
      conditiontext = field;
    } else {

      if (table == fieldIdentifier_left + fieldIdentifier_right)
        selectText = field;
      else
        selectText = (schema ? schema + '.' : '') + table + '.' + field;

      if (encloseField(table) == fieldIdentifier_left + fieldIdentifier_right)
        conditiontext = encloseField(field);
      else
        conditiontext = (schema ? schema + '.' : '') + encloseField(table) + '.' + encloseField(field) + '';
    }
  }

  if (operator != undefined) {
    if(Array.isArray(value) && value.length ==1)
     {
       const updatedValue = value[0];
       value = updatedValue;
     }
    var sign = operatorSign(operator, value);
    if (sign.indexOf('IN') > -1) { //IN condition has different format
      if (typeof value[0] == 'string') {
        conditiontext += " " + sign + " ('" + value.join("','") + "')";
      } else {
        conditiontext += " " + sign + " ('" + value.join(",") + ")";
      }
    } else {
      var tempValue = '';
      if (typeof value === 'undefined' || value == null) {
        tempValue = 'null';
      } else if (typeof value === 'object') {
        sign = operatorSign(operator, '');
        if (value.hasOwnProperty('field')) {
          var rTable = value.table ? value.table : '';
          var rSchema = value.schema ? encloseField(value.schema) : null;
          tempValue = (rSchema ? rSchema + '.' : '') + encloseField(rTable) + '.' + encloseField(value.field);
        }
      } else {
        if (typeof value == 'string') {
          tempValue = '\'' + replaceSingleQuote(value) + '\'';
        } else {
          tempValue = value;
        }
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
    var fromSchema = join.schema ? encloseField(join.schema) : null;
    var fromTblAlias = join.alias;
    var joinwith = join.joinwith;
    // var strJoinConditions = '';
    joinText += (fromSchema ? fromSchema + '.' : '') + encloseField(fromTbl) + (fromTblAlias ? (' as ' + fromTblAlias) : '');
    for (var j = 0; j < joinwith.length; j++) {
      var table = joinwith[j].table,
        schema = joinwith[j].schema ? encloseField(joinwith[j].schema): null,
        tableAlias = joinwith[j].alias,
        type = joinwith[j].type ? joinwith[j].type : 'INNER',
        joincondition = joinwith[j].joincondition;

      //            for (var jc = 0; jc < joincondition.length; jc++) {
      //                strJoinConditions += joincondition[jc].on + ' ' + operatorSign(joincondition[jc].operator, joincondition[jc].value) + ' ' + joincondition[jc].value + ' ';
      //            }
      joinText += ' ' + type.toString().toUpperCase() + ' JOIN ' + (schema ? schema + '.' : '') + encloseField(table) + (tableAlias ? (' as ' + tableAlias) : '') + ' ON ' + createFilter(joincondition).join('');
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
    connection.query(query, function(err, result) {
      cb(err, result, null);
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
