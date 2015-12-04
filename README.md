## NODE-DATABASE-CONNECTORS
#### Author: Axiom
##### Created on: 3rd Dec 2015


#### Function

* _prepareQuery :_

  ```javascript  
  var connectionIdentifier = require('node-database-connectors');  
  var objConnection = connectionIdentifier.identify(sampleConfig);
  var query = objConnection.prepareQuery(jsonQuery);
    ```



  - _sampleConfig_ : Configuration for database connection. (As given below)
  ```javascript
  var sampleConfig = {
    type: "database",
    engine: 'MyISAM',
    databaseType: 'mysql',
    database: 'database',
    host: "hostname",
    port: "port",
    user: "user",
    password: "password",
    cacheResponse: false
  };
  ```

  - _jsonQuery_ : JSON structure of Select, Insert, Update, Delete for Generating query
  * _Sample 1 (Select Query)_
  ```javascript
    var jsonQuery = {
      table: "tbl_SampleMaster",
      alias: "SM",
      select: [{
        field: 'pk_tableID',
        alias: 'pk'
      }, {
        field: 'refNumber'
      }],
      sortby: [{
        field: 'refNumber'
      }],
      filter: {
        AND: [{
          field: 'pk_id',
          operator: 'EQ',
          value: '1'
        }]
      }
    };
  ```
  _Output :_  
    ``` javascript

    SELECT ``.`pk_tableID` as `pk`,``.`refNumber`
    FROM `tbl_SampleMaster` as TM
    WHERE (``.`pk_id` = '1')
    ORDER BY `refNumber` ASC;

    ```

  * _Sample 2 (Select Query)_
  ```javascript
  var jsonQuery = {
    join: {
      table: 'tbl_tableMaster',
      alias: 'A',
      joinwith: [{
        table: 'tbl_OtherMaster',
        alias: 'B',
        joincondition: {
          table: 'A',
          field: 'TM_pk_id',
          operator: 'eq',
          value: {
            table: 'B',
            field: 'OT_fk_id'
          }
        }
      }]
    },
    select: [{
      table: 'A',
      field: 'pk_tableID',
      alias: 'pk'
    }, {
      table: 'B',
      field: 'refNumber'
    }],
    filter: {
      AND: [{
        field: 'pk_id',
        operator: 'EQ',
        value: '1'
      }]
    }
  };
  ```
  _Output :_
  ``` javascript

    SELECT `A`.`pk_tableID` as `pk`,`B`.`refNumber`
    FROM `tbl_tableMaster` as A
    INNER JOIN `tbl_OtherMaster` as B ON `A`.`TM_pk_id` = `B`.`OT_fk_id`
    WHERE (``.`pk_id` = '1');
    ```
  * _Sample 3 (Insert Query)_
  ```javascript
  var jsonQuery = {
    table: "tbl_SampleMaster",
    insert: [{
      field: 'SM_code',
      fValue: 'D0001'
    }, {
      field: 'SM_fname',
      fValue: 'Digi'
    }, {
      field: 'SM_lname',
      fValue: 'Corp'
    }],
  };
  ```
  _Output :_
  ``` javascript

    INSERT INTO tbl_PersonMaster(`SM_code`,`SM_fname`,`SM_lname`)
    VALUES(`D001`,`Digi`,`Corp`);
    ```


  * _Sample 3-1 (Insert Query)_
  ```javascript
  var jsonQuery = {
    table: "tbl_PersonMaster",
    insert:{
      field:['PM_Code','PM_fname','PM_lname'],
      fValue:[['CorDig','Digi', 'Corp'],['SofMic','Micro', 'Soft']],
    }
  };
  ```
  _Output :_
  ``` javascript
    INSERT INTO tbl_PersonMaster(`PM_Code`,`PM_fname`,`PM_lname`)
    VALUES((`CorDig`,`Digi`,`Corp`),(`SofMic`,`Micro`,`Soft`))
    ```



  * _Sample 4 (Update Query)_
  ```javascript
  var jsonQuery = {
    table: "tbl_SampleMaster",
    update: [{
      field: 'SM_code',
      fValue: 'D001'
    }, {
      field: 'SM_fname',
      fValue: 'Digi'
    }, {
      field: 'SM_lname',
      fValue: 'Corp'
    }],
    filter: {
      AND: [{
        field: 'pk_id',
        operator: 'EQ',
        value: '1'
      }]
    }
  };
  ```
  _Output :_
  ``` javascript

    UPDATE tbl_PersonMaster SET ``.`SM_code`=`D001`,``.`PM_fname`=`Ashraf`,``.`PM_lname`=`Ansari`
    WHERE (``.`pk_id` = '1');
    ```

  * _Sample 5 (Delete Query)_
  ```javascript
  var jsonQuery = {
    table: "tbl_PersonMaster",
    alias: "PM",
    delete: [],
    filter: {
      AND: [{
        field: 'pk_id',
        operator: 'EQ',
        value: '1'
      }]
    }
  };
  ```
  _Output :_
  ``` javascript

    DELETE FROM tbl_PersonMaster WHERE(``.`pk_id` = '1');
    ```    
