var Net = require('net'),
  EventEmitter = require('events').EventEmitter,
  Util = require('util'),
  ErrorMessages = require('./constants/ErrorMessages'),
  DATA_TYPES = require('./constants/DataTypes'),
  CASConstants = require('./constants/CASConstants'),
  PacketReader = require('./packets/PacketReader'),
  PacketWriter = require('./packets/PacketWriter'),
  ActionQueue = require('./utils/ActionQueue'),
  Helpers = require('../src/utils/Helpers'),
  Cache = require('../src/utils/Cache'),
  ClientInfoExchangePacket = require('./packets/ClientInfoExchangePacket'),
  OpenDatabasePacket = require('./packets/OpenDatabasePacket'),
  GetEngineVersionPacket = require('./packets/GetEngineVersionPacket'),
  ExecuteQueryPacket = require('./packets/ExecuteQueryPacket'),
  CloseQueryPacket = require('./packets/CloseQueryPacket'),
  BatchExecuteNoQueryPacket = require('./packets/BatchExecuteNoQueryPacket'),
  CloseDatabasePacket = require('./packets/CloseDatabasePacket'),
  FetchPacket = require('./packets/FetchPacket'),
  SetAutoCommitModePacket = require('./packets/SetAutoCommitModePacket'),
  RollbackPacket = require('./packets/RollbackPacket'),
  CommitPacket = require('./packets/CommitPacket');

module.exports = CUBRIDConnection;

//Support custom events
Util.inherits(CUBRIDConnection, EventEmitter);

/**
 * Create a new CUBRID connection instance
 * @param brokerServer
 * @param brokerPort
 * @param user
 * @param password
 * @param database
 * @param cacheTimeout
 * @constructor
 */
function CUBRIDConnection(brokerServer, brokerPort, user, password, database, cacheTimeout) {
  // Using EventEmitter.call on an object will do the setup of instance methods / properties
  // (not inherited) of an EventEmitter.
  // It is similar in purpose to super(...) in Java or base(...) in C#, but it is not implicit in Javascript.
  // Because of this, we must manually call it ourselves:
  EventEmitter.call(this);

  this._queryCache = null;
  if (typeof cacheTimeout !== 'undefined' && cacheTimeout > 0) {
    this._queryCache = new Cache();
  }

  this._socket = new Net.Socket();

  // Connection parameters
  this.brokerServer = brokerServer || 'localhost';
  this.initialBrokerPort = brokerPort || 33000;
  this.connectionBrokerPort = -1;
  this.user = user || 'public';
  this.password = password || '';
  this.database = database || 'demodb';

  // Session public variables
  this.autoCommitMode = null; //will be initialized on connect
  this.sessionId = 0;

  // Execution semaphore variables; prevent double-connect-attempts, overlapping-queries etc.
  this.connectionOpened = false;
  this.connectionPending = false;
  this.queryPending = false;

  // Driver events
  this.EVENT_ERROR = 'error';
  this.EVENT_CONNECTED = 'connect';
  this.EVENT_ENGINE_VERSION_AVAILABLE = 'engine version';
  this.EVENT_BATCH_COMMANDS_COMPLETED = 'batch execute done';
  this.EVENT_QUERY_DATA_AVAILABLE = 'query data';
  this.EVENT_FETCH_DATA_AVAILABLE = 'fetch';
  this.EVENT_FETCH_NO_MORE_DATA_AVAILABLE = 'fetch done';
  this.EVENT_BEGIN_TRANSACTION = 'begin transaction';
  this.EVENT_SET_AUTOCOMMIT_MODE_COMPLETED = 'set autocommit mode';
  this.EVENT_COMMIT_COMPLETED = 'commit';
  this.EVENT_ROLLBACK_COMPLETED = 'rollback';
  this.EVENT_QUERY_CLOSED = 'close query';
  this.EVENT_CONNECTION_CLOSED = 'close';

  //Auto-commit constants
  this.AUTOCOMMIT_ON = true;
  this.AUTOCOMMIT_OFF = false;

  //Database schema variables
  this.SCHEMA_TABLE = CASConstants.CCI_SCH_CLASS;
  this.SCHEMA_VIEW = CASConstants.CCI_SCH_VCLASS;

  //Private variables
  this._CASInfo = [0, 0xFF, 0xFF, 0xFF];
  this._queriesHandleList = new Array();
  this._INVALID_RESPONSE_LENGTH = -1;

  //Uncomment the following lines if you will not always provide an 'error' listener in your consumer code,
  //to avoid any unexpected exception. Be aware that:
  //Error events are treated as a special case in node. If there is no listener for it,
  //then the default action is to print a stack trace and exit the program.
  //http://nodejs.org/api/events.html
  //this.on('error',function(err){
  //  Helpers.logError(err.message);
  //  //... (add your own error-handling code)
  //});
}

/**
 * Get broker connection port
 * @param self
 * @param callback
 * @private
 */
CUBRIDConnection.prototype._doGetBrokerPort = function (self, callback) {
  self._socket = Net.createConnection(self.initialBrokerPort, self.brokerServer);
  self._socket.setNoDelay(true);

  var packetWriter = new PacketWriter();
  var clientInfoExchangePacket = new ClientInfoExchangePacket();
  clientInfoExchangePacket.write(packetWriter);
  self._socket.write(packetWriter._buffer);

  self._socket.on('error', function (err) {
    this.connectionOpened = false;
    delete this._queriesHandleList;
    callback.call(err);
  });

  self._socket.once('data', function (data) {
    var packetReader = new PacketReader();
    packetReader.write(data);
    clientInfoExchangePacket.parse(packetReader);
    var newPort = clientInfoExchangePacket.newConnectionPort;
    self.connectionBrokerPort = newPort;
    self._socket.end();
    if (callback && typeof(callback) === 'function') {
      if (newPort > 0) {
        callback.call(null);
      } else {
        var err = new Error(ErrorMessages.ERROR_NEW_BROKER_PORT);
        callback.call(err);
      }
    }
  });
};

/**
 * Login to a database
 * @param self
 * @param callback
 * @private
 */
CUBRIDConnection.prototype._doDatabaseLogin = function (self, callback) {
  var err = null;
  var responseData = new Buffer(0);
  var expectedResponseLength = this._INVALID_RESPONSE_LENGTH;

  self._socket = Net.createConnection(self.connectionBrokerPort, self.brokerServer);
  self._socket.setNoDelay(true);

  var packetWriter = new PacketWriter();
  var openDatabasePacket = new OpenDatabasePacket(
    {
      database : self.database,
      user     : self.user,
      password : self.password,
      casInfo  : self._CASInfo
    }
  );
  openDatabasePacket.write(packetWriter);
  self._socket.write(packetWriter._buffer);

  self._socket.on('error', function (err) {
    this.connectionOpened = false;
    delete this._queriesHandleList;
    callback.call(self, err);
  });

  self._socket.on('data', function (data) {
    responseData = Helpers._combineData(responseData, data);
    if (expectedResponseLength === self._INVALID_RESPONSE_LENGTH
      && responseData.length >= DATA_TYPES.DATA_LENGTH_SIZEOF) {
      expectedResponseLength = Helpers._getExpectedResponseLength(responseData);
    }
    if (responseData.length === expectedResponseLength) {
      self._socket.removeAllListeners('data');
      var packetReader = new PacketReader();
      packetReader.write(responseData);
      openDatabasePacket.parse(packetReader);
      self._CASInfo = openDatabasePacket.casInfo;
      var errorCode = openDatabasePacket.errorCode;
      var errorMsg = openDatabasePacket.errorMsg;
      if (errorCode !== 0) {
        err = new Error(errorCode + ':' + errorMsg);
      } else {
        self.sessionId = openDatabasePacket.sessionId;
        self.autoCommitMode = (self._CASInfo[3] & 0x01) ? self.AUTOCOMMIT_ON : self.AUTOCOMMIT_OFF;
      }
      callback.call(self, err);
    }
  });
};

/**
 * Connect to database
 * @param callback
 */
CUBRIDConnection.prototype.connect = function (callback) {
  var self = this;

  if (self.connectionPending == true) {
    var err = new Error(ErrorMessages.ERROR_CONNECTION_ALREADY_PENDING);
    Helpers._emitEvent(self, err, self.EVENT_ERROR, null);
    if (callback && typeof(callback) === 'function') {
      callback.call(self, err);
    }
    return;
  }

  self.connectionPending = true;

  ActionQueue.enqueue(
    [
      function (cb) {
        self._doGetBrokerPort(self, cb);
      },

      function (cb) {
        self._doDatabaseLogin(self, cb);
      }
    ],

    function (err) {
      self.queryPending = false; //reset query execution status
      self.connectionPending = false;
      if (typeof err != 'undefined' && err != null) {
        self.connectionOpened = false;
      } else {
        self.connectionOpened = true;
      }
      Helpers._emitEvent(self, err, self.EVENT_ERROR, self.EVENT_CONNECTED);
      if (callback && typeof(callback) === 'function') {
        callback.call(self, err);
      }
    }
  );
};

/**
 * Connect to database using an URL-type connection string
 * @param callback
 */
CUBRIDConnection.prototype.connectWithURL = function (properties, callback) {
  this.autoCommitMode = properties.autocommit || true;
  this.althosts = properties.althosts || null;
  this.login_timeout = properties.login_timeout || -1;
  this.query_timeout = properties.query_timeout || -1;
  this.disconnect_on_query_timeout = properties.disconnect_on_query_timeout || false;
  this.alternative_hosts = properties.alternative_hosts || '';
  this.time = properties.time || -1;
  this.milli_sec = properties.milli_sec || -1;
  this.autocommit = properties.autocommit || true;

  var self = this;

  //TODO
  //...

  if (callback && typeof(callback) === 'function') {
    callback.call(self, new Error('Not implemented yet'));
  }
};

/**
 * Get the server database engine version
 * @param callback
 */
CUBRIDConnection.prototype.getEngineVersion = function (callback) {
  var err = null;
  var engineVersion = '';
  var self = this;
  var responseData = new Buffer(0);
  var expectedResponseLength = this._INVALID_RESPONSE_LENGTH;

  ActionQueue.enqueue(
    [
      function (cb) {
        if (self.connectionOpened === false) {
          self.connect(cb);
        } else {
          cb();
        }
      },

      function (cb) {
        var packetWriter = new PacketWriter();
        var getEngineVersionPacket = new GetEngineVersionPacket(
          {
            casInfo : self._CASInfo
          }
        );

        getEngineVersionPacket.write(packetWriter);
        self._socket.write(packetWriter._buffer);

        self._socket.on('data', function (data) {
          responseData = Helpers._combineData(responseData, data);
          if (expectedResponseLength === self._INVALID_RESPONSE_LENGTH
            && responseData.length >= DATA_TYPES.DATA_LENGTH_SIZEOF) {
            expectedResponseLength = Helpers._getExpectedResponseLength(responseData);
          }
          if (responseData.length === expectedResponseLength) {
            self._socket.removeAllListeners('data');
            var packetReader = new PacketReader();
            packetReader.write(data);
            getEngineVersionPacket.parse(packetReader);
            var errorCode = getEngineVersionPacket.errorCode;
            var errorMsg = getEngineVersionPacket.errorMsg;
            if (errorCode !== 0) {
              err = new Error(errorCode + ':' + errorMsg);
            } else {
              engineVersion = getEngineVersionPacket.engineVersion;
            }
            if (cb && typeof(cb) === 'function') {
              cb.call(self, err, engineVersion);
            }
          }
        });
      }
    ],

    function (err, engineVersion) {
      Helpers._emitEvent(self, err, self.EVENT_ERROR, self.EVENT_ENGINE_VERSION_AVAILABLE, engineVersion);
      if (callback && typeof(callback) === 'function') {
        callback.call(self, err, engineVersion);
      }
    }
  );
};

/**
 * Execute SQL statements in batch mode
 * @param sqls
 * @param callback
 */
CUBRIDConnection.prototype.batchExecuteNoQuery = function (sqls, callback) {
  var self = this;
  var sqlsArr = null;
  var err = null;

  if (Array.isArray(sqls)) {
    if (sqls.length == 0) {
      Helpers._emitEvent(self, null, null, self.EVENT_BATCH_COMMANDS_COMPLETED);
      if (callback && typeof(callback) === 'function') {
        callback.call(this, null);
      }
      return;
    }
    sqlsArr = sqls;
  } else {
    sqlsArr = new Array(sqls);
  }

  for (var i = 0; i < sqlsArr.length; i++) {
    if (!Helpers._validateInputSQLString(sqlsArr[i])) {
      err = new Error(ErrorMessages.ERROR_INPUT_VALIDATION);
      Helpers._emitEvent(self, err, self.EVENT_ERROR, null);
      if (callback && typeof(callback) === 'function') {
        callback.call(self, err);
      }
      return;
    }
  }

  var responseData = new Buffer(0);
  var expectedResponseLength = this._INVALID_RESPONSE_LENGTH;

  ActionQueue.enqueue(
    [
      function (cb) {
        if (self.connectionOpened === false) {
          self.connect(cb);
        } else {
          cb();
        }
      },

      function (cb) {
        var packetWriter = new PacketWriter();
        var batchExecuteNoQueryPacket = new BatchExecuteNoQueryPacket(
          {
            SQLs           : sqlsArr,
            casInfo        : self._CASInfo,
            autoCommitMode : self.autoCommitMode
          }
        );
        batchExecuteNoQueryPacket.write(packetWriter);
        self._socket.write(packetWriter._buffer);

        self._socket.on('data', function (data) {
          responseData = Helpers._combineData(responseData, data);
          if (expectedResponseLength === self._INVALID_RESPONSE_LENGTH
            && responseData.length >= DATA_TYPES.DATA_LENGTH_SIZEOF) {
            expectedResponseLength = Helpers._getExpectedResponseLength(responseData);
          }
          if (responseData.length === expectedResponseLength) {
            self._socket.removeAllListeners('data');
            var packetReader = new PacketReader();
            packetReader.write(data);
            batchExecuteNoQueryPacket.parse(packetReader);
            var errorCode = batchExecuteNoQueryPacket.errorCode;
            var errorMsg = batchExecuteNoQueryPacket.errorMsg;
            if (errorCode !== 0) {
              err = new Error(errorCode + ':' + errorMsg);
            }
            if (cb && typeof(cb) === 'function') {
              cb.call(self, err);
            }
          }
        });
      }
    ],

    function (err) {
      Helpers._emitEvent(self, err, self.EVENT_ERROR, self.EVENT_BATCH_COMMANDS_COMPLETED);
      if (callback && typeof(callback) === 'function') {
        callback.call(self, err);
      }
    }
  );
};

/**
 * Execute query and retrieve rows results
 * @param sql
 * @param callback
 */
CUBRIDConnection.prototype.query = function (sql, callback) {
  var err = null;
  var self = this;

  if (self.queryPending == true) {
    err = new Error(ErrorMessages.ERROR_QUERY_ALREADY_PENDING);
    Helpers._emitEvent(self, err, self.EVENT_ERROR, null);
    if (callback && typeof(callback) === 'function') {
      callback.call(self, err);
    }
    return;
  }

  if (!Helpers._validateInputSQLString(sql)) {
    Helpers._emitEvent(self, new Error(ErrorMessages.ERROR_INPUT_VALIDATION), self.EVENT_ERROR, null);
    if (callback && typeof(callback) === 'function') {
      callback.call(self, err);
    }
    return;
  }

  var responseData = new Buffer(0);
  var expectedResponseLength = this._INVALID_RESPONSE_LENGTH;

  self.queryPending = true;

  ActionQueue.enqueue(
    [
      function (cb) {
        if (self.connectionOpened === false) {
          self.connect(cb);
        } else {
          cb();
        }
      },

      function (cb) {
        // Check if data is already in cache
        if (self._queryCache != null) {
          if (self._queryCache.contains(sql)) {
            self.queryPending = false;
            //query handle set to null, to prevent further fetch (cache is intended only for small data)
            Helpers._emitEvent(self, null, null, self.EVENT_QUERY_DATA_AVAILABLE, self._queryCache.get(sql), null);
            if (callback && typeof(callback) === 'function') {
              callback(err, self._queryCache.get(sql), null);
            }
            return;
          }
        }

        var packetWriter = new PacketWriter();
        var executeQueryPacket = new ExecuteQueryPacket(
          {
            sql            : sql,
            casInfo        : self._CASInfo,
            autoCommitMode : self.autoCommitMode
          }
        );
        executeQueryPacket.write(packetWriter);
        self._socket.write(packetWriter._buffer);

        self._socket.on('data', function (data) {
          responseData = Helpers._combineData(responseData, data);
          if (expectedResponseLength === self._INVALID_RESPONSE_LENGTH
            && responseData.length >= DATA_TYPES.DATA_LENGTH_SIZEOF) {
            expectedResponseLength = Helpers._getExpectedResponseLength(responseData);
          }
          if (responseData.length === expectedResponseLength) {
            self._socket.removeAllListeners('data');
            var packetReader = new PacketReader();
            packetReader.write(responseData);
            var result = executeQueryPacket.parse(packetReader);
            var errorCode = executeQueryPacket.errorCode;
            var errorMsg = executeQueryPacket.errorMsg;
            if (errorCode !== 0) {
              err = new Error(errorCode + ':' + errorMsg);
            } else {
              self._queriesHandleList.push(executeQueryPacket);
            }
            if (cb && typeof(cb) === 'function') {
              if (typeof err != 'undefined' && err != null) {
                self.queryPending = false;
              } else {
                if (self._queryCache !== null) {
                  self._queryCache.getSet(sql, result);
                }
              }
              cb.call(self, err, result, executeQueryPacket.handle);
            }
          }
        });
      }
    ],

    function (err, result, handle) {
      if (typeof err != 'undefined' && err != null) {
        self.queryPending = false;
      }
      Helpers._emitEvent(self, err, self.EVENT_ERROR, self.EVENT_QUERY_DATA_AVAILABLE, result, handle);
      if (callback && typeof(callback) === 'function') {
        callback.call(self, err, result, handle);
      }
    }
  );
};

/**
 * Execute query with parameters
 * @param sql
 * @param arrParamsValues
 * @param callback
 * @return {*}
 */
CUBRIDConnection.prototype.queryWithParams = function (sql, arrParamsValues, callback) {
  var formattedSQL = Helpers._sqlFormat(sql, arrParamsValues);

  return this.query(formattedSQL, callback);
};

/**
 * Fetch query next rows results
 * @param queryHandle
 * @param callback
 */
CUBRIDConnection.prototype.fetch = function (queryHandle, callback) {
  var err = null;
  var self = this;
  var responseData = new Buffer(0);
  var expectedResponseLength = this._INVALID_RESPONSE_LENGTH;

  self._socket.on('data', function (data) {
    responseData = Helpers._combineData(responseData, data);
    if (expectedResponseLength === self._INVALID_RESPONSE_LENGTH
      && responseData.length >= DATA_TYPES.DATA_LENGTH_SIZEOF) {
      expectedResponseLength = Helpers._getExpectedResponseLength(responseData);
    }
    if (responseData.length === expectedResponseLength) {
      self._socket.removeAllListeners('data');
      var packetReader = new PacketReader();
      packetReader.write(responseData);
      var result = fetchPacket.parse(packetReader, self._queriesHandleList[i]);
      var errorCode = fetchPacket.errorCode;
      var errorMsg = fetchPacket.errorMsg;
      if (errorCode !== 0) {
        err = new Error(errorCode + ':' + errorMsg);
      }
      Helpers._emitEvent(self, err, self.EVENT_ERROR, self.EVENT_FETCH_DATA_AVAILABLE, result);
      if (callback && typeof(callback) === 'function') {
        callback.call(self, err, result);
      }
    }
  });

  var foundQueryHandle = false;
  for (var i = 0; i < this._queriesHandleList.length; i++) {
    if (this._queriesHandleList[i].handle === queryHandle) {
      foundQueryHandle = true;
      break;
    }
  }

  if (!foundQueryHandle) {
    err = new Error(ErrorMessages.ERROR_NO_ACTIVE_QUERY);
    self._socket.removeAllListeners('data'); //TODO ???
    Helpers._emitEvent(self, err, self.EVENT_ERROR, null);
    if (callback && typeof(callback) === 'function') {
      callback.call(self, err, null);
    }
  } else {
    if (this._queriesHandleList[i].currentTupleCount === this._queriesHandleList[i].totalTupleCount) {
      self._socket.removeAllListeners('data');
      Helpers._emitEvent(self, null, null, self.EVENT_FETCH_NO_MORE_DATA_AVAILABLE);
      if (callback && typeof(callback) === 'function') {
        callback.call(self, err, null);
      }
    } else {
      var packetWriter = new PacketWriter();
      var fetchPacket = new FetchPacket(
        {
          casInfo : self._CASInfo
        }
      );
      fetchPacket.write(packetWriter, this._queriesHandleList[i]); //TODO Verify this
      self._socket.write(packetWriter._buffer);
    }
  }
};

/**
 * Close query
 * @param queryHandle
 * @param callback
 */
CUBRIDConnection.prototype.closeQuery = function (queryHandle, callback) {
  var err = null;
  var self = this;
  var responseData = new Buffer(0);
  var expectedResponseLength = this._INVALID_RESPONSE_LENGTH;

  if (!Helpers._validateInputTimeout(queryHandle)) {
    Helpers._emitEvent(self, new Error(ErrorMessages.ERROR_INPUT_VALIDATION), self.EVENT_ERROR, null);
    if (callback && typeof(callback) === 'function') {
      callback.call(self, err);
    }
    return;
  }

  self.queryPending = false;

  var packetWriter = new PacketWriter();
  var closeQueryPacket = new CloseQueryPacket(
    {
      casInfo   : self._CASInfo,
      reqHandle : queryHandle
    }
  );

  for (var i = 0; i < this._queriesHandleList.length; i++) {
    if (this._queriesHandleList[i].handle === queryHandle) {
      //TODO Remove handle ONLY if packet was executed ok
      this._queriesHandleList.splice(i, 1);
      break;
    }
  }

  //TODO Test if query was found! (same as in fetch)

  closeQueryPacket.write(packetWriter);
  self._socket.write(packetWriter._buffer);

  self._socket.on('data', function (data) {
    responseData = Helpers._combineData(responseData, data);
    if (expectedResponseLength === self._INVALID_RESPONSE_LENGTH
      && responseData.length >= DATA_TYPES.DATA_LENGTH_SIZEOF) {
      expectedResponseLength = Helpers._getExpectedResponseLength(responseData);
    }
    if (responseData.length === expectedResponseLength) {
      self._socket.removeAllListeners('data');
      var packetReader = new PacketReader();
      packetReader.write(data);
      closeQueryPacket.parse(packetReader);
      var errorCode = closeQueryPacket.errorCode;
      var errorMsg = closeQueryPacket.errorMsg;
      if (errorCode !== 0) {
        err = new Error(errorCode + ':' + errorMsg);
      }
      Helpers._emitEvent(self, err, self.EVENT_ERROR, self.EVENT_QUERY_CLOSED, queryHandle);
      if (callback && typeof(callback) === 'function') {
        callback.call(self, err);
      }
    }
  });
};

/**
 * Close connection
 * @param callback
 */
CUBRIDConnection.prototype.close = function (callback) {
  var err = null;
  var self = this;
  var responseData = new Buffer(0);
  var expectedResponseLength = this._INVALID_RESPONSE_LENGTH;

  //reset status
  self.queryPending = false;
  self.connectionPending = false;
  self.connectionOpened = false;

  ActionQueue.enqueue(
    [
      function (cb) {
        ActionQueue.while(
          function () {
            return (self._queriesHandleList[0] !== null && self._queriesHandleList[0] !== undefined);
          },

          function (callb) {
            self.closeQuery(self._queriesHandleList[0].handle, callb);
          },

          function (err) {
            //log non-blocking error
            if (typeof err != 'undefined' && err != null) {
              Helpers.logError(ErrorMessages.ERROR_ON_CLOSE_QUERY_HANDLE + err);
            }
            cb.call(null);
          }
        );
      },

      function (cb) {
        var packetWriter = new PacketWriter();
        var closeDatabasePacket = new CloseDatabasePacket(
          {
            casInfo : self._CASInfo
          }
        );
        closeDatabasePacket.write(packetWriter);
        self._socket.write(packetWriter._buffer);

        self._socket.on('data', function (data) {
          responseData = Helpers._combineData(responseData, data);
          if (expectedResponseLength === self._INVALID_RESPONSE_LENGTH
            && responseData.length >= DATA_TYPES.DATA_LENGTH_SIZEOF) {
            expectedResponseLength = Helpers._getExpectedResponseLength(responseData);
          }
          if (responseData.length === expectedResponseLength) {
            self._socket.removeAllListeners('data');
            var packetReader = new PacketReader();
            packetReader.write(data);
            closeDatabasePacket.parse(packetReader);
            // Close internal socket connection
            self._socket.destroy();
            var errorCode = closeDatabasePacket.errorCode;
            var errorMsg = closeDatabasePacket.errorMsg;
            if (errorCode !== 0) {
              err = new Error(errorCode + ':' + errorMsg);
            }
            if (cb && typeof(cb) === 'function') {
              cb.call(self, err);
            }
          }
        });
      }
    ],

    function (err) {
      Helpers._emitEvent(self, err, self.EVENT_ERROR, self.EVENT_CONNECTION_CLOSED);
      if (callback && typeof(callback) === 'function') {
        callback.call(self, err);
      }
    }
  );
};

/**
 * Start transaction
 * @param callback
 */
CUBRIDConnection.prototype.beginTransaction = function (callback) {
  var self = this;
  _toggleAutoCommitMode(self, self.AUTOCOMMIT_OFF, function (err) {
    Helpers._emitEvent(self, err, self.EVENT_ERROR, self.EVENT_BEGIN_TRANSACTION);
    if (callback && typeof(callback) === 'function') {
      callback.call(self, err);
    }
  });
};

/**
 * Set session auto-commit mode
 * @param autoCommitMode
 * @param callback
 */
CUBRIDConnection.prototype.setAutoCommitMode = function (autoCommitMode, callback) {
  var self = this;
  _toggleAutoCommitMode(self, autoCommitMode, function (err) {
    Helpers._emitEvent(self, err, self.EVENT_ERROR, self.EVENT_SET_AUTOCOMMIT_MODE_COMPLETED);
    if (callback && typeof(callback) === 'function') {
      callback.call(self, err);
    }
  });
};

/**
 * Rollback transaction
 * @param callback
 */
CUBRIDConnection.prototype.rollback = function (callback) {
  var err = null;
  var self = this;
  var responseData = new Buffer(0);
  var expectedResponseLength = this._INVALID_RESPONSE_LENGTH;

  if (self.autoCommitMode === false) {
    var packetWriter = new PacketWriter();
    var rollbackPacket = new RollbackPacket(
      {
        casInfo : self._CASInfo
      }
    );
    rollbackPacket.write(packetWriter);
    self._socket.write(packetWriter._buffer);
  } else {
    self._socket.removeAllListeners('data');
    Helpers._emitEvent(self, err, self.EVENT_ERROR, null);
    if (callback && typeof(callback) === 'function') {
      callback.call(self, err);
    }
  }

  self._socket.on('data', function (data) {
    responseData = Helpers._combineData(responseData, data);
    if (expectedResponseLength === self._INVALID_RESPONSE_LENGTH
      && responseData.length >= DATA_TYPES.DATA_LENGTH_SIZEOF) {
      expectedResponseLength = Helpers._getExpectedResponseLength(responseData);
    }
    if (responseData.length === expectedResponseLength) {
      self._socket.removeAllListeners('data');
      var packetReader = new PacketReader();
      packetReader.write(data);
      rollbackPacket.parse(packetReader);
      var errorCode = rollbackPacket.errorCode;
      var errorMsg = rollbackPacket.errorMsg;
      if (errorCode !== 0) {
        err = new Error(errorCode + ':' + errorMsg);
      }
      Helpers._emitEvent(self, err, self.EVENT_ERROR, self.EVENT_ROLLBACK_COMPLETED);
      if (callback && typeof(callback) === 'function') {
        callback.call(self, err);
      }
    }
  });
};

/**
 * Commit transaction
 * @param callback
 */
CUBRIDConnection.prototype.commit = function (callback) {
  var err = null;
  var self = this;
  var responseData = new Buffer(0);
  var expectedResponseLength = this._INVALID_RESPONSE_LENGTH;

  if (self.autoCommitMode === false) {
    var packetWriter = new PacketWriter();
    var commitPacket = new CommitPacket(
      {
        casInfo : self._CASInfo
      }
    );
    commitPacket.write(packetWriter);
    self._socket.write(packetWriter._buffer);
  } else {
    self._socket.removeAllListeners('data');
    Helpers._emitEvent(self, err, self.EVENT_ERROR, null);
    if (callback && typeof(callback) === 'function') {
      callback.call(self, err);
    }
  }

  self._socket.on('data', function (data) {
    responseData = Helpers._combineData(responseData, data);
    if (expectedResponseLength === self._INVALID_RESPONSE_LENGTH
      && responseData.length >= DATA_TYPES.DATA_LENGTH_SIZEOF) {
      expectedResponseLength = Helpers._getExpectedResponseLength(responseData);
    }
    if (responseData.length === expectedResponseLength) {
      self._socket.removeAllListeners('data');
      var packetReader = new PacketReader();
      packetReader.write(data);
      commitPacket.parse(packetReader);
      var errorCode = commitPacket.errorCode;
      var errorMsg = commitPacket.errorMsg;
      if (errorCode !== 0) {
        err = new Error(errorCode + ':' + errorMsg);
      }
      Helpers._emitEvent(self, err, self.EVENT_ERROR, self.EVENT_COMMIT_COMPLETED);
      if (callback && typeof(callback) === 'function') {
        callback.call(self, err);
      }
    }
  });
};

/**
 * Autocommit mode helper
 * @param self
 * @param autoCommitMode
 * @param callback
 * @private
 */
function _toggleAutoCommitMode(self, autoCommitMode, callback) {
  var err = null;
  var responseData = new Buffer(0);
  var expectedResponseLength = self._INVALID_RESPONSE_LENGTH;

  if (!Helpers._validateInputBoolean(autoCommitMode)) {
    Helpers._emitEvent(self, new Error(ErrorMessages.ERROR_INPUT_VALIDATION), self.EVENT_ERROR, null);
    if (callback && typeof(callback) === 'function') {
      callback.call(self, err);
    }
    return;
  }

  if (self.autoCommitMode === autoCommitMode) {
    if (callback && typeof(callback) === 'function') {
      callback.call(self, err);
      return;
    }
  }

  var packetWriter = new PacketWriter();
  var setAutoCommitModePacket = new SetAutoCommitModePacket(
    {
      casInfo        : self._CASInfo,
      autoCommitMode : autoCommitMode
    }
  );
  setAutoCommitModePacket.write(packetWriter);
  self._socket.write(packetWriter._buffer);

  self._socket.on('data', function (data) {
    responseData = Helpers._combineData(responseData, data);
    if (expectedResponseLength === -1 && responseData.length >= DATA_TYPES.DATA_LENGTH_SIZEOF) {
      expectedResponseLength = Helpers._getExpectedResponseLength(responseData);
    }
    if (responseData.length === expectedResponseLength) {
      self._socket.removeAllListeners('data');
      var packetReader = new PacketReader();
      packetReader.write(data);
      setAutoCommitModePacket.parse(packetReader);
      var errorCode = setAutoCommitModePacket.errorCode;
      var errorMsg = setAutoCommitModePacket.errorMsg;
      if (errorCode !== 0) {
        err = new Error(errorCode + ':' + errorMsg);
      } else {
        self.autoCommitMode = autoCommitMode;
      }

      if (callback && typeof(callback) === 'function') {
        callback.call(self, err);
      }
    }
  });
}

/**
 * Get databases schema information
 * @param schemaType
 * @param result
 * @param callback
 */
CUBRIDConnection.prototype.getSchema = function (schemaType, result, callback) {
  var self = this;

  //TODO
  //...

  if (callback && typeof(callback) === 'function') {
    callback.call(self, new Error('Not implemented yet'));
  }
};

