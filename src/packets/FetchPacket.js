var DATA_TYPES = require('../constants/DataTypes'),
  Helpers = require('../utils/Helpers'),
  ErrorMessages = require('../constants/ErrorMessages'),
  CAS = require('../constants/CASConstants');

module.exports = FetchPacket;

/**
 * Constructor
 * @param options
 * @constructor
 */
function FetchPacket(options) {
  this.casInfo = options.casInfo;

  this.responseCode = 0;
  this.errorCode = 0;
  this.errorMsg = '';
}

/**
 * Write data
 * @param writer
 */
FetchPacket.prototype.write = function (writer, queryHandle) {
  var bufferLength = DATA_TYPES.DATA_LENGTH_SIZEOF + DATA_TYPES.CAS_INFO_SIZE +
    DATA_TYPES.BYTE_SIZEOF + DATA_TYPES.INT_SIZEOF + DATA_TYPES.INT_SIZEOF +
    DATA_TYPES.INT_SIZEOF + DATA_TYPES.INT_SIZEOF + DATA_TYPES.INT_SIZEOF + DATA_TYPES.INT_SIZEOF +
    DATA_TYPES.INT_SIZEOF + DATA_TYPES.BYTE_SIZEOF + DATA_TYPES.INT_SIZEOF + DATA_TYPES.INT_SIZEOF;

  writer._writeInt(bufferLength - DATA_TYPES.DATA_LENGTH_SIZEOF - DATA_TYPES.CAS_INFO_SIZE);
  writer._writeBytes(DATA_TYPES.CAS_INFO_SIZE, this.casInfo);

  writer._writeByte(CAS.CASFunctionCode.CAS_FC_FETCH);
  writer._writeInt(DATA_TYPES.INT_SIZEOF); //int sizeof
  writer._writeInt(queryHandle.handle); //serverHandler
  writer._writeInt(DATA_TYPES.INT_SIZEOF); //int sizeof
  writer._writeInt(queryHandle.currentTupleCount + 1); //Start position (= current cursor position + 1)
  writer._writeInt(DATA_TYPES.INT_SIZEOF); //int sizeof
  writer._writeInt(100); //Fetch size; 0 = default; recommended = 100
  writer._writeInt(DATA_TYPES.BYTE_SIZEOF); //byte sizeof
  writer._writeByte(0); //Is case sensitive
  writer._writeInt(DATA_TYPES.INT_SIZEOF); //int sizeof
  writer._writeInt(0); //Is the ResultSet index...?

  return writer;
};

/**
 * Read data
 * @param parser
 * @param queryHandle
 */
FetchPacket.prototype.parse = function (parser, queryHandle) {
  var responseLength = parser._parseInt();
  this.casInfo = parser._parseBytes(DATA_TYPES.CAS_INFO_SIZE);

  this.responseCode = parser._parseInt();
  if (this.responseCode !== 0) {
    this.errorCode = parser._parseInt();
    this.errorMsg = parser._parseNullTerminatedString(responseLength - 2 * DATA_TYPES.INT_SIZEOF);
    if (this.errorMsg.length == 0) {
      this.errorMsg = Helpers._resolveErrorCode(this.errorCode);
    }
  } else {
    this.tupleCount = parser._parseInt();
    return JSON.stringify({ColumnValues : queryHandle._getData(parser, this.tupleCount)});
  }
};


