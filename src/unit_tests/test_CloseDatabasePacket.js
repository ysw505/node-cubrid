var PacketReader = require('../packets/PacketReader'),
  PacketWriter = require('../packets/PacketWriter'),
  CloseDatabasePacket = require('../packets/CloseDatabasePacket'),
  CAS = require('../constants/CASConstants'),
  assert = require('assert');

function testCloseConnectionPacket_01() {
  var packetReader = new PacketReader();
  var packetWriter = new PacketWriter();
  var options = {casInfo : [0, 255, 255, 255]};
  var closeDatabasePacket = new CloseDatabasePacket(options);

  closeDatabasePacket.write(packetWriter);
  assert.equal(packetWriter._toBuffer()[3], 1); //total length

  assert.equal(packetWriter._toBuffer()[4], 0); //casInfo
  assert.equal(packetWriter._toBuffer()[5], 255); //casInfo
  assert.equal(packetWriter._toBuffer()[6], 255); //casInfo
  assert.equal(packetWriter._toBuffer()[7], 255); //casInfo

  assert.equal(packetWriter._toBuffer()[8], CAS.CASFunctionCode.CAS_FC_CON_CLOSE);

  packetReader.write(new Buffer([0, 0, 0, 0, 0, 255, 255, 255, 0, 0, 0, 0]));

  assert.equal(packetReader._packetLength(), 12);

  closeDatabasePacket.parse(packetReader);

  assert.equal(closeDatabasePacket.casInfo[0], 0); //casInfo
  assert.equal(closeDatabasePacket.casInfo[1], 255); //casInfo
  assert.equal(closeDatabasePacket.casInfo[2], 255); //casInfo
  assert.equal(closeDatabasePacket.casInfo[3], 255); //casInfo

  assert.equal(closeDatabasePacket.responseCode, 0);

  assert.equal(closeDatabasePacket.errorCode, 0);
  assert.equal(closeDatabasePacket.errorMsg, '');
}

console.log('Unit test ' + module.filename.toString() + ' started...');

testCloseConnectionPacket_01();

console.log('Unit test ended OK.');

