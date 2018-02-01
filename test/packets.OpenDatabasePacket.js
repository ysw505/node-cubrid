'use strict';

const expect = require('chai').expect;

const PacketReader = require('../src/packets/PacketReader');
const PacketWriter = require('../src/packets/PacketWriter');
const OpenDatabasePacket = require('../src/packets/OpenDatabasePacket');

describe('OpenDatabasePacket', function () {
  it('should succeed to verify the return value of write() and parse()', function () {

    //  test.expect(12);
      var packetReader = new PacketReader();
      var options = {database : 'manager_master', user : 'nsight', password : 'ns0)3#ht', logger: console};
      var openDatabasePacket = new OpenDatabasePacket(options);
      var packetWriter = new PacketWriter(openDatabasePacket.getBufferLength());

      openDatabasePacket.write(packetWriter);

      console.info( packetWriter._toBuffer() )
//      test.equal(packetWriter._toBuffer().slice(0, 6).toString(), options.database);
//      test.equal(packetWriter._toBuffer().slice(32, 38).toString(), options.user);
//      test.equal(packetWriter._toBuffer().slice(64, 65)[0], 0);
//
      packetReader.write(new Buffer([0, 0, 0, 15,
          0, 255, 255, 255,
          0, 0, 0, 0,
          5, 5, 5, 5, 5, 5, 5, 5,
          0, 0, 0, 3]));
      openDatabasePacket.parse(packetReader);


      expect(openDatabasePacket.casInfo[0]).to.equal(0); // Casinfo
      expect(openDatabasePacket.casInfo[1]).to.equal(255); // Casinfo
      expect(openDatabasePacket.casInfo[2]).to.equal(255); // Casinfo
      expect(openDatabasePacket.casInfo[3]).to.equal(255); // Casinfo

      expect(openDatabasePacket.responseCode).to.equal(0);
      //expect(openDatabasePacket.errorCode).to.equal(0);
      //expect(openDatabasePacket.errorMsg).to.equal('');
      expect(openDatabasePacket.brokerInfo.dbType).to.equal(5);
      expect(openDatabasePacket.sessionId).to.equal(3);
      
  });
});
