'use strict';

const path = require('path');
const rootDir = path.resolve(__dirname, '..', '..');
const CUBRID = require(rootDir);
const ConsoleLogger = require(path.join(rootDir, 'src', 'ConsoleLogger'));

const config = {
  hosts: ['10.105.172.145'],
  port: 30102,
  user: 'nsight',
  password: 'ns0)3#ht',
  database: 'manager_master',
  maxConnectionRetryCount: 1,
  logger: new ConsoleLogger,
};

exports.config = config;

function createDefaultCUBRIDDemodbConnection() {
  return new CUBRID.createCUBRIDConnection(config);
}

exports.cleanup = function (tableName) {
  return function cleanup() {
    let client = createDefaultCUBRIDDemodbConnection();

    this.timeout(5000);
    
    return client
        .execute(`DROP TABLE IF EXISTS ${tableName}`)
        .then(() => {
          return client.close();
        });
  };
};

exports.createDefaultCUBRIDDemodbConnection = createDefaultCUBRIDDemodbConnection;
