const createArchive = require('./utils/createArchive');
const getBuffers = require('./utils/getBuffers');
const events = require('events').EventEmitter;
const util = require('util');

const downloader = function (opts) {
  opts = opts || {};

  if (!opts.client) throw Error('s3Torrent: client option required (aws-sdk S3 client instance)');
  if (!opts.bucket) throw Error('s3Torrent: bucked option is required');

  this.s3 = opts.client;
  this.bucket = opts.bucket;
  this.concurrency = opts.concurrency || 6;

  this.getBuffers = getBuffers;
  this.createArchive = createArchive;
};

util.inherits(downloader, events);

module.exports = downloader;
