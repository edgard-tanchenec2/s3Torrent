const async = require('async');
const concat = require('concat-stream');
const partialDownloader = require('s3-download-stream');

const getBuffers = function(files, cb) {
  async.mapLimit(files, 20, (fileUrl, cb) => {
    fileUrl = fileUrl.substr(fileUrl.indexOf(this.bucket) + this.bucket.length + 1, fileUrl.length);

    const config = {
      client: this.s3,
      concurrency: this.concurrency,
      params: {
        Key: fileUrl,
        Bucket: this.bucket
      }
    };
    const s3File = partialDownloader(config);

    s3File
      .pipe(concat( (buffer) => cb(null, { buffer, fileUrl }) ));

    s3File.on('end', () => {
      console.log('downloaded - ' + fileUrl);
    });

    s3File.on('error', cb);
  }, cb);
};

module.exports = getBuffers;
