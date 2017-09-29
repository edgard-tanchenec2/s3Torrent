const async = require('async');
const AWS = require('aws-sdk');
const concat = require('concat-stream');
const archiver = require('archiver');
const s3Downloader = require('s3-download-stream');

const downloader = function (opts) {
  this.s3 = new AWS.S3({
    region: opts.region,
    accessKeyId: opts.accessKeyId,
    secretAccessKey: opts.secretAccessKey,
    signatureVersion: opts.signatureVersion,
    httpOptions: { timeout: 0 }
  });
  this.bucket = opts.bucket;
  this.concurrency = opts.concurrency;

  this.getBuffers = (files, cb) => {
    async.mapLimit(files, 5, (fileUrl, cb) => {
      fileUrl = fileUrl.substr(fileUrl.indexOf(this.bucket) + this.bucket.length + 1, fileUrl.length);

      const params = { Bucket: this.bucket, Key: fileUrl };
      const s3File = this.s3.getObject(params).createReadStream();

      s3File
        .pipe(concat( (buffer) => cb(null, { buffer, fileUrl }) ));

      s3File.on('end', () => {
        console.log('downloaded - ' + fileUrl);
      });

      s3File.on('error', cb);
    }, cb);
  };

  this.getArchived = (files, writeStream) => {
    const archive = archiver('zip');

    archive.on('warning', console.log);
    archive.on('error', console.log);
    archive.on('entry', console.log);
    archive.on('progress', console.log);

    archive.pipe(writeStream);

    async.eachLimit(files, 2, (fileUrl, cb) => {
      const filePath = fileUrl.substr(fileUrl.indexOf(this.bucket) + this.bucket.length + 1, fileUrl.length);
      const fileName = filePath.substr(filePath.lastIndexOf('/') + 1, filePath.length);
      const params = { Bucket: this.bucket, Key: filePath };
      this.s3.headObject(params, (err) => {
        if (err) {
          console.log(err);
          console.log(fileUrl);
          return cb();
        }

        const config = {
          client: this.s3,
          concurrency: this.concurrency,
          params: {
            Key: filePath,
            Bucket: this.bucket
          }
        };

        const s3File = s3Downloader(config);
        let error;

        archive.append(s3File, { name: fileName });

        console.log('start downloading: ' + fileUrl);

        s3File.on('end', () => {
          console.log('downloaded: ' + fileUrl);

          if (error) {
            console.log(error);
            console.log(fileUrl);
          }

          cb();
        });

        s3File.on('error', (err) => {
          error = err;
        });
      });
    }, (err) => {
      if (err) {
        return console.log(err);
      }

      console.log('finished');
      archive.finalize();
    });
  };
};

module.exports = downloader;
