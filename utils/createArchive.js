const archiver = require('archiver');
const async = require('async');
const partialDownloader = require('s3-download-stream');

const isWriteStream = (stream) => {
  return stream !== null
    && typeof stream === 'object'
    && typeof stream.pipe === 'function'
    && stream.writable !== false
    && typeof stream._write === 'function'
    && typeof stream._writableState === 'object';
};

const createArchive = function(files, writeStream, opts) {
  if (!files || !files[0]) {
    throw Error('s3Torrent: First argument must be an array of files url');
  }

  if (!isWriteStream(writeStream)) {
    throw Error('s3Torrent: Second argument must be an write stream');
  }

  const archive = archiver('zip');

  archive.on('warning', (err) => {
    this.emit('warning', err);
  });
  archive.on('error', (err) => {
    this.emit('error', err);
  });
  archive.on('entry', (data) => {
    this.emit('entry', data);
  });
  archive.on('progress', (data) => {
    this.emit('progress', data);
  });

  archive.pipe(writeStream);

  async.eachLimit(files, 2, (fileUrl, cb) => {
    const filePath = fileUrl.substr(fileUrl.indexOf(this.bucket) + this.bucket.length + 1, fileUrl.length);
    const fileName = filePath.substr(filePath.lastIndexOf('/') + 1, filePath.length);
    const params = { Bucket: this.bucket, Key: filePath };
    this.s3.headObject(params, (err) => {
      if (err) {
        this.emit('failed', err, fileUrl);

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

      const s3File = partialDownloader(config);

      archive.append(s3File, { name: fileName });

      console.log('start downloading: ' + fileUrl);

      s3File.on('end', () => {
        console.log('downloaded: ' + fileUrl);
        cb();
      });

      s3File.on('error', (err) => {
        this.emit('failed', err, fileUrl);
      });
    });
  }, (err) => {
    if (err) {
      throw Error(err);
    }

    console.log('finished');
    archive.finalize();
  });
};

module.exports = createArchive;
