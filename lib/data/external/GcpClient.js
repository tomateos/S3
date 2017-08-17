const { errors } = require('arsenal');
const GCP = require('google-cloud');
const createLogger = require('../multipleBackendLogger');
const logHelper = require('./utils').logHelper;

class GcpClient {
    constructor(config) {
        this._gcpEndpoint = config.gcpEndpoint;
        this._gcpParams = config.gcpParams;
        this._gcpBucketName = config.gcpBucketName;
        this._bucketMatch = config.bucketMatch;
        this._dataStoreName = config.dataStoreName;
        this._client = new GCP.storage(this._gcpParams);
    }

    _createGcpKey(requestBucketName, requestObjectKey, bucketMatch) {
        if (bucketMatch) {
            return requestObjectKey;
        }
        return `${requestBucketName}/${requestObjectKey}`;
    }

    _setMetaHeaders(metaHeaders, tags) {
        const translatedMetaHeaders = {};
        Object.keys(metaHeaders).forEach(headerName => {
            const isMetaHeader = headerName.startsWith('x-amz-meta');
            if (isMetaHeader) {
                const translated = headerName.replace('x-amz-meta-', '');
                translatedMetaHeaders[translated] = metaHeaders[headerName];
            }
        });
        return translatedMetaHeaders;
    }

    _getMetaHeaders(objectMD) {
        const metaHeaders = {};
        Object.keys(objectMD).forEach(mdKey => {
            const isMetaHeader = mdKey.startsWith('x-amz-meta-');
            if (isMetaHeader) {
                metaHeaders[mdKey] = objectMD[mdKey];
            }
        });
        return this._setMetaHeaders(metaHeaders);
    }

    _translateMetaHeaders(metaHeaders) {
        Object.keys(metaHeaders).forEach(headerName => {
            if (!metaHeaders[headerName] &&
                typeof(metaHeaders[headerName]) === 'string') {
                metaHeaders[headerName] = null;
            }
        });
        return metaHeaders;
    }

    put(stream, size, keyContext, reqUids, callback) {
        const gcpKey = this._createGcpKey(keyContext.bucketName,
            keyContext.objectKey, this._bucketMatch);
        const bucket = this._client.bucket(this._gcpBucketName);
        const file = bucket.file(gcpKey);
        const options = {
            metadata: {
                // metadata: keyContext.metaHeaders
                metadata: this._setMetaHeaders(keyContext.metaHeaders)
            }
        }
        stream.pipe(file.createWriteStream(options))
        .on('error', err => {
            if (err) {
                const log = createLogger(reqUids);
                logHelper(log, 'error', 'err from GCP PUT data backend',
                    err, this._dataStoreName);
                return callback(errors.InternalError
                    .customizeDescription('Error returned from ' +
                    `GCP: ${err.message}`)
                );
            }
        }).on('finish', () => {
            return callback(null, gcpKey);
        });
    }

    get(objectGetInfo, range, reqUids, callback) {
        const key = typeof(objectGetInfo) === 'string' ? objectGetInfo :
            objectGetInfo.key;
        // const response = objectGetInfo.response;
        const bucket = this._client.bucket(this._gcpBucketName);
        const file = bucket.file(key);
        const stream = file.createReadStream().on('error', (err) => {
            const log = createLogger(reqUids);
            logHelper(log, 'error', 'err from GCP GET data backend',
                err, this._dataStoreName);
            return callback(errors.InternalError);
        });
        return callback(null, stream);
    }

    delete(objectGetInfo, reqUids, callback) {
        const key = typeof(objectGetInfo) === 'string' ? objectGetInfo :
            objectGetInfo.key;
        const bucket = this._client.bucket(this._gcpBucketName);
        const file = bucket.file(key);
        return file.delete((err, apiResponse) => {
            if (err) {
                const log = createLogger(reqUids);
                logHelper(log, 'error', 'error deleting object from ' +
                'GCP datastore', err, this._dataStoreName);
                return callback(errors.InternalError
                    .customizeDescription('Error returned from ' + 
                    `GCP: ${err.message}`)
                );
            }
            return callback();
        });
    }

    checkGcpHealth(location, callback) {
        const gcpResp = {};
        let bucket = this._client.bucket(this._gcpBucketName);
        this._client.exist(bucket, function(err, exists) {
            if (err) {
                gcpResp[location] = {
                    error: err.message
                };
                return callback(null, gcpResp);
            }
            gcpResp[location] = {
                message: 'Congrats! You own the bucket'
            };
            return callback(null, gcpResp);
        });
    }

    objectPutTagging(key, bucket, objectMD, log, callback) {
        const gcpKey = this._createGcpKey(bucket, key, this._bucketMatch);
        const gcpBucket = this._client.bucket(this._gcpBucketName);
        const gcpFile = gcpBucket.file(gcpKey);
        const metaHeaders = this._getMetaHeaders(objectMD);
        const gcpMD = {
            metadata: this._translateMetaHeaders(metaHeaders)
        }
        gcpFile.setMetadata(gcpMD, (err, apiResponse) => {
            if (err) {
                logHelper(log, 'error', 'error from data backend on ' +
                'putObjectTagging', err, this._dataStoreName);
                return callback(errors.InternalError
                    .customizeDescription('Error returend from ' +
                    `GCP: ${err.message}`)
                );
            }
            return callback();
        });
    }

    objectDeleteTagging(key, bucket, objectMD, log, callback) {
        const gcpKey = this._createGcpKey(bucket, key, this._bucketMatch);
        const gcpBucket = this._client.bucket(this._gcpBucketName);
        const gcpFile = gcpBucket.file(gcpKey);
        const metaHeaders = this._getMetaHeaders(objectMD);
        const gcpMD = {
            metadata: this._translateMetaHeaders(metaHeaders)
        }
        gcpFile.setMetadata(gcpMD, (err, apiResponse) => {
            if (err) {
                logHelper(log, 'error', 'error from data backend on ' +
                'deleteObjectTagging', err, this._dataStoreName);
                return callback(errors.InternalError
                    .customizeDescription('Error returend from ' +
                    `GCP: ${err.message}`)
                );
            }
            return callback();
        });
    }
}

module.exports = GcpClient;
