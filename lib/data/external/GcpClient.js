const { errors } = require('arsenal');
const GCP = require('@google-cloud/storage');
const queryString = require('query-string');
const createLogger = require('../multipleBackendLogger');
const logHelper = require('./utils').logHelper;

class GcpClient {
    /**
    * @constructor
    * @param {Object} config - config
    */
    constructor(config) {
        this._gcpEndpoint = config.gcpEndpoint;
        this._gcpParams = config.gcpParams;
        this._gcpBucketName = config.gcpBucketName;
        this._bucketMatch = config.bucketMatch;
        this._dataStoreName = config.dataStoreName;
        this._client = new GCP(this._gcpParams);
    }

    /**
    * Create key for object in bucket
    * @param {string} requestBucketName - requested bucket name
    * @param {string} requestObjectKey - requested object key
    * @param {boolean} bucketmatch - bucket matching
    * @return {string}
    */
    _createGcpKey(requestBucketName, requestObjectKey, bucketMatch) {
        if (bucketMatch) {
            return requestObjectKey;
        }
        return `${requestBucketName}/${requestObjectKey}`;
    }

    /**
    * Utility function to create GCP file object based on object key
    * @param {string} objectKey - name of the requested object
    * @return {object}
    */
    _callFile(objectKey) {
        const bucket = this._client.bucket(this._gcpBucketName);
        const file = bucket.file(objectKey);
        return file;
    }

    /**
    * Utility function to create GCP metadata object from header and tags
    * @param {string} metaHeaders - metadata header
    * @param {string} tags - tags
    * @return {object}
    */
    _translateMetaHeaders(metaHeaders, tags) {
        const translatedMetaHeaders = {};
        if (tags) {
            const tagObj = queryString.parse(tags);
            Object.keys(tagObj).forEach(tagName => {
                translatedMetaHeaders[tagName] = tagObj[tagName];
            });
        }
        Object.keys(metaHeaders).forEach(headerName => {
            const translated = headerName.replace('x-amz-meta-', '');
            translatedMetaHeaders[translated] = metaHeaders[headerName];
        });
        return translatedMetaHeaders;
    }

    /**
    * Utility function that retreives headers with metadata
    * @param {object} objectMD - object metadata
    * @return {object}
    */
    _getMetaHeaders(objectMD) {
        const metaHeaders = {};
        Object.keys(objectMD).forEach(mdKey => {
            const isMetaHeader = mdKey.startsWith('x-amz-meta-');
            if (isMetaHeader) {
                metaHeaders[mdKey] = objectMD[mdKey];
            }
        });
        return this._translateMetaHeaders(metaHeaders);
    }

    /**
    * Utility function that processes and generates a metadata object for use
    * with GCP metadata editing
    * @param {object} metaHeaders - object metadata
    * @return {object}
    */
    _setMetaHeaders(metaHeaders) {
        const translatedMetaHeaders = {};
        Object.keys(metaHeaders).forEach(headerName => {
            if (!metaHeaders[headerName] &&
                typeof(metaHeaders[headerName]) === 'string') {
                translatedMetaHeaders[headerName] = null;
            } else {
                translatedMetaHeaders[headerName] = metaHeaders[headerName];
            }
        });
        return translatedMetaHeaders;
    }

    /**
    * Puts object to Google cloud bucket
    * @param {stream} stream - data stream
    * @param {number} size - size of stream
    * @param {object} keyContext - information about the requested object
    * @param {number} reqUids - request id
    * @param {callback} callback - callback function
    * @return {undefined}
    */
    put(stream, size, keyContext, reqUids, callback) {
        const gcpKey = this._createGcpKey(keyContext.bucketName,
            keyContext.objectKey, this._bucketMatch);
        const file = this._callFile(gcpKey);
        const options = {
            metadata: {
                metadata: this._translateMetaHeaders(keyContext.metaHeaders,
                    keyContext.tagging),
            },
        };
        stream.pipe(file.createWriteStream(options))
        .on('error', err => {
            const log = createLogger(reqUids);
            logHelper(log, 'error', 'err from GCP PUT data backend',
                err, this._dataStoreName);
            return callback(errors.InternalError
                .customizeDescription('Error returned from ' +
                `GCP: ${err.message}`)
            );
        }).on('finish', () => {
            callback(null, gcpKey);
        });
    }

    /**
    * Gets object from Google cloud bucket
    * @param {object} objectGetInfo - object information
    * @param {number} range - byte range
    * @param {number} reqUids - request id
    * @param {callback} callback - callback
    * @return {undefined}
    */
    get(objectGetInfo, range, reqUids, callback) {
        const key = objectGetInfo.key;
        const file = this._callFile(key);
        const stream = file.createReadStream().on('error', err => {
            const log = createLogger(reqUids);
            logHelper(log, 'error', 'err from GCP GET data backend',
                err, this._dataStoreName);
            return callback(errors.InternalError);
        });
        return callback(null, stream);
    }

    /**
    * Deletes object from Google cloud bucket
    * @param {object} objectGetInfo - object information
    * @param {number} reqUids - request id
    * @param {callback} callback - callback
    * @return {undefined}
    */
    delete(objectGetInfo, reqUids, callback) {
        const key = objectGetInfo.key;
        const file = this._callFile(key);
        return file.delete(err => {
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

    /**
    * Checks health of connection to Google cloud bucket
    * @param {string} location - location constraint id
    * @param {callback} callback - callback
    * @return {undefined}
    */
    checkGcpHealth(location, callback) {
        const gcpResp = {};
        const bucket = this._client.bucket(this._gcpBucketName);
        this._client.exist(bucket, err => {
            if (err) {
                gcpResp[location] = {
                    error: err.message,
                };
                return callback(null, gcpResp);
            }
            gcpResp[location] = {
                message: 'Congrats! You own the bucket',
            };
            return callback(null, gcpResp);
        });
    }

    /**
    * Function that sets tags for put objects
    * @param {string} key - object name
    * @param {string} bucket - object path
    * @param {object} objectMD - object metadata
    * @param {string} log - log
    * @param {callback} callback - callback
    * @return {undefined}
    */
    objectPutTagging(key, bucket, objectMD, log, callback) {
        const gcpKey = this._createGcpKey(bucket, key);
        const gcpFile = this._callFile(gcpKey);
        const metaHeaders = this._getMetaHeaders(objectMD);
        const gcpMD = {
            metadata: this._setMetaHeaders(metaHeaders),
        };
        gcpFile.setMetadata(gcpMD, err => {
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

    /**
    * Function that sets tags for deleted objects
    * @param {string} key - object name
    * @param {string} bucket - object path
    * @param {object} objectMD - object metadata
    * @param {string} log - log
    * @param {callback} callback - callback
    * @return {undefined}
    */
    objectDeleteTagging(key, bucket, objectMD, log, callback) {
        const gcpKey = this._createGcpKey(bucket, key);
        const gcpFile = this._callFile(gcpKey);
        const metaHeaders = this._getMetaHeaders(objectMD);
        const gcpMD = {
            metadata: this._setMetaHeaders(metaHeaders),
        };
        gcpFile.setMetadata(gcpMD, err => {
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
