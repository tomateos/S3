const { errors, s3middleware } = require('arsenal');
const { parseTagFromQuery } = s3middleware.tagging;

const createLogger = require('./multipleBackendLogger');
const async = require('async');

const { config } = require('../Config');
const parseLC = require('./locationConstraintParser');

const clients = parseLC(config);

const multipleBackendGateway = {
    put: (hashedStream, size, keyContext,
     backendInfo, reqUids, callback) => {
        const controllingLocationConstraint =
            backendInfo.getControllingLocationConstraint();
        const client = clients[controllingLocationConstraint];
        if (!client) {
            const log = createLogger(reqUids);
            log.error('no data backend matching controlling locationConstraint',
            { controllingLocationConstraint });
            return process.nextTick(() => {
                callback(errors.InternalError);
            });
        }

        let writeStream = hashedStream;
        if (keyContext.cipherBundle && keyContext.cipherBundle.cipher) {
            writeStream = keyContext.cipherBundle.cipher;
            hashedStream.pipe(writeStream);
        }

        if (keyContext.tagging) {
            const validationTagRes = parseTagFromQuery(keyContext.tagging);
            if (validationTagRes instanceof Error) {
                const log = createLogger(reqUids);
                log.debug('tag validation failed', {
                    error: validationTagRes,
                    method: 'multipleBackendGateway put',
                });
                return callback(errors.InternalError);
            }
        }

        return client.put(writeStream, size, keyContext,
            reqUids, (err, key, eTag) => {
                const log = createLogger(reqUids);
                log.info('put to location', { controllingLocationConstraint });
                if (err) {
                    log.error('error from datastore',
                             { error: err, dataStoreType: client.clientType });
                    return callback(errors.InternalError);
                }
                const dataRetrievalInfo = {
                    key,
                    dataStoreName: controllingLocationConstraint,
                    dataStoreType: client.clientType,
                };
                if (eTag) {
                    dataRetrievalInfo.eTag = eTag;
                }
                return callback(null, dataRetrievalInfo);
            });
    },

    get: (objectGetInfo, range, reqUids, callback) => {
        let key;
        let client;
        // for backwards compatibility
        if (typeof objectGetInfo === 'string') {
            key = objectGetInfo;
            client = clients.legacy;
        } else {
            key = objectGetInfo.key;
            client = clients[objectGetInfo.dataStoreName];
        }
        if (client.clientType === 'scality') {
            return client.get(key, range, reqUids, callback);
        }
        return client.get(objectGetInfo, range, reqUids, callback);
    },

    delete: (objectGetInfo, reqUids, callback) => {
        let key;
        let client;
        // for backwards compatibility
        if (typeof objectGetInfo === 'string') {
            key = objectGetInfo;
            client = clients.legacy;
        } else {
            key = objectGetInfo.key;
            client = clients[objectGetInfo.dataStoreName];
        }
        if (client.clientType === 'scality') {
            return client.delete(key, reqUids, callback);
        }
        return client.delete(objectGetInfo, reqUids, callback);
    },

    healthcheck: (log, callback) => {
        const multBackendResp = {};
        const awsArray = [];
        const azureArray = [];
        async.each(Object.keys(clients), (location, cb) => {
            const client = clients[location];
            if (client.clientType === 'scality') {
                return client.healthcheck(log, (err, res) => {
                    if (err) {
                        multBackendResp[location] = { error: err };
                    } else {
                        multBackendResp[location] = { code: res.statusCode,
                            message: res.statusMessage };
                    }
                    return cb();
                });
            } else if (client.clientType === 'aws_s3') {
                awsArray.push(location);
                return cb();
            } else if (client.clientType === 'azure') {
                azureArray.push(location);
                return cb();
            }
            // if backend type isn't 'scality' or 'aws_s3', it will be
            //  'mem' or 'file', for which the default response is 200 OK
            multBackendResp[location] = { code: 200, message: 'OK' };
            return cb();
        }, () => {
            async.parallel([
                next => {
                    if (awsArray.length > 0) {
                        const randomAWS = awsArray[Math.floor(Math.random() *
                            awsArray.length)];
                        const checkThisOne = clients[randomAWS];
                        return checkThisOne.checkAWSHealth(randomAWS, next);
                    }
                    return next();
                },
                next => {
                    if (azureArray.length > 0) {
                        const randomAzure = azureArray[
                          Math.floor(Math.random() * azureArray.length)];
                        const checkThisOne = clients[randomAzure];
                        return checkThisOne.checkAzureHealth(randomAzure, next);
                    }
                    return next();
                },
            ], (errNull, externalResp) => {
                externalResp.forEach(resp =>
                  Object.assign(multBackendResp, resp));
                callback(null, multBackendResp);
            });
        });
    },

    createMPU: (key, metaHeaders, bucketName, websiteRedirectHeader,
    location, log, cb) => {
        const client = clients[location];
        if (client.clientType === 'aws_s3') {
            return client.createMPU(key, metaHeaders, bucketName,
            websiteRedirectHeader, log, cb);
        }
        return cb();
    },

    uploadPart: (request, streamingV4Params, stream, size, location, key,
    uploadId, partNumber, bucketName, log, cb) => {
        const client = clients[location];

        if (client.uploadPart) {
            return client.uploadPart(request, streamingV4Params, stream, size,
            key, uploadId, partNumber, bucketName, log, cb);
        }
        return cb();
    },

    listParts: (key, uploadId, location, bucketName, partNumberMarker, maxParts,
    log, cb) => {
        const client = clients[location];

        if (client.listParts) {
            return client.listParts(key, uploadId, bucketName, partNumberMarker,
                maxParts, log, cb);
        }
        return cb();
    },

    completeMPU: (key, uploadId, location, jsonList, bucketName, log, cb) => {
        const client = clients[location];
        const partList = jsonList.Part;
        if (client.clientType === 'aws_s3') {
            return client.completeMPU(partList, key, uploadId, bucketName, log,
            (err, completeObjData) => {
                if (err) {
                    return cb(err);
                }
                return cb(null, completeObjData);
            });
        }
        return cb();
    },

    abortMPU: (key, uploadId, location, bucketName, log, cb) => {
        const client = clients[location];
        if (client.clientType === 'azure') {
            const skipDataDelete = true;
            return cb(null, skipDataDelete);
        }
        if (client.abortMPU) {
            return client.abortMPU(key, uploadId, bucketName, log, err => {
                if (err) {
                    return cb(err);
                }
                return cb();
            });
        }
        return cb();
    },

    objectTagging: (method, key, bucket, objectMD, log, cb) => {
        // if legacy, objectMD will not contain dataStoreName, so just return
        const client = clients[objectMD.dataStoreName];
        if (client && client[`object${method}Tagging`]) {
            return client[`object${method}Tagging`](key, bucket, objectMD, log,
                cb);
        }
        return cb();
    },
    // NOTE: using copyObject only if copying object from one external
    // backend to the same external backend
    copyObject: (request, location, externalSourceKey,
    sourceLocationConstraintName, log, cb) => {
        const client = clients[location];
        if (client.copyObject) {
            return client.copyObject(request, externalSourceKey,
            sourceLocationConstraintName, log, (err, key) => {
                const dataRetrievalInfo = {
                    key,
                    dataStoreName: location,
                    dataStoreType: client.clientType,
                };
                cb(err, dataRetrievalInfo);
            });
        }
        return cb(errors.NotImplemented
            .customizeDescription('Can not copy object from ' +
              `${client.clientType} to ${client.clientType}`));
    },
    uploadPartCopy: (request, location, awsSourceKey,
    sourceLocationConstraintName, log, cb) => {
        const client = clients[location];
        if (client.uploadPartCopy) {
            return client.uploadPartCopy(request, awsSourceKey,
              sourceLocationConstraintName,
            log, cb);
        }
        return cb(errors.NotImplemented.customizeDescription(
          'Can not copy object from ' +
          `${client.clientType} to ${client.clientType}`));
    },
};

module.exports = multipleBackendGateway;
