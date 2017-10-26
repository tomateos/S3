const url = require('url');
const async = require('async');

const { auth, errors } = require('arsenal');
const { responseJSONBody } = require('arsenal').s3routes.routesUtils;
const vault = require('../auth/vault');
const metadata = require('../metadata/wrapper');
const locationConstraintCheck = require(
    '../api/apiUtils/object/locationConstraintCheck');
const { dataStore } = require('../api/apiUtils/object/storeObject');
const prepareRequestContexts = require(
'../api/apiUtils/authorization/prepareRequestContexts');
const { metadataValidateBucketAndObj } = require('../metadata/metadataUtils');
const { BackendInfo } = require('../api/apiUtils/object/BackendInfo');
const { locationConstraints } = require('../Config').config;
const multipleBackendGateway = require('../data/multipleBackendGateway');

auth.setHandler(vault);

const NAMESPACE = 'default';
const CIPHER = null; // replication/lifecycle does not work on encrypted objects

function _decodeURI(uri) {
    // do the same decoding than in S3 server
    return decodeURIComponent(uri.replace(/\+/g, ' '));
}

function normalizeBackbeatRequest(req) {
    /* eslint-disable no-param-reassign */
    const parsedUrl = url.parse(req.url, true);
    req.path = _decodeURI(parsedUrl.pathname);
    const pathArr = req.path.split('/');
    req.query = parsedUrl.query;
    req.resourceType = pathArr[3];
    req.bucketName = pathArr[4];
    req.objectKey = pathArr.slice(5).join('/');
    /* eslint-enable no-param-reassign */
}

function _respond(response, payload, log, callback) {
    const body = typeof payload === 'object' ?
        JSON.stringify(payload) : payload;
    const httpHeaders = {
        'x-amz-id-2': log.getSerializedUids(),
        'x-amz-request-id': log.getSerializedUids(),
        'content-type': 'application/json',
        'content-length': Buffer.byteLength(body),
    };
    response.writeHead(200, httpHeaders);
    response.end(body, 'utf8', callback);
}

function _getRequestPayload(req, cb) {
    const payload = [];
    let payloadLen = 0;
    req.on('data', chunk => {
        payload.push(chunk);
        payloadLen += chunk.length;
    }).on('error', cb)
    .on('end', () => cb(null, Buffer.concat(payload, payloadLen).toString()));
}

function _checkMultipleBackendRequest(request, log) {
    const { headers, bucketName, query } = request;
    const { operation } = query;
    let errMessage;
    if ((operation === 'initiatempu' || operation === 'putobject') &&
        headers['x-scal-version-id'] === undefined) {
        errMessage = 'bad request: missing x-scal-version-id header';
        log.error(errMessage);
        return errors.BadRequest.customizeDescription(errMessage);
    }
    if (operation === 'putpart' &&
        headers['x-scal-part-number'] === undefined) {
        errMessage = 'bad request: missing part-number header';
        log.error(errMessage);
        return errors.BadRequest.customizeDescription(errMessage);
    }
    if ((operation === 'putpart' || operation === 'completempu') &&
        headers['x-scal-upload-id'] === undefined) {
        errMessage = 'bad request: missing upload-id header';
        log.error(errMessage);
        return errors.BadRequest.customizeDescription(errMessage);
    }
    if (operation === 'putobject' &&
        headers['x-scal-canonical-id'] === undefined) {
        errMessage = 'bad request: missing x-scal-canonical-id header';
        log.error(errMessage);
        return errors.BadRequest.customizeDescription(errMessage);
    }
    if (operation === 'putobject' &&
        headers['content-md5'] === undefined) {
        errMessage = 'bad request: missing content-md5 header';
        log.error(errMessage);
        return errors.BadRequest.customizeDescription(errMessage);
    }
    if (headers['x-scal-storage-type'] === undefined) {
        errMessage = 'bad request: missing x-scal-storage-type header';
        log.error(errMessage);
        return errors.BadRequest.customizeDescription(errMessage);
    }
    if (headers['x-scal-storage-class'] === undefined) {
        errMessage = 'bad request: missing x-scal-storage-class header';
        log.error(errMessage);
        return errors.BadRequest.customizeDescription(errMessage);
    }
    const location = locationConstraints[headers['x-scal-storage-class']];
    const isValidLocation = location &&
        location.type === headers['x-scal-storage-type'] &&
        location.details.bucketName === bucketName;
    if (!isValidLocation) {
        errMessage = 'invalid request: invalid location constraint in request';
        log.debug(errMessage, {
            method: request.method,
            bucketName: request.bucketName,
            objectKey: request.objectKey,
            resourceType: request.resourceType,
        });
        return errors.InvalidRequest.customizeDescription(errMessage);
    }
    return undefined;
}

/*
PUT /_/backbeat/metadata/<bucket name>/<object key>
PUT /_/backbeat/data/<bucket name>/<object key>
PUT /_/backbeat/multiplebackenddata/<bucket name>/<object key>
    ?operation=putobject
PUT /_/backbeat/multiplebackenddata/<bucket name>/<object key>
    ?operation=putpart
POST /_/backbeat/multiplebackenddata/<bucket name>/<object key>
    ?operation=initiatempu
POST /_/backbeat/multiplebackenddata/<bucket name>/<object key>
    ?operation=completempu
*/

function putData(request, response, bucketInfo, objMd, log, callback) {
    let errMessage;
    const canonicalID = request.headers['x-scal-canonical-id'];
    if (canonicalID === undefined) {
        errMessage = 'bad request: missing x-scal-canonical-id header';
        log.error(errMessage);
        return callback(errors.BadRequest.customizeDescription(errMessage));
    }
    const contentMD5 = request.headers['content-md5'];
    if (contentMD5 === undefined) {
        errMessage = 'bad request: missing content-md5 header';
        log.error(errMessage);
        return callback(errors.BadRequest.customizeDescription(errMessage));
    }
    const context = {
        bucketName: request.bucketName,
        owner: canonicalID,
        namespace: NAMESPACE,
        objectKey: request.objectKey,
    };
    const payloadLen = parseInt(request.headers['content-length'], 10);
    const backendInfoObj = locationConstraintCheck(
        request, null, bucketInfo, log);
    if (backendInfoObj.err) {
        log.error('error getting backendInfo', {
            error: backendInfoObj.err,
            method: 'routeBackbeat',
        });
        return callback(errors.InternalError);
    }
    const backendInfo = backendInfoObj.backendInfo;
    return dataStore(
        context, CIPHER, request, payloadLen, {},
        backendInfo, log, (err, retrievalInfo, md5) => {
            if (err) {
                log.error('error putting data', {
                    error: err,
                    method: 'putData',
                });
                return callback(err);
            }
            if (contentMD5 !== md5) {
                return callback(errors.BadDigest);
            }
            const { key, dataStoreName } = retrievalInfo;
            const dataRetrievalInfo = [{
                key,
                dataStoreName,
            }];
            return _respond(response, dataRetrievalInfo, log, callback);
        });
}

function putMetadata(request, response, bucketInfo, objMd, log, callback) {
    return _getRequestPayload(request, (err, payload) => {
        if (err) {
            return callback(err);
        }
        let omVal;
        try {
            omVal = JSON.parse(payload);
        } catch (err) {
            // FIXME: add error type MalformedJSON
            return callback(errors.MalformedPOSTRequest);
        }
        const { headers, bucketName, objectKey } = request;
        // check if it's metadata only operation
        if (headers['x-scal-replication-content'] === 'METADATA') {
            if (!objMd) {
                // if the target does not exist, return an error to
                // backbeat, who will have to retry the operation as a
                // complete replication
                return callback(errors.ObjNotFound);
            }
            // use original data locations
            omVal.location = objMd.location;
        }
        // specify both 'versioning' and 'versionId' to create a "new"
        // version (updating master as well) but with specified
        // versionId
        const options = {
            versioning: true,
            versionId: omVal.versionId,
        };
        log.trace('putting object version', {
            objectKey: request.objectKey, omVal, options });
        return metadata.putObjectMD(bucketName, objectKey, omVal, options, log,
            (err, md) => {
                if (err) {
                    log.error('error putting object metadata', {
                        error: err,
                        method: 'putMetadata',
                    });
                    return callback(err);
                }
                return _respond(response, md, log, callback);
            });
    });
}

function putObject(request, response, log, callback) {
    const err = _checkMultipleBackendRequest(request, log);
    if (err) {
        return callback(err);
    }
    const storageLocation = request.headers['x-scal-storage-class'];
    const sourceVersionId = request.headers['x-scal-version-id'];
    const canonicalID = request.headers['x-scal-canonical-id'];
    const contentMD5 = request.headers['content-md5'];
    const metaHeaders = {
        'x-amz-meta-scal-location-constraint': storageLocation,
        'x-amz-meta-scal-replication-status': 'REPLICA',
        'x-amz-meta-scal-version-id': sourceVersionId,
    };
    const context = {
        bucketName: request.bucketName,
        owner: canonicalID,
        namespace: NAMESPACE,
        objectKey: request.objectKey,
        metaHeaders,
    };
    const payloadLen = parseInt(request.headers['content-length'], 10);
    const backendInfo = new BackendInfo(storageLocation);
    return dataStore(context, CIPHER, request, payloadLen, {}, backendInfo, log,
        (err, retrievalInfo, md5) => {
            if (err) {
                log.error('error putting data', {
                    error: err,
                    method: 'putObject',
                });
                return callback(err);
            }
            if (contentMD5 !== md5) {
                return callback(errors.BadDigest);
            }
            const dataRetrievalInfo = {
                versionId: retrievalInfo.dataStoreVersionId,
            };
            return _respond(response, dataRetrievalInfo, log, callback);
        });
}

function deleteObject(request, response, log, callback) {
    const err = _checkMultipleBackendRequest(request, log);
    if (err) {
        return callback(err);
    }
    const storageLocation = request.headers['x-scal-storage-class'];
    const objectGetInfo = {
        key: request.objectKey,
        dataStoreName: storageLocation,
    };
    const reqUids = log.getSerializedUids();
    return multipleBackendGateway.delete(objectGetInfo, reqUids, err => {
        if (err) {
            log.error('error deleting object in multiple backend', {
                error: err,
                method: 'deleteObject',
            });
            return callback(err);
        }
        return _respond(response, {}, log, callback);
    });
}

function initiateMultipartUpload(request, response, log, callback) {
    const err = _checkMultipleBackendRequest(request, log);
    if (err) {
        return callback(err);
    }
    const storageLocation = request.headers['x-scal-storage-class'];
    const sourceVersionId = request.headers['x-scal-version-id'];
    const metaHeaders = {
        'scal-location-constraint': storageLocation,
        'scal-replication-status': 'REPLICA',
        'scal-version-id': sourceVersionId,
    };
    return multipleBackendGateway.createMPU(request.objectKey, metaHeaders,
        request.bucketName, undefined, storageLocation, log, (err, data) => {
            if (err) {
                log.error('error initiating multipart upload', {
                    error: err,
                    method: 'initiateMultipartUpload',
                });
                return callback(err);
            }
            const dataRetrievalInfo = {
                uploadId: data.UploadId,
            };
            return _respond(response, dataRetrievalInfo, log, callback);
        });
}

function putPart(request, response, log, callback) {
    const err = _checkMultipleBackendRequest(request, log);
    if (err) {
        return callback(err);
    }
    const storageLocation = request.headers['x-scal-storage-class'];
    const partNumber = request.headers['x-scal-part-number'];
    const uploadId = request.headers['x-scal-upload-id'];
    const payloadLen = parseInt(request.headers['content-length'], 10);
    return multipleBackendGateway.uploadPart(undefined, {}, request, payloadLen,
        storageLocation, request.objectKey, uploadId, partNumber,
        request.bucketName, log, (err, data) => {
            if (err) {
                log.error('error putting MPU part', {
                    error: err,
                    method: 'putPart',
                });
                return callback(err);
            }
            const dataRetrievalInfo = {
                partNumber,
                ETag: data.dataStoreETag,
            };
            return _respond(response, dataRetrievalInfo, log, callback);
        });
}

function completeMultipartUpload(request, response, log, callback) {
    const err = _checkMultipleBackendRequest(request, log);
    if (err) {
        return callback(err);
    }
    const storageLocation = request.headers['x-scal-storage-class'];
    const uploadId = request.headers['x-scal-upload-id'];
    const data = [];
    request.on('data', chunk => data.push(chunk));
    request.on('end', () => {
        let Part;
        try {
            Part = JSON.parse(Buffer.concat(data));
        } catch (e) {
            // FIXME: add error type MalformedJSON
            return callback(errors.MalformedPOSTRequest);
        }
        return multipleBackendGateway.completeMPU(request.objectKey, uploadId,
            storageLocation, { Part }, undefined,
            request.bucketName, log, err => {
                if (err) {
                    log.error('error completing MPU', {
                        error: err,
                        method: 'completeMultipartUpload',
                    });
                    return callback(err);
                }
                return _respond(response, {}, log, callback);
            });
    });
    return undefined;
}

const backbeatRoutes = {
    PUT: {
        data: putData,
        metadata: putMetadata,
        multiplebackenddata: {
            putobject: putObject,
            putpart: putPart,
        },
    },
    POST: {
        multiplebackenddata: {
            initiatempu: initiateMultipartUpload,
            completempu: completeMultipartUpload,
        },
    },
    DELETE: {
        multiplebackenddata: {
            deleteobject: deleteObject,
        },
    },
};

function routeBackbeat(clientIP, request, response, log) {
    log.debug('routing request', { method: 'routeBackbeat' });
    normalizeBackbeatRequest(request);
    const useMultipleBackend = request.resourceType === 'multiplebackenddata';
    const invalidRequest = (!request.bucketName ||
                            !request.objectKey ||
                            !request.resourceType ||
                            (!request.query.operation && useMultipleBackend));
    if (invalidRequest) {
        log.debug('invalid request', {
            method: request.method, bucketName: request.bucketName,
            objectKey: request.objectKey, resourceType: request.resourceType,
            query: request.query,
        });
        return responseJSONBody(errors.MethodNotAllowed, null, response, log);
    }
    const requestContexts = prepareRequestContexts('objectReplicate', request);
    return async.waterfall([next => auth.server.doAuth(
        request, log, (err, userInfo) => {
            if (err) {
                log.debug('authentication error', {
                    error: err,
                    method: request.method,
                    bucketName: request.bucketName,
                    objectKey: request.objectKey,
                });
            }
            return next(err, userInfo);
        }, 's3', requestContexts),
        (userInfo, next) => {
            if (useMultipleBackend) {
                // Bucket and object do not exist in metadata.
                return next(null, null, null);
            }
            const mdValParams = { bucketName: request.bucketName,
                objectKey: request.objectKey,
                authInfo: userInfo,
                requestType: 'ReplicateObject' };
            return metadataValidateBucketAndObj(mdValParams, log, next);
        },
        (bucketInfo, objMd, next) => {
            const invalidRoute = backbeatRoutes[request.method] === undefined ||
                backbeatRoutes[request.method][request.resourceType] ===
                    undefined ||
                (backbeatRoutes[request.method][request.resourceType]
                    [request.query.operation] === undefined &&
                    useMultipleBackend);
            if (invalidRoute) {
                log.debug('no such route', { method: request.method,
                    bucketName: request.bucketName,
                    objectKey: request.objectKey,
                    resourceType: request.resourceType,
                    query: request.query,
                });
                return next(errors.MethodNotAllowed);
            }
            if (useMultipleBackend) {
                return backbeatRoutes[request.method][request.resourceType]
                    [request.query.operation](request, response, log, next);
            }
            const versioningConfig = bucketInfo.getVersioningConfiguration();
            if (!versioningConfig || versioningConfig.Status !== 'Enabled') {
                log.debug('bucket versioning is not enabled', {
                    method: request.method,
                    bucketName: request.bucketName,
                    objectKey: request.objectKey,
                    resourceType: request.resourceType,
                });
                return next(errors.InvalidBucketState);
            }
            return backbeatRoutes[request.method][request.resourceType](
                request, response, bucketInfo, objMd, log, next);
        }],
        err => {
            if (err) {
                return responseJSONBody(err, null, response, log);
            }
            log.debug('backbeat route response sent successfully',
                { method: request.method,
                    bucketName: request.bucketName,
                    objectKey: request.objectKey });
            return undefined;
        });
}


module.exports = routeBackbeat;
