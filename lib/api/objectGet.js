'use strict';

const utils = require('../utils.js');
const services = require('./services.js');
const async = require('async');


/**
 * GET Object - Get an object
 * @param  {string} accessKey - user's access key
 *
 *
 *
 */

let objectGet = function(accessKey, datastore,  metastore, request, callback){
  let bucketname = utils.getResourceNames(request).bucket;
  let bucketUID = utils.getResourceUID(request.namespace, bucketname);
  let objectKey = utils.getResourceNames(request).object;
  let objectUID = utils.getResourceUID(request.namespace, bucketname + objectKey);
  let metadataValParams = {accessKey: accessKey, bucketUID: bucketUID, objectUID: objectUID, metastore: metastore};
  let datastoreParams = {};

async.waterfall([
      function(next){
          services.metadataValidateAuthorization(metadataValParams, next)
          //TODO: Either in metadataValidateAuthorization or in parallel function: 
          //1) check whether request.header has "if-modified-since" or "if-unmodified-since", 
          //if so check update time against specified date.
          //2) check whether request.header has "if-match" or "if-none-match",
          //if so check whether object etag matches.  Check from medata or actual object? 
          //3) Add user meta headers in metadata bucket to response headers.
      },
      function(bucket, objectExistsDatastoreLocation, next){
          services.getFromDatastore(bucket, objectExistsDatastoreLocation, datastore, datastoreParams, next)
      }
  ], function (err, result) {
			return callback(err, result);
  });
};

module.exports = objectGet;