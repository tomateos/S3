//const { errors } = require('arsenal');
const gcloud = require('google-cloud');
const GS = gcloud.storage;
//const createLogger = require('../multipleBackendLogger');
//const logHelper = require('./utils').logHelper;

class GcpClient {
	constructor(config) {
		this._gcpParams = config.gcpParams;
		this._gcpBucketName = config.gcpBucketName;
		this._bucketMatch = config.bucketMatch;
		this._client = new GS(this._gcpParams);
	}

	_createGcpKey(requestBucketName, requestObjectKey,
		bucketMatch) {
		if (bucketMatch) {
			return requestObjectKey;
		}
		return `${requestBucketName}/${requestObjectKey}`;
	}

	put(stream, size, keyContext, reqUids, callback) {
		const gcpKey = this._createGcpKey(keyContext.bucketName,
			keyContext.objectKey, this._bucketMatch);
	}

	get(objectGetInfo, range, reqUids, callback) {
	}

	checkGcpHealth(location, callback) {
		const gcpResp = {};
		let bucket = this._client.bucket(this._gcpBucketName);
		this._client.exist(bucket, function(err, exists) {
			if (err) {
				gcpResp[location] = { error: err.message }
				return callback(null, gcpResp);
			}
			gcpResp[location] = {
				message: 'Congrats! You own the bucket'
			};
			return callback(null, gcpResp);
		});
	}
}
