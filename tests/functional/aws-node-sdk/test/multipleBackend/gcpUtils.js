const gcp = require('google-cloud');
const crypto = require('crypto');

const { config } = require('../../../../../lib/Config');

const gcpLocation = 'gcp-test';

const utils = {};

utils.uniqName = name => `${name}${new Date().getTime()}`;

utils.getGcpClient = () => {
    let isTestingGcp;
    let gcpCredentials;
    let gcpClient;

    if (process.env['GCP_CRED']) {
        isTestingGcp = true;
        gcpCredentials = process.env['GCP_CRED'];
    } else if (config.locationConstraints[gcpLocation] &&
          config.locationConstraints[gcpLocation].details &&
          config.locationConstraints[gcpLocation].details.credentialsEnv) {
        isTestingGcp = true;
        gcpCredentials = config.locationConstraints[gcpLocation].details
            .credentialsEnv;
    } else {
        isTestingGcp = false;
    }

    if (isTestingGcp) {
        gcpClient = gcp.storage(gcpCredentials);
    }

    return gcpClient;
};

utils.getGcpBucketName = () => {
    let gcpBucketName;

    if (isTestingGcp) {
        if (config.locationConstraints[gcpLocation] &&
            config.locationConstraints[gcpLocation].details &&
            config.locationConstraints[gcpLocation].details.gcpBucketName) {
            gcpBucketName = config.locationConstraints[gcpLocation].details
                .gcpBucketName;
            isTestingGcp = true;
        } else {
            isTestingGcp = false;
        }
    }
    return gcpBucketName;
};

utils.getGcpKeys = () => {
    const keys = [
        {
            describe: 'empty',
            name: `somekey-${Date.now()}`,
            body: '',
            MD5: 'd41d8cd98f00b204e9800998ecf8427e',
        },
        {
            describe: 'normal',
            name: `somekey-${Date.now()}`,
            body: Buffer.from('I am a body', 'utf8'),
            MD5: 'be747eb4b75517bf6b3cf7c5fbb62f3a',
        },
        {
            describe: 'big',
            name: `bigkey-${Date.now()}`,
            body: Buffer.alloc(10485760),
            MD5: 'f1c9645dbc14efddc7d8a322685f26eb',
        },
    ];
    return keys;
};

// For contentMD5, Gcp requires base64 but AWS requires hex, so convert
// from base64 to hex
utils.convertMD5 = contentMD5 =>
    Buffer.from(contentMD5, 'base64').toString('hex');

utils.expectedETag = (body, getStringified = true) => {
    const eTagValue = crypto.createHash('md5').update(body).digest('hex');
    if (!getStringified) {
        return eTagValue;
    }
    return `"${eTagValue}"`;
};

module.exports = utils;
