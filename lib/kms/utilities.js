const { auth } = require('arsenal');
const commander = require('commander');

const http = require('http');
const https = require('https');
const logger = require('../utilities/logger');

function _createEncryptedBucket(host,
                                port,
                                bucketName,
                                accessKey,
                                secretKey,
                                verbose, ssl,
                                locationConstraint) {
    const options = {
        host,
        port,
        method: 'PUT',
        path: `/${bucketName}/`,
        headers: {
            'x-amz-scal-server-side-encryption': 'AES256',
        },
        rejectUnauthorized: false,
    };
    const transport = ssl ? https : http;
    const request = transport.request(options, response => {
        if (verbose) {
            logger.info('response status code', {
                statusCode: response.statusCode,
            });
            logger.info('response headers', { headers: response.headers });
        }
        const body = [];
        response.setEncoding('utf8');
        response.on('data', chunk => body.push(chunk));
        response.on('end', () => {
            if (response.statusCode >= 200 && response.statusCode < 300) {
                logger.info('Success', {
                    statusCode: response.statusCode,
                    body: verbose ? body.join('') : undefined,
                });
                process.exit(0);
            } else {
                logger.error('request failed with HTTP Status ', {
                    statusCode: response.statusCode,
                    body: body.join(''),
                });
                process.exit(1);
            }
        });
    });

    auth.client.generateV4Headers(request, '', accessKey, secretKey, 's3');
    if (verbose) {
        logger.info('request headers', { headers: request._headers });
    }
    if (locationConstraint) {
        const createBucketConfiguration = '<CreateBucketConfiguration ' +
        'xmlns="http://s3.amazonaws.com/doc/2006-03-01/">' +
        `<LocationConstraint>${locationConstraint}</LocationConstraint>` +
        '</CreateBucketConfiguration >';
        request.write(createBucketConfiguration);
    }
    request.end();
}

/**
 * This function is used as a binary to send a request to S3 and create an
 * encrypted bucket, because most of the s3 tools don't support custom
 * headers
 *
 * @return {undefined}
 */
function createEncryptedBucket() {
    commander
        .version('0.0.1')
        .option('-a, --access-key <accessKey>', 'Access key id')
        .option('-k, --secret-key <secretKey>', 'Secret access key')
        .option('-b, --bucket <bucket>', 'Name of the bucket')
        .option('-h, --host <host>', 'Host of the server')
        .option('-p, --port <port>', 'Port of the server')
        .option('-s', '--ssl', 'Enable ssl')
        .option('-v, --verbose')
        .option('-l, --location-constraint <locationConstraint>',
        'location Constraint')
        .parse(process.argv);

    const { host, port, accessKey, secretKey, bucket, verbose, ssl,
    locationConstraint } = commander;

    if (!host || !port || !accessKey || !secretKey || !bucket) {
        logger.error('missing parameter');
        commander.outputHelp();
        process.exit(1);
    }
    _createEncryptedBucket(host, port, bucket, accessKey, secretKey, verbose,
        ssl, locationConstraint);
}

module.exports = {
    createEncryptedBucket,
};
