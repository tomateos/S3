const assert = require('assert');
const async = require('async');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');
const { removeAllVersions } = require('../../lib/utility/versioning-util');
const customS3Request = require('../../lib/utility/customS3Request');

const { taggingTests } = require('../../lib/utility/tagging');

const sourceBucketName = 'supersourcebucket8102016';
const sourceObjName = 'supersourceobject';
const destBucketName = 'destinationbucket8102016';
const destObjName = 'copycatobject';

const originalMetadata = {
    oldmetadata: 'same old',
    overwriteme: 'wipe me out with replace',
};
const originalCacheControl = 'max-age=1337';
const originalContentDisposition = 'attachment; filename="1337.txt";';
const originalContentEncoding = 'base64,aws-chunked';
const originalExpires = new Date(12345678);

const originalTagKey = 'key1';
const originalTagValue = 'value1';
const originalTagging = `${originalTagKey}=${originalTagValue}`;

const newMetadata = {
    newmetadata: 'new kid in town',
    overwriteme: 'wiped',
};
const newCacheControl = 'max-age=86400';
const newContentDisposition = 'attachment; filename="fname.ext";';
const newContentEncoding = 'gzip,aws-chunked';
const newExpires = new Date();

const newTagKey = 'key2';
const newTagValue = 'value2';
const newTagging = `${newTagKey}=${newTagValue}`;

const content = 'I am the best content ever';
const secondContent = 'I am the second best content ever';

const otherAccountBucketUtility = new BucketUtility('lisa', {});
const otherAccountS3 = otherAccountBucketUtility.s3;

function checkNoError(err) {
    assert.equal(err, null,
        `Expected success, got error ${JSON.stringify(err)}`);
}

function checkError(err, code) {
    assert.notEqual(err, null, 'Expected failure but got success');
    assert.strictEqual(err.code, code);
}

function dateFromNow(diff) {
    const d = new Date();
    d.setHours(d.getHours() + diff);
    return d.toISOString();
}

function dateConvert(d) {
    return (new Date(d)).toISOString();
}


describe('Object Version Copy', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;
        let etag;
        let etagTrim;
        let lastModified;
        let versionId;
        let copySource;
        let copySourceVersionId;

        function emptyAndDeleteBucket(bucketName, callback) {
            return removeAllVersions({ Bucket: bucketName }, err => {
                if (err) {
                    callback(err);
                }
                return s3.deleteBucket({ Bucket: bucketName }, callback);
            });
        }

        beforeEach(() => bucketUtil.createOne(sourceBucketName)
            .then(() => bucketUtil.createOne(destBucketName))
            .then(() => s3.putBucketVersioningAsync({
                Bucket: sourceBucketName,
                VersioningConfiguration: { Status: 'Enabled' },
            }))
            .then(() => s3.putObjectAsync({
                Bucket: sourceBucketName,
                Key: sourceObjName,
                Body: content,
                Metadata: originalMetadata,
                CacheControl: originalCacheControl,
                ContentDisposition: originalContentDisposition,
                ContentEncoding: originalContentEncoding,
                Expires: originalExpires,
                Tagging: originalTagging,
            })).then(res => {
                etag = res.ETag;
                versionId = res.VersionId;
                copySource = `${sourceBucketName}/${sourceObjName}` +
                    `?versionId=${versionId}`;
                etagTrim = etag.substring(1, etag.length - 1);
                copySourceVersionId = res.VersionId;
                return s3.headObjectAsync({
                    Bucket: sourceBucketName,
                    Key: sourceObjName,
                });
            }).then(res => {
                lastModified = res.LastModified;
            }).then(() => s3.putObjectAsync({ Bucket: sourceBucketName,
                Key: sourceObjName,
                Body: secondContent }))
        );

        afterEach(done => async.parallel([
            next => emptyAndDeleteBucket(sourceBucketName, next),
            next => emptyAndDeleteBucket(destBucketName, next),
        ], done));

        function requestCopy(fields, cb) {
            s3.copyObject(Object.assign({
                Bucket: destBucketName,
                Key: destObjName,
                CopySource: copySource,
            }, fields), cb);
        }

        function successCopyCheck(error, response, copyVersionMetadata,
            destBucketName, destObjName, done) {
            checkNoError(error);
            assert.strictEqual(response.CopySourceVersionId,
              copySourceVersionId);
            assert.notStrictEqual(response.CopySourceVersionId,
              response.VersionId);
            const destinationVersionId = response.VersionId;
            assert.strictEqual(response.ETag, etag);
            const copyLastModified = new Date(response.LastModified)
                .toUTCString();
            s3.getObject({ Bucket: destBucketName,
                Key: destObjName }, (err, res) => {
                checkNoError(err);
                assert.strictEqual(res.VersionId, destinationVersionId);
                assert.strictEqual(res.Body.toString(), content);
                assert.deepStrictEqual(res.Metadata, copyVersionMetadata);
                assert.strictEqual(res.LastModified, copyLastModified);
                done();
            });
        }

        function checkSuccessTagging(key, value, cb) {
            s3.getObjectTagging({ Bucket: destBucketName, Key: destObjName },
            (err, data) => {
                checkNoError(err);
                assert.strictEqual(data.TagSet[0].Key, key);
                assert.strictEqual(data.TagSet[0].Value, value);
                cb();
            });
        }

        it('should copy an object from a source bucket to a different ' +
            'destination bucket and copy the tag set if no tagging directive' +
            'header provided', done => {
            s3.copyObject({ Bucket: destBucketName, Key: destObjName,
                CopySource: copySource },
                err => {
                    checkNoError(err);
                    checkSuccessTagging(originalTagKey, originalTagValue, done);
                });
        });

        it('should copy an object from a source bucket to a different ' +
            'destination bucket and copy the tag set if COPY tagging ' +
            'directive header provided', done => {
            s3.copyObject({ Bucket: destBucketName, Key: destObjName,
                CopySource: copySource,
                TaggingDirective: 'COPY' },
                err => {
                    checkNoError(err);
                    checkSuccessTagging(originalTagKey, originalTagValue, done);
                });
        });

        it('should copy an object from a source to the same destination ' +
        'updating tag if REPLACE tagging directive header provided',
        done => {
            s3.copyObject({ Bucket: destBucketName, Key: destObjName,
                CopySource: copySource,
                TaggingDirective: 'REPLACE', Tagging: newTagging },
                err => {
                    checkNoError(err);
                    checkSuccessTagging(newTagKey, newTagValue, done);
                });
        });

        describe('Copy object with versioning updating tag set', () => {
            taggingTests.forEach(taggingTest => {
                it(taggingTest.it, done => {
                    const key = encodeURIComponent(taggingTest.tag.key);
                    const value = encodeURIComponent(taggingTest.tag.value);
                    const tagging = `${key}=${value}`;
                    const params = { Bucket: destBucketName, Key: destObjName,
                        CopySource: copySource,
                        TaggingDirective: 'REPLACE', Tagging: tagging };
                    s3.copyObject(params, err => {
                        if (taggingTest.error) {
                            checkError(err, taggingTest.error);
                            return done();
                        }
                        assert.equal(err, null, 'Expected success, ' +
                        `got error ${JSON.stringify(err)}`);
                        return checkSuccessTagging(taggingTest.tag.key,
                          taggingTest.tag.value, done);
                    });
                });
            });
        });

        it('should return InvalidArgument for a request with versionId query',
        done => {
            const params = { Bucket: destBucketName, Key: destObjName,
                CopySource: copySource };
            const query = { versionId: 'testVersionId' };
            customS3Request(s3.copyObject, params, { query }, err => {
                assert(err, 'Expected error but did not find one');
                assert.strictEqual(err.code, 'InvalidArgument');
                assert.strictEqual(err.statusCode, 400);
                done();
            });
        });

        it('should return InvalidArgument for a request with empty string ' +
        'versionId query', done => {
            const params = { Bucket: destBucketName, Key: destObjName,
                CopySource: copySource };
            const query = { versionId: '' };
            customS3Request(s3.copyObject, params, { query }, err => {
                assert(err, 'Expected error but did not find one');
                assert.strictEqual(err.code, 'InvalidArgument');
                assert.strictEqual(err.statusCode, 400);
                done();
            });
        });

        it('should copy a version from a source bucket to a different ' +
            'destination bucket and copy the metadata if no metadata directve' +
            'header provided', done => {
            s3.copyObject({ Bucket: destBucketName, Key: destObjName,
                CopySource: copySource },
                (err, res) =>
                    successCopyCheck(err, res, originalMetadata,
                        destBucketName, destObjName, done)
                );
        });

        it('should also copy additional headers (CacheControl, ' +
        'ContentDisposition, ContentEncoding, Expires) when copying an ' +
        'object from a source bucket to a different destination bucket',
          done => {
              s3.copyObject({ Bucket: destBucketName, Key: destObjName,
                  CopySource: copySource },
                  err => {
                      checkNoError(err);
                      s3.getObject({ Bucket: destBucketName, Key: destObjName },
                        (err, res) => {
                            if (err) {
                                done(err);
                            }
                            assert.strictEqual(res.CacheControl,
                              originalCacheControl);
                            assert.strictEqual(res.ContentDisposition,
                              originalContentDisposition);
                            // Should remove V4 streaming value 'aws-chunked'
                            // to be compatible with AWS behavior
                            assert.strictEqual(res.ContentEncoding,
                              'base64,'
                            );
                            assert.strictEqual(res.Expires,
                                originalExpires.toGMTString());
                            done();
                        });
                  });
          });

        it('should copy an object from a source bucket to a different ' +
            'key in the same bucket',
            done => {
                s3.copyObject({ Bucket: sourceBucketName, Key: destObjName,
                    CopySource: copySource },
                    (err, res) =>
                        successCopyCheck(err, res, originalMetadata,
                            sourceBucketName, destObjName, done)
                    );
            });

        it('should copy an object from a source to the same destination ' +
            '(update metadata)', done => {
            s3.copyObject({ Bucket: sourceBucketName, Key: sourceObjName,
                CopySource: copySource,
                MetadataDirective: 'REPLACE',
                Metadata: newMetadata },
                (err, res) =>
                    successCopyCheck(err, res, newMetadata,
                        sourceBucketName, sourceObjName, done)
                );
        });

        it('should copy an object and replace the metadata if replace ' +
            'included as metadata directive header', done => {
            s3.copyObject({ Bucket: destBucketName, Key: destObjName,
                CopySource: copySource,
                MetadataDirective: 'REPLACE',
                Metadata: newMetadata,
            },
                (err, res) =>
                    successCopyCheck(err, res, newMetadata,
                        destBucketName, destObjName, done)
                );
        });

        it('should copy an object and replace ContentType if replace ' +
            'included as a metadata directive header, and new ContentType is ' +
            'provided', done => {
            s3.copyObject({ Bucket: destBucketName, Key: destObjName,
                CopySource: copySource,
                MetadataDirective: 'REPLACE',
                ContentType: 'image',
            }, () => {
                s3.getObject({ Bucket: destBucketName,
                    Key: destObjName }, (err, res) => {
                    if (err) {
                        return done(err);
                    }
                    assert.strictEqual(res.ContentType, 'image');
                    return done();
                });
            });
        });

        it('should copy an object and keep ContentType if replace ' +
            'included as a metadata directive header, but no new ContentType ' +
            'is provided', done => {
            s3.copyObject({ Bucket: destBucketName, Key: destObjName,
                CopySource: copySource, MetadataDirective: 'REPLACE',
            }, () => {
                s3.getObject({ Bucket: destBucketName,
                    Key: destObjName }, (err, res) => {
                    if (err) {
                        return done(err);
                    }
                    assert.strictEqual(res.ContentType,
                        'application/octet-stream');
                    return done();
                });
            });
        });

        it('should also replace additional headers if replace ' +
            'included as metadata directive header and new headers are ' +
            'specified', done => {
            s3.copyObject({ Bucket: destBucketName, Key: destObjName,
                CopySource: copySource,
                MetadataDirective: 'REPLACE',
                CacheControl: newCacheControl,
                ContentDisposition: newContentDisposition,
                ContentEncoding: newContentEncoding,
                Expires: newExpires,
            }, err => {
                checkNoError(err);
                s3.getObject({ Bucket: destBucketName,
                    Key: destObjName }, (err, res) => {
                    if (err) {
                        done(err);
                    }
                    assert.strictEqual(res.CacheControl, newCacheControl);
                    assert.strictEqual(res.ContentDisposition,
                      newContentDisposition);
                    // Should remove V4 streaming value 'aws-chunked'
                    // to be compatible with AWS behavior
                    assert.strictEqual(res.ContentEncoding, 'gzip,');
                    assert.strictEqual(res.Expires, newExpires.toGMTString());
                    done();
                });
            });
        });

        it('should copy an object and the metadata if copy ' +
            'included as metadata directive header (and ignore any new ' +
            'metadata sent with copy request)', done => {
            s3.copyObject({ Bucket: destBucketName, Key: destObjName,
                CopySource: copySource,
                MetadataDirective: 'COPY',
                Metadata: newMetadata,
            },
                err => {
                    checkNoError(err);
                    s3.getObject({ Bucket: destBucketName,
                        Key: destObjName }, (err, res) => {
                        assert.deepStrictEqual(res.Metadata, originalMetadata);
                        done();
                    });
                });
        });

        it('should copy an object and its additional headers if copy ' +
            'included as metadata directive header (and ignore any new ' +
            'headers sent with copy request)', done => {
            s3.copyObject({ Bucket: destBucketName, Key: destObjName,
                CopySource: copySource,
                MetadataDirective: 'COPY',
                Metadata: newMetadata,
                CacheControl: newCacheControl,
                ContentDisposition: newContentDisposition,
                ContentEncoding: newContentEncoding,
                Expires: newExpires,
            }, err => {
                checkNoError(err);
                s3.getObject({ Bucket: destBucketName, Key: destObjName },
                  (err, res) => {
                      if (err) {
                          done(err);
                      }
                      assert.strictEqual(res.CacheControl,
                        originalCacheControl);
                      assert.strictEqual(res.ContentDisposition,
                        originalContentDisposition);
                      assert.strictEqual(res.ContentEncoding,
                        'base64,');
                      assert.strictEqual(res.Expires,
                        originalExpires.toGMTString());
                      done();
                  });
            });
        });

        it('should copy a 0 byte object to different destination', done => {
            const emptyFileETag = '"d41d8cd98f00b204e9800998ecf8427e"';
            s3.putObject({ Bucket: sourceBucketName, Key: sourceObjName,
                Body: '', Metadata: originalMetadata }, (err, res) => {
                checkNoError(err);
                copySource = `${sourceBucketName}/${sourceObjName}` +
                    `?versionId=${res.VersionId}`;
                s3.copyObject({ Bucket: destBucketName, Key: destObjName,
                    CopySource: copySource,
                },
                    (err, res) => {
                        checkNoError(err);
                        assert.strictEqual(res.ETag, emptyFileETag);
                        s3.getObject({ Bucket: destBucketName,
                            Key: destObjName }, (err, res) => {
                            assert.deepStrictEqual(res.Metadata,
                                originalMetadata);
                            assert.strictEqual(res.ETag, emptyFileETag);
                            done();
                        });
                    });
            });
        });

        it('should copy a 0 byte object to same destination', done => {
            const emptyFileETag = '"d41d8cd98f00b204e9800998ecf8427e"';
            s3.putObject({ Bucket: sourceBucketName, Key: sourceObjName,
                Body: '' }, (err, res) => {
                checkNoError(err);
                copySource = `${sourceBucketName}/${sourceObjName}` +
                    `?versionId=${res.VersionId}`;
                s3.copyObject({ Bucket: sourceBucketName, Key: sourceObjName,
                    CopySource: copySource,
                    StorageClass: 'REDUCED_REDUNDANCY',
                },
                    (err, res) => {
                        checkNoError(err);
                        assert.strictEqual(res.ETag, emptyFileETag);
                        s3.getObject({ Bucket: sourceBucketName,
                            Key: sourceObjName }, (err, res) => {
                            assert.deepStrictEqual(res.Metadata,
                                {});
                            assert.deepStrictEqual(res.StorageClass,
                                'REDUCED_REDUNDANCY');
                            assert.strictEqual(res.ETag, emptyFileETag);
                            done();
                        });
                    });
            });
        });

        it('should copy an object to a different destination and change ' +
            'the storage class if storage class header provided', done => {
            s3.copyObject({ Bucket: destBucketName, Key: destObjName,
                CopySource: copySource,
                StorageClass: 'REDUCED_REDUNDANCY',
            },
                err => {
                    checkNoError(err);
                    s3.getObject({ Bucket: destBucketName,
                        Key: destObjName }, (err, res) => {
                        assert.strictEqual(res.StorageClass,
                            'REDUCED_REDUNDANCY');
                        done();
                    });
                });
        });

        it('should copy an object to the same destination and change the ' +
            'storage class if the storage class header provided', done => {
            s3.copyObject({ Bucket: sourceBucketName, Key: sourceObjName,
                CopySource: copySource,
                StorageClass: 'REDUCED_REDUNDANCY',
            },
                err => {
                    checkNoError(err);
                    s3.getObject({ Bucket: sourceBucketName,
                        Key: sourceObjName }, (err, res) => {
                        checkNoError(err);
                        assert.strictEqual(res.StorageClass,
                            'REDUCED_REDUNDANCY');
                        done();
                    });
                });
        });

        it('should copy an object to a new bucket and overwrite an already ' +
            'existing object in the destination bucket', done => {
            s3.putObject({ Bucket: destBucketName, Key: destObjName,
                Body: 'overwrite me', Metadata: originalMetadata },
                err => {
                    checkNoError(err);
                    s3.copyObject({ Bucket: destBucketName, Key: destObjName,
                        CopySource: copySource,
                        MetadataDirective: 'REPLACE',
                        Metadata: newMetadata,
                    }, (err, res) => {
                        checkNoError(err);
                        assert.strictEqual(res.ETag, etag);
                        s3.getObject({ Bucket: destBucketName,
                            Key: destObjName }, (err, res) => {
                            assert.deepStrictEqual(res.Metadata,
                                newMetadata);
                            assert.strictEqual(res.ETag, etag);
                            assert.strictEqual(res.Body.toString(), content);
                            done();
                        });
                    });
                });
        });

        // skipping test as object level encryption is not implemented yet
        it.skip('should copy an object and change the server side encryption' +
            'option if server side encryption header provided', done => {
            s3.copyObject({ Bucket: destBucketName, Key: destObjName,
                CopySource: copySource,
                ServerSideEncryption: 'AES256',
            },
                err => {
                    checkNoError(err);
                    s3.getObject({ Bucket: destBucketName,
                        Key: destObjName }, (err, res) => {
                        assert.strictEqual(res.ServerSideEncryption,
                            'AES256');
                        done();
                    });
                });
        });

        it('should return Not Implemented error for obj. encryption using ' +
            'AWS-managed encryption keys', done => {
            const params = { Bucket: destBucketName, Key: 'key',
                CopySource: copySource,
                ServerSideEncryption: 'AES256' };
            s3.copyObject(params, err => {
                assert.strictEqual(err.code, 'NotImplemented');
                done();
            });
        });

        it('should return Not Implemented error for obj. encryption using ' +
            'customer-provided encryption keys', done => {
            const params = { Bucket: destBucketName, Key: 'key',
                CopySource: copySource,
                SSECustomerAlgorithm: 'AES256' };
            s3.copyObject(params, err => {
                assert.strictEqual(err.code, 'NotImplemented');
                done();
            });
        });

        it('should copy an object and set the acl on the new object', done => {
            s3.copyObject({ Bucket: destBucketName, Key: destObjName,
                CopySource: copySource,
                ACL: 'authenticated-read',
            },
                err => {
                    checkNoError(err);
                    s3.getObjectAcl({ Bucket: destBucketName,
                        Key: destObjName }, (err, res) => {
                        // With authenticated-read ACL, there are two
                        // grants:
                        // (1) FULL_CONTROL to the object owner
                        // (2) READ to the authenticated-read
                        assert.strictEqual(res.Grants.length, 2);
                        assert.strictEqual(res.Grants[0].Permission,
                            'FULL_CONTROL');
                        assert.strictEqual(res.Grants[1].Permission,
                            'READ');
                        assert.strictEqual(res.Grants[1].Grantee.URI,
                            'http://acs.amazonaws.com/groups/' +
                            'global/AuthenticatedUsers');
                        done();
                    });
                });
        });

        it('should copy an object and default the acl on the new object ' +
            'to private even if the copied object had a ' +
            'different acl', done => {
            s3.putObjectAcl({ Bucket: sourceBucketName, Key: sourceObjName,
                ACL: 'authenticated-read' }, () => {
                s3.copyObject({ Bucket: destBucketName, Key: destObjName,
                    CopySource: copySource,
                },
                    () => {
                        s3.getObjectAcl({ Bucket: destBucketName,
                            Key: destObjName }, (err, res) => {
                            // With private ACL, there is only one grant
                            // of FULL_CONTROL to the object owner
                            assert.strictEqual(res.Grants.length, 1);
                            assert.strictEqual(res.Grants[0].Permission,
                                'FULL_CONTROL');
                            done();
                        });
                    });
            });
        });

        it('should return an error if attempt to copy with same source as' +
            'destination and do not change any metadata', done => {
            s3.copyObject({ Bucket: sourceBucketName, Key: sourceObjName,
                CopySource: copySource,
            },
                err => {
                    checkError(err, 'InvalidRequest');
                    done();
                });
        });

        it('should return an error if attempt to copy from nonexistent bucket',
            done => {
                s3.copyObject({ Bucket: destBucketName, Key: destObjName,
                    CopySource: `nobucket453234/${sourceObjName}`,
                },
                err => {
                    checkError(err, 'NoSuchBucket');
                    done();
                });
            });

        it('should return an error if use invalid redirect location',
            done => {
                s3.copyObject({ Bucket: destBucketName, Key: destObjName,
                    CopySource: copySource,
                    WebsiteRedirectLocation: 'google.com',
                },
                err => {
                    checkError(err, 'InvalidRedirectLocation');
                    done();
                });
            });


        it('should return an error if attempt to copy to nonexistent bucket',
            done => {
                s3.copyObject({ Bucket: 'nobucket453234', Key: destObjName,
                    CopySource: `${sourceBucketName}/${sourceObjName}`,
                },
                err => {
                    checkError(err, 'NoSuchBucket');
                    done();
                });
            });

        it('should return an error if attempt to copy nonexistent object',
            done => {
                s3.copyObject({ Bucket: destBucketName, Key: destObjName,
                    CopySource: `${sourceBucketName}/nokey`,
                },
                err => {
                    checkError(err, 'NoSuchKey');
                    done();
                });
            });

        it('should return NoSuchKey if attempt to copy version with ' +
        ' delete marker', done => {
            s3.deleteObject({
                Bucket: sourceBucketName,
                Key: sourceObjName,
            }, (err, data) => {
                if (err) {
                    done(err);
                }
                assert.strictEqual(data.DeleteMarker, 'true');
                s3.copyObject({
                    Bucket: destBucketName,
                    Key: destObjName,
                    CopySource: `${sourceBucketName}/${sourceObjName}`,
                },
                err => {
                    checkError(err, 'NoSuchKey');
                    done();
                });
            });
        });

        it('should return InvalidRequest if attempt to copy specific ' +
        'version that is a delete marker', done => {
            s3.deleteObject({
                Bucket: sourceBucketName,
                Key: sourceObjName,
            }, (err, data) => {
                if (err) {
                    done(err);
                }
                assert.strictEqual(data.DeleteMarker, 'true');
                const deleteMarkerId = data.VersionId;
                s3.copyObject({
                    Bucket: destBucketName,
                    Key: destObjName,
                    CopySource: `${sourceBucketName}/${sourceObjName}` +
                    `?versionId=${deleteMarkerId}`,
                },
                err => {
                    checkError(err, 'InvalidRequest');
                    done();
                });
            });
        });

        it('should return an error if send invalid metadata directive header',
            done => {
                s3.copyObject({ Bucket: destBucketName, Key: destObjName,
                    CopySource: copySource,
                    MetadataDirective: 'copyHalf',
                },
                err => {
                    checkError(err, 'InvalidArgument');
                    done();
                });
            });

        describe('copying by another account', () => {
            const otherAccountBucket = 'otheraccountbucket42342342342';
            const otherAccountKey = 'key';
            beforeEach(() => otherAccountBucketUtility
                .createOne(otherAccountBucket)
            );

            afterEach(() => otherAccountBucketUtility.empty(otherAccountBucket)
                .then(() => otherAccountBucketUtility
                .deleteOne(otherAccountBucket))
            );

            it('should not allow an account without read persmission on the ' +
                'source object to copy the object', done => {
                otherAccountS3.copyObject({ Bucket: otherAccountBucket,
                    Key: otherAccountKey,
                    CopySource: copySource,
                },
                    err => {
                        checkError(err, 'AccessDenied');
                        done();
                    });
            });

            it('should not allow an account without write persmission on the ' +
                'destination bucket to copy the object', done => {
                otherAccountS3.putObject({ Bucket: otherAccountBucket,
                    Key: otherAccountKey, Body: '' }, () => {
                    otherAccountS3.copyObject({ Bucket: destBucketName,
                        Key: destObjName,
                        CopySource: `${otherAccountBucket}/${otherAccountKey}`,
                    },
                        err => {
                            checkError(err, 'AccessDenied');
                            done();
                        });
                });
            });

            it('should allow an account with read permission on the ' +
                'source object and write permission on the destination ' +
                'bucket to copy the object', done => {
                s3.putObjectAcl({ Bucket: sourceBucketName,
                    Key: sourceObjName, ACL: 'public-read', VersionId:
                    versionId }, () => {
                    otherAccountS3.copyObject({ Bucket: otherAccountBucket,
                        Key: otherAccountKey,
                        CopySource: copySource,
                    },
                        err => {
                            checkNoError(err);
                            done();
                        });
                });
            });
        });

        it('If-Match: returns no error when ETag match, with double quotes ' +
            'around ETag',
            done => {
                requestCopy({ CopySourceIfMatch: etag }, err => {
                    checkNoError(err);
                    done();
                });
            });

        it('If-Match: returns no error when one of ETags match, with double ' +
            'quotes around ETag',
            done => {
                requestCopy({ CopySourceIfMatch:
                    `non-matching,${etag}` }, err => {
                    checkNoError(err);
                    done();
                });
            });

        it('If-Match: returns no error when ETag match, without double ' +
            'quotes around ETag',
            done => {
                requestCopy({ CopySourceIfMatch: etagTrim }, err => {
                    checkNoError(err);
                    done();
                });
            });

        it('If-Match: returns no error when one of ETags match, without ' +
            'double quotes around ETag',
            done => {
                requestCopy({ CopySourceIfMatch:
                    `non-matching,${etagTrim}` }, err => {
                    checkNoError(err);
                    done();
                });
            });

        it('If-Match: returns no error when ETag match with *', done => {
            requestCopy({ CopySourceIfMatch: '*' }, err => {
                checkNoError(err);
                done();
            });
        });

        it('If-Match: returns PreconditionFailed when ETag does not match',
            done => {
                requestCopy({ CopySourceIfMatch: 'non-matching ETag' }, err => {
                    checkError(err, 'PreconditionFailed');
                    done();
                });
            });

        it('If-None-Match: returns no error when ETag does not match', done => {
            requestCopy({ CopySourceIfNoneMatch: 'non-matching' }, err => {
                checkNoError(err);
                done();
            });
        });

        it('If-None-Match: returns no error when all ETags do not match',
            done => {
                requestCopy({
                    CopySourceIfNoneMatch: 'non-matching,non-matching-either',
                }, err => {
                    checkNoError(err);
                    done();
                });
            });

        it('If-None-Match: returns NotModified when ETag match, with double ' +
            'quotes around ETag',
            done => {
                requestCopy({ CopySourceIfNoneMatch: etag }, err => {
                    checkError(err, 'PreconditionFailed');
                    done();
                });
            });

        it('If-None-Match: returns NotModified when one of ETags match, with ' +
            'double quotes around ETag',
            done => {
                requestCopy({
                    CopySourceIfNoneMatch: `non-matching,${etag}`,
                }, err => {
                    checkError(err, 'PreconditionFailed');
                    done();
                });
            });

        it('If-None-Match: returns NotModified when ETag match, without ' +
            'double quotes around ETag',
            done => {
                requestCopy({ CopySourceIfNoneMatch: etagTrim }, err => {
                    checkError(err, 'PreconditionFailed');
                    done();
                });
            });

        it('If-None-Match: returns NotModified when one of ETags match, ' +
            'without double quotes around ETag',
            done => {
                requestCopy({
                    CopySourceIfNoneMatch: `non-matching,${etagTrim}`,
                }, err => {
                    checkError(err, 'PreconditionFailed');
                    done();
                });
            });

        it('If-Modified-Since: returns no error if Last modified date is ' +
            'greater',
            done => {
                requestCopy({ CopySourceIfModifiedSince: dateFromNow(-1) },
                    err => {
                        checkNoError(err);
                        done();
                    });
            });

        // Skipping this test, because real AWS does not provide error as
        // expected
        it.skip('If-Modified-Since: returns NotModified if Last modified ' +
            'date is lesser',
            done => {
                requestCopy({ CopySourceIfModifiedSince: dateFromNow(1) },
                    err => {
                        checkError(err, 'PreconditionFailed');
                        done();
                    });
            });

        it('If-Modified-Since: returns NotModified if Last modified ' +
            'date is equal',
            done => {
                requestCopy({ CopySourceIfModifiedSince:
                    dateConvert(lastModified) },
                    err => {
                        checkError(err, 'PreconditionFailed');
                        done();
                    });
            });

        it('If-Unmodified-Since: returns no error when lastModified date is ' +
            'greater',
            done => {
                requestCopy({ CopySourceIfUnmodifiedSince: dateFromNow(1) },
                err => {
                    checkNoError(err);
                    done();
                });
            });

        it('If-Unmodified-Since: returns no error when lastModified ' +
            'date is equal',
            done => {
                requestCopy({ CopySourceIfUnmodifiedSince:
                    dateConvert(lastModified) },
                    err => {
                        checkNoError(err);
                        done();
                    });
            });

        it('If-Unmodified-Since: returns PreconditionFailed when ' +
            'lastModified date is lesser',
            done => {
                requestCopy({ CopySourceIfUnmodifiedSince: dateFromNow(-1) },
                err => {
                    checkError(err, 'PreconditionFailed');
                    done();
                });
            });

        it('If-Match & If-Unmodified-Since: returns no error when match Etag ' +
            'and lastModified is greater',
            done => {
                requestCopy({
                    CopySourceIfMatch: etagTrim,
                    CopySourceIfUnmodifiedSince: dateFromNow(-1),
                }, err => {
                    checkNoError(err);
                    done();
                });
            });

        it('If-Match match & If-Unmodified-Since match', done => {
            requestCopy({
                CopySourceIfMatch: etagTrim,
                CopySourceIfUnmodifiedSince: dateFromNow(1),
            }, err => {
                checkNoError(err);
                done();
            });
        });

        it('If-Match not match & If-Unmodified-Since not match', done => {
            requestCopy({
                CopySourceIfMatch: 'non-matching',
                CopySourceIfUnmodifiedSince: dateFromNow(-1),
            }, err => {
                checkError(err, 'PreconditionFailed');
                done();
            });
        });

        it('If-Match not match & If-Unmodified-Since match', done => {
            requestCopy({
                CopySourceIfMatch: 'non-matching',
                CopySourceIfUnmodifiedSince: dateFromNow(1),
            }, err => {
                checkError(err, 'PreconditionFailed');
                done();
            });
        });

        // Skipping this test, because real AWS does not provide error as
        // expected
        it.skip('If-Match match & If-Modified-Since not match', done => {
            requestCopy({
                CopySourceIfMatch: etagTrim,
                CopySourceIfModifiedSince: dateFromNow(1),
            }, err => {
                checkNoError(err);
                done();
            });
        });

        it('If-Match match & If-Modified-Since match', done => {
            requestCopy({
                CopySourceIfMatch: etagTrim,
                CopySourceIfModifiedSince: dateFromNow(-1),
            }, err => {
                checkNoError(err);
                done();
            });
        });

        it('If-Match not match & If-Modified-Since not match', done => {
            requestCopy({
                CopySourceIfMatch: 'non-matching',
                CopySourceIfModifiedSince: dateFromNow(1),
            }, err => {
                checkError(err, 'PreconditionFailed');
                done();
            });
        });

        it('If-Match not match & If-Modified-Since match', done => {
            requestCopy({
                CopySourceIfMatch: 'non-matching',
                CopySourceIfModifiedSince: dateFromNow(-1),
            }, err => {
                checkError(err, 'PreconditionFailed');
                done();
            });
        });

        it('If-None-Match & If-Modified-Since: returns NotModified when Etag ' +
            'does not match and lastModified is greater',
            done => {
                requestCopy({
                    CopySourceIfNoneMatch: etagTrim,
                    CopySourceIfModifiedSince: dateFromNow(-1),
                }, err => {
                    checkError(err, 'PreconditionFailed');
                    done();
                });
            });

        it('If-None-Match not match & If-Modified-Since not match', done => {
            requestCopy({
                CopySourceIfNoneMatch: etagTrim,
                CopySourceIfModifiedSince: dateFromNow(1),
            }, err => {
                checkError(err, 'PreconditionFailed');
                done();
            });
        });

        it('If-None-Match match & If-Modified-Since match', done => {
            requestCopy({
                CopySourceIfNoneMatch: 'non-matching',
                CopySourceIfModifiedSince: dateFromNow(-1),
            }, err => {
                checkNoError(err);
                done();
            });
        });

        // Skipping this test, because real AWS does not provide error as
        // expected
        it.skip('If-None-Match match & If-Modified-Since not match', done => {
            requestCopy({
                CopySourceIfNoneMatch: 'non-matching',
                CopySourceIfModifiedSince: dateFromNow(1),
            }, err => {
                checkError(err, 'PreconditionFailed');
                done();
            });
        });

        it('If-None-Match match & If-Unmodified-Since match', done => {
            requestCopy({
                CopySourceIfNoneMatch: 'non-matching',
                CopySourceIfUnmodifiedSince: dateFromNow(1),
            }, err => {
                checkNoError(err);
                done();
            });
        });

        it('If-None-Match match & If-Unmodified-Since not match', done => {
            requestCopy({
                CopySourceIfNoneMatch: 'non-matching',
                CopySourceIfUnmodifiedSince: dateFromNow(-1),
            }, err => {
                checkError(err, 'PreconditionFailed');
                done();
            });
        });

        it('If-None-Match not match & If-Unmodified-Since match', done => {
            requestCopy({
                CopySourceIfNoneMatch: etagTrim,
                CopySourceIfUnmodifiedSince: dateFromNow(1),
            }, err => {
                checkError(err, 'PreconditionFailed');
                done();
            });
        });

        it('If-None-Match not match & If-Unmodified-Since not match', done => {
            requestCopy({
                CopySourceIfNoneMatch: etagTrim,
                CopySourceIfUnmodifiedSince: dateFromNow(-1),
            }, err => {
                checkError(err, 'PreconditionFailed');
                done();
            });
        });
    });
});
