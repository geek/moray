/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var fs = require('fs');
var util = require('util');

var assert = require('assert-plus');
var verror = require('verror');




///--- Globals

var WError = verror.WError;



///--- Helpers

function ISODateString(d) {
    function pad(n) {
        return n < 10 ? '0' + n : n;
    }

    if (typeof (d) === 'string')
        d = new Date(d);

    return d.getUTCFullYear() + '-'
        + pad(d.getUTCMonth()+1) + '-'
        + pad(d.getUTCDate()) + 'T'
        + pad(d.getUTCHours()) + ':'
        + pad(d.getUTCMinutes()) + ':'
        + pad(d.getUTCSeconds()) + 'Z';
}



///--- Errors

function BucketNotFoundError(cause, bucket) {
    if (arguments.length === 1) {
        bucket = cause;
        cause = {};
    }
    assert.string(bucket, 'bucket');

    WError.call(this, cause, '%s does not exist', bucket);
    this.name = this.constructor.name;
}
util.inherits(BucketNotFoundError, WError);


function BucketConflictError(cause, bucket) {
    if (arguments.length === 1) {
        bucket = cause;
        cause = {};
    }
    assert.string(bucket, 'bucket');

    WError.call(this, cause, '%s was likely being concurrently updated',
                bucket);
    this.name = this.constructor.name;
}
util.inherits(BucketConflictError, WError);


function BucketVersionError(cause, b, v, v2) {
    if (arguments.length === 3) {
        v2 = v;
        v = b;
        b = cause;
        cause = {};
    }
    assert.string(b, 'bucket');
    assert.number(v, 'version');
    assert.number(v2, 'oldVersion');

    WError.call(this, cause, '%s has a newer version than %d (%d)',
                b, v2, v);
    this.name = this.constructor.name;
}
util.inherits(BucketVersionError, WError);


function ConnectTimeoutError(cause, t) {
    if (arguments.length === 1) {
        t = cause;
        cause = {};
    }

    WError.call(this, cause, 'connect timeout after %d ms', t);
    this.name = 'ConnectTimeoutError';
}
util.inherits(ConnectTimeoutError, WError);


function EtagConflictError(cause, bucket, key, expect, actual) {
    if (arguments.length === 4) {
        actual = expect;
        expect = key;
        key = bucket;
        bucket = cause;
        cause = {};
    }
    assert.string(bucket, 'bucket');
    assert.string(key, 'key');
    assert.string(expect, 'expect');
    assert.string(actual, 'actual');

    WError.call(this, cause,
                '%s::%s was expected to have etag "%s", but was "%s"',
                bucket, key, expect, actual);

    this.name = this.constructor.name;
    this.context = {
        bucket: bucket,
        key: key,
        expected: expect,
        actual: actual
    };
}
util.inherits(EtagConflictError, WError);



function FieldUpdateError(cause, fields) {
    if (arguments.length === 1) {
        fields = cause;
        cause = {};
    }
    assert.object(fields, 'fields');

    WError.call(this, cause,
                '%j is not a valid update definition',
                fields);
    this.name = this.constructor.name;
}
util.inherits(FieldUpdateError, WError);


function InternalError() {
    WError.apply(this, arguments);
    this.name = this.constructor.name;
}
util.inherits(InternalError, WError);


function InvalidBucketConfigError() {
    WError.apply(this, arguments);
    this.name = this.constructor.name;
}
util.inherits(InvalidBucketConfigError, WError);


function InvalidBucketNameError(cause, bucket) {
    if (arguments.length === 1) {
        bucket = cause;
        cause = {};
    }
    assert.string(bucket, 'bucket');

    WError.call(this, cause, '%s is not a valid bucket name', bucket);
    this.name = this.constructor.name;
}
util.inherits(InvalidBucketNameError, WError);


function InvalidIndexDefinitionError(cause, type) {
    if (arguments.length === 1) {
        type = cause;
        cause = {};
    }
    assert.string(type, 'type');

    WError.call(this, cause,
                '%s is an invalid index type. Supported types are %j',
                ['boolean', 'number', 'string'], type);
    this.name = this.constructor.name;
}
util.inherits(InvalidIndexTypeError, WError);


function InvalidIndexTypeError(cause, name, type) {
    if (arguments.length === 2) {
        type = name;
        name = cause;
        cause = {};
    }
    assert.string(name, 'name');
    assert.string(type, 'type');

    WError.call(this, cause, 'index(%s) is of type %s', name, type);
    this.name = this.constructor.name;
}
util.inherits(InvalidIndexTypeError, WError);


function InvalidQueryError(cause, filter) {
    if (arguments.length === 1) {
        filter = cause;
        cause = {};
    }
    assert.string(filter, 'filter');
    WError.call(this, cause, '%s is an invalid filter', filter);
    this.name = this.constructor.name;
}
util.inherits(InvalidQueryError, WError);


function InvocationError() {
    WError.apply(this, arguments);
    this.name = this.constructor.name;
}
util.inherits(InvocationError, WError);


function NoDatabaseError(cause) {
    WError.call(this, (cause || {}),
                'Not connected to manatee and/or postgres');
    this.name = this.constructor.name;
}
util.inherits(NoDatabaseError, WError);


function NoDatabasePeersError(cause) {
    WError.call(this, (cause || {}), 'no manatee peers available');
    this.name = this.constructor.name;
}
util.inherits(NoDatabasePeersError, WError);


function NotFunctionError(cause, name) {
    if (arguments.length === 1) {
        name = cause;
        cause = {};
    }
    assert.string(name, 'name');

    WError.call(this, cause, '%s must be [Function]', name);
    this.name = this.constructor.name;
}
util.inherits(NotFunctionError, WError);


function NotIndexedError(cause, bucket, filter) {
    if (arguments.length === 2) {
        filter = bucket;
        bucket = cause;
        cause = {};
    }
    assert.string(bucket, 'bucket');
    assert.string(filter, 'filter');

    WError.call(this, cause,
                '%s does not have indexes that support %s',
                bucket, filter);
    this.name = this.constructor.name;
}
util.inherits(NotIndexedError, WError);


function ObjectNotFoundError(cause, bucket, key) {
    if (arguments.length === 2) {
        key = bucket;
        bucket = cause;
        cause = {};
    }
    assert.string(bucket, 'bucket');
    assert.string(key, 'key');

    WError.call(this, cause, '%s::%s does not exist', bucket, key);
    this.name = this.constructor.name;
}
util.inherits(ObjectNotFoundError, WError);


function QueryTimeoutError(cause, sql) {
    if (arguments.length === 1) {
        sql = cause;
        cause = {};
    }

    WError.call(this, cause, 'query timeout: %s', sql);
    this.name = 'QueryTimeoutError';
}
util.inherits(QueryTimeoutError, WError);


function SchemaChangeError(cause, bucket, attribute) {
    if (arguments.length === 2) {
        attribute = bucket;
        bucket = cause;
        cause = {};
    }
    assert.string(bucket, 'bucket');
    assert.string(attribute, 'attribute');

    WError.call(this, cause, '%s: an index already exists for %s',
                bucket, attribute);
    this.name = this.constructor.name;
}
util.inherits(SchemaChangeError, WError);


function UniqueAttributeError(cause, attribute, value) {
    if (arguments.length === 2) {
        value = attribute;
        attribute = cause;
        cause = {};
    }
    assert.string(attribute, 'attribute');
    assert.string(value, 'value');
    WError.call(this, cause || {},
                '"%s" is a unique attribute; value "%s" already exists',
                attribute, value);
    this.name = this.constructor.name;
}
util.inherits(UniqueAttributeError, WError);

module.exports = {
    BucketNotFoundError: BucketNotFoundError,
    BucketConflictError: BucketConflictError,
    BucketVersionError: BucketVersionError,
    ConnectTimeoutError: ConnectTimeoutError,
    EtagConflictError: EtagConflictError,
    FieldUpdateError: FieldUpdateError,
    InternalError: InternalError,
    InvalidBucketConfigError: InvalidBucketConfigError,
    InvalidBucketNameError: InvalidBucketNameError,
    InvalidIndexDefinitionError: InvalidIndexDefinitionError,
    InvalidIndexTypeError: InvalidIndexTypeError,
    InvalidQueryError: InvalidQueryError,
    InvocationError: InvocationError,
    NoDatabaseError: NoDatabaseError,
    NoDatabasePeersError: NoDatabasePeersError,
    NotFunctionError: NotFunctionError,
    NotIndexedError: NotIndexedError,
    ObjectNotFoundError: ObjectNotFoundError,
    QueryTimeoutError: QueryTimeoutError,
    SchemaChangeError: SchemaChangeError,
    UniqueAttributeError: UniqueAttributeError
};
