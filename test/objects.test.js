// Copyright 2012 Joyent.  All rights reserved.

var clone = require('clone');
var uuid = require('node-uuid');
var vasync = require('vasync');

if (require.cache[__dirname + '/helper.js'])
        delete require.cache[__dirname + '/helper.js'];
var helper = require('./helper.js');



///--- Globals

var after = helper.after;
var before = helper.before;
var test = helper.test;

var BUCKET_CFG = {
        index: {
                str: {
                        type: 'string'
                },
                str_u: {
                        type: 'string',
                        unique: true
                },
                num: {
                        type: 'number'
                },
                num_u: {
                        type: 'number',
                        unique: true
                },
                bool: {
                        type: 'boolean'
                },
                bool_u: {
                        type: 'boolean',
                        unique: true
                }
        },
        pre: [function (req, cb) {
                var v = req.value;
                if (v.pre)
                        v.pre = 'pre_overwrite';

                cb();
        }],
        post: [function (req, cb) {
                cb();
        }]
};



///--- Helpers

function assertObject(b, t, obj, k, v) {
        t.ok(obj);
        if (!obj)
                return (undefined);

        t.equal(obj.bucket, b);
        t.equal(obj.key, k);
        t.deepEqual(obj.value, v);
        t.ok(obj._id);
        t.ok(obj._etag);
        t.ok(obj._mtime);
        return (undefined);
}

///--- Tests

before(function (cb) {
        var self = this;
        this.bucket = 'moray_unit_test_' + uuid.v4().substr(0, 7);
        this.assertObject = assertObject.bind(this, this.bucket);
        this.client = helper.createClient();
        this.client.on('connect', function () {
                var b = self.bucket;
                self.client.createBucket(b, BUCKET_CFG, cb);
        });
});


after(function (cb) {
        var self = this;
        this.client.delBucket(this.bucket, function (err) {
                self.client.close();
                cb(err);
        });
});


test('get object 404', function (t) {
        var c = this.client;
        c.getObject(this.bucket, uuid.v4().substr(0, 7), function (err) {
                t.ok(err);
                t.equal(err.name, 'ObjectNotFoundError');
                t.ok(err.message);
                t.end();
        });
});


test('CRUD object', function (t) {
        var b = this.bucket;
        var c = this.client;
        var k = uuid.v4();
        var v = {
                str: 'hi'
        };
        var v2 = {
                str: 'hello world',
                pre: 'hi'
        };
        var self = this;

        vasync.pipeline({
                funcs: [ function put(_, cb) {
                        c.putObject(b, k, v, cb);
                }, function get(_, cb) {
                        c.getObject(b, k, function (err, obj) {
                                if (err)
                                        return (cb(err));

                                t.ok(obj);
                                self.assertObject(t, obj, k, v);
                                cb();
                        });
                }, function overwrite(_, cb) {
                        c.putObject(b, k, v2, cb);
                }, function getAgain(_, cb) {
                        c.getObject(b, k, {noCache: true}, function (err, obj) {
                                if (err)
                                        return (cb(err));

                                t.ok(obj);
                                v2.pre = 'pre_overwrite';
                                self.assertObject(t, obj, k, v2);
                                cb();
                        });
                }, function del(_, cb) {
                        c.delObject(b, k, cb);
                } ],
                arg: {}
        }, function (err) {
                t.ifError(err);
                t.end();
        });
});