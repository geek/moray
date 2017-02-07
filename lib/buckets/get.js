/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var util = require('util');

var BucketNotFoundError = require('../errors').BucketNotFoundError;
var control = require('../control');


///--- Globals

var ARGS_SCHEMA = [
    { name: 'options', type: 'object' },
    { name: 'bucket', type: 'string' }
];

var PIPELINE = [
    control.getPGHandle,
    loadBucket
];


///--- Handlers

function loadBucket(req, cb) {
    var log = req.log;
    var pg = req.pg;
    var q;
    var row;
    var sql = util.format('SELECT * FROM buckets_config WHERE name=\'%s\'',
                          req.bucket.name);

    log.debug({
        bucket: req.bucket.name
    }, 'loadBucket: entered');

    q = pg.query(sql);

    q.once('error', function (err) {
        log.debug({
            bucket: req.bucket.name,
            err: err
        }, 'loadBucket: failed');
        cb(err);
    });

    q.once('row', function (r) {
        row = r;
    });

    q.once('end', function () {
        log.debug({
            bucket: row
        }, 'loadBucket: done');
        if (row) {
            row.options = row.options || {};
            row.options.version = row.options.version || 0;
            req.bucket = row;
            cb(null);
        } else {
            cb(new BucketNotFoundError(req.bucket.name));
        }
    });
}


function get(options) {
    control.assertOptions(options);

    function _get(rpc) {
        var argv = rpc.argv();
        if (control.invalidArgs(rpc, argv, ARGS_SCHEMA)) {
            return;
        }

        var opts = argv[0];
        var bucket = argv[1];

        var req = control.buildReq(opts, rpc, options);
        req.bucket = {
            name: bucket
        };

        req.log.debug({
            bucket: bucket,
            opts: opts
        }, 'getBucket: entered');

        control.handlerPipeline({
            req: req,
            funcs: PIPELINE,
            cbOutput: function () { return req.bucket; }
        });
    }

    return (_get);
}


///--- Exports

module.exports = {
    get: get
};
