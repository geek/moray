/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */



var assert = require('assert-plus');
var vasync = require('vasync');
var libuuid = require('libuuid');

var InvocationError = require('./errors').InvocationError;
var pgError = require('./pg').pgError;
var dtrace = require('./dtrace');

///--- API


function assertOptions(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.object(options.manatee, 'options.manatee');
    assert.object(options.bucketCache, 'options.bucketCache');
}


function _getPGHandleAfter(req, cb) {
    function done(startErr, pg) {
        if (startErr) {
            req.log.debug(startErr, '%s: no DB handle', req.route);
            cb(startErr);
        } else {
            if (req.opts.timeout)
                pg.setTimeout(req.opts.timeout);
            req.pg = pg;
            cb(null);
        }
    }

    return done;
}


function getPGHandle(req, cb) {
    req.manatee.pg(_getPGHandleAfter(req, cb));
}


function getPGHandleAndTransaction(req, cb) {
    // run pg.begin before returning control
    req.manatee.start(_getPGHandleAfter(req, cb));
}

function releasePGHandle(req) {
    if (req.pg) {
        req.pg.release();
        req.pg = null;
    }
}

function handlerPipeline(options) {
    assert.object(options, 'options');
    assert.arrayOfFunc(options.funcs, 'options.funcs');
    assert.object(options.req, 'options.req');
    assert.object(options.req.rpc, 'options.req.rpc');
    assert.string(options.req.route, 'options.req.route');
    // Modifiers for rpc.end() and dtrace.fire() output
    assert.optionalFunc(options.cbOutput, 'options.cbOutput');
    assert.optionalFunc(options.cbProbe, 'options.cbProbe');

    var req = options.req;
    var route = req.route;
    var rpc = req.rpc;
    var log = req.log;

    var cbOutput = options.cbOutput || function () { return null; };
    var cbProbe = options.cbProbe || function () { return ([req.msgid]); };

    function pipelineCallback(err) {
        var probe = route.toLowerCase() + '-done';
        function done() {
            if (dtrace[probe]) {
                dtrace[probe].fire(cbProbe);
            }
            // post-pipeline checks
            if (req.pg) {
                assert.fail('PG connection left open');
            }
        }

        if (err) {
            log.debug(err, '%s: failed', route);
            if (req.pg) {
                req.pg.rollback();
                req.pg = null;
            }
            rpc.fail(pgError(err));
            done();
            return;
        }
        req.pg.commit(function (err2) {
            req.pg = null;
            if (err2) {
                log.debug(err2, '%s: failed', route);
                rpc.fail(pgError(err2));
            } else {
                var result = cbOutput();
                if (result) {
                    log.debug({result: result}, '%s: done', route);
                    rpc.end(result);
                } else {
                    log.debug('%s: done', route);
                    rpc.end();
                }
            }
            done();
        });
        return;
    }

    vasync.forEachPipeline({
        inputs: options.funcs,
        func: function (handler, cb) {
            dtrace.fire('handler-start', function () {
                return [req.msgid, route, handler.name, req.req_id];
            });
            handler(options.req, function (err) {
                dtrace.fire('handler-done', function () {
                    return [req.msgid, route, handler.name];
                });
                cb(err);
            });
        }
    }, pipelineCallback);
}

function buildReq(opts, rpc, serverOpts) {
    assert.object(opts);
    assert.object(rpc);
    assert.object(serverOpts);

    var connId = rpc.connectionId();
    var msgid = rpc.requestId();
    var route = rpc.methodName();
    var req_id = opts.req_id || libuuid.create();

    var log = serverOpts.log.child({
        connId: connId,
        msgid: msgid,
        route: route,
        req_id: req_id
    });

    var req = {
        log: log,
        req_id: req_id,
        route: route,
        connId: connId,
        msgid: msgid,
        opts: opts,
        rpc: rpc,
        manatee: serverOpts.manatee,
        bucketCache: serverOpts.bucketCache
    };

    return req;
}


/*
 * Validate that the correct number of arguments are provided to an RPC, and are
 * of the expected type. If the arguments are incorrect, then this function will
 * handle failing the RPC with an appropriate error, and return 'true'. If the
 * arguments are okay, then the function will return 'false', and the RPC can
 * continue normally.
 *
 * - rpc: The node-fast rpc object
 * - argv: The array of arguments provided to the RPC (obtained from rpc.argv())
 * - types: An array of { name, type } objects describing the arguments expected
 *   by this RPC, and their types
 *
 * Valid types to check for are:
 *
 * - array: Check that the argument is an Array
 * - string: Check that the argument is a nonempty String
 * - integer: Check that the argument is a nonnegative integer.
 * - object: Check that the argument is a non-null object.
 */
function invalidArgs(rpc, argv, types) {
    var route = rpc.methodName();
    var len = types.length;

    if (argv.length !== len) {
        rpc.fail(new InvocationError(
            '%s expects %d argument%s', route, len, len === 1 ? '' : 's'));
        return true;
    }

    for (var i = 0; i < len; i++) {
        var name = types[i].name;
        var type = types[i].type;
        var val = argv[i];

        switch (type) {
        case 'array':
            if (Array.isArray(val)) {
                break;
            }

            rpc.fail(new InvocationError(
                '%s expects "%s" (args[%d]) to be an array',
                route, name, i));
            return true;
        case 'integer':
            if (typeof (val) === 'number' && val >= 0 &&
                val === Math.floor(val)) {
                break;
            }

            rpc.fail(new InvocationError(
                '%s expects "%s" (args[%d]) to be a nonnegative integer',
                route, name, i));
            return true;
        case 'object':
            if (typeof (val) === 'object' && !Array.isArray(val) &&
                val !== null) {
                break;
            }

            rpc.fail(new InvocationError(
                '%s expects "%s" (args[%d]) to be an object',
                route, name, i));
            return true;
        case 'string':
            if (typeof (val) === 'string' && val.length > 0) {
                break;
            }

            rpc.fail(new InvocationError(
                '%s expects "%s" (args[%d]) to be a nonempty string',
                route, name, i));
            return true;
        default:
            throw new Error('Unknown type: ' + type);
        }
    }

    return false;
}


///--- Exports

module.exports = {
    assertOptions: assertOptions,
    getPGHandle: getPGHandle,
    getPGHandleAndTransaction: getPGHandleAndTransaction,
    releasePGHandle: releasePGHandle,
    handlerPipeline: handlerPipeline,
    invalidArgs: invalidArgs,
    buildReq: buildReq
};
