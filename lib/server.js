/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2015, Joyent, Inc.
 */

var util = require('util');
var EventEmitter = require('events').EventEmitter;

var assert = require('assert-plus');
var libuuid = require('libuuid');
var mod_fast = require('fast');
var mod_kang = require('kang');
var mod_net = require('net');
var mod_os = require('os');
var VError = require('verror').VError;
var LRU = require('lru-cache');
var vasync = require('vasync');

var control = require('./control');
var buckets = require('./buckets');
var objects = require('./objects');
var ping = require('./ping');
var sql = require('./sql');


///--- Globals

// API version, increment for depended-upon changes
var API_VERSION = 2;


///--- API


function MorayServer(options) {
    var self = this;

    EventEmitter.call(this);

    var log = options.log;
    var db;
    if (options.standalone) {
        options.standalone.log = log;
        db = require('./standalone').createClient(options.standalone);
    } else {
        options.manatee.log = log;
        db = require('./manatee').createClient(options.manatee);
    }

    this.ms_bucketCache = new LRU({
        name: 'BucketCache',
        max: 100,
        maxAge: (300 * 1000)
    });
    this.ms_objectCache = new LRU({
        name: 'ObjectCache',
        max: 1000,
        maxAge: (30 * 1000)
    });
    var opts = {
        log: options.log,
        manatee: db,
        bucketCache: this.ms_bucketCache,
        objectCache: this.ms_objectCache
    };

    var socket = mod_net.createServer({ 'allowHalfOpen': true });
    var server = new mod_fast.FastServer({
        log: log.child({ component: 'fast' }),
        server: socket
    });

    var methods = [
        { rpcmethod: 'createBucket', rpchandler: buckets.creat(opts) },
        { rpcmethod: 'getBucket', rpchandler: buckets.get(opts) },
        { rpcmethod: 'listBuckets', rpchandler: buckets.list(opts) },
        { rpcmethod: 'updateBucket', rpchandler: buckets.update(opts) },
        { rpcmethod: 'delBucket', rpchandler: buckets.del(opts) },
        { rpcmethod: 'putObject', rpchandler: objects.put(opts) },
        { rpcmethod: 'batch', rpchandler: objects.batch(opts) },
        { rpcmethod: 'getObject', rpchandler: objects.get(opts) },
        { rpcmethod: 'delObject', rpchandler: objects.del(opts) },
        { rpcmethod: 'findObjects', rpchandler: objects.find(opts) },
        { rpcmethod: 'updateObjects', rpchandler: objects.update(opts) },
        { rpcmethod: 'reindexObjects', rpchandler: objects.reindex(opts) },
        { rpcmethod: 'deleteMany', rpchandler: objects.deleteMany(opts) },
        { rpcmethod: 'getTokens', rpchandler: getTokens(opts) },
        { rpcmethod: 'sql', rpchandler: sql.sql(opts) },
        { rpcmethod: 'ping', rpchandler: ping.ping(opts) },
        { rpcmethod: 'version', rpchandler: ping.version(opts, API_VERSION) }
    ];

    if (options.audit !== false) {
        // XXX: Implement this!
        throw new Error('Implement me!');
    }

    methods.forEach(function (rpc) {
        server.registerRpcMethod(rpc);
    });

    this.port = options.port;
    this.ip = options.bindip;

    this.fast_socket = socket;
    this.fast_server = server;
    this.kang_server = null;
    this.db_conn = db;
    this.log = options.log;

    if (options.kang_port) {
        mod_kang.knStartServer({
            port: options.kang_port,
            host: options.bindip,
            uri_base: '/kang',
            service_name: 'moray',
            version: API_VERSION.toString(),
            ident: mod_os.hostname() + '/' + process.pid,
            list_types: server.kangListTypes.bind(server),
            list_objects: server.kangListObjects.bind(server),
            get: server.kangGetObject.bind(server),
            stats: server.kangStats.bind(server)
        }, function (err, kang) {
            self.kang_server = kang;
        });
    }

    ['listening', 'error'].forEach(function (event) {
        // re-emit certain events from fast server
        self.fast_socket.on(event, self.emit.bind(self, event));
    });
    ['ready'].forEach(function (event) {
        // re-emit certain events from manatee
        self.db_conn.on(event, self.emit.bind(self, event));
    });
}
util.inherits(MorayServer, EventEmitter);


MorayServer.prototype.listen = function listen() {
    var self = this;
    this.fast_socket.listen(this.port, this.ip, function (err) {
        if (err) {
            self.log.error(err, 'failed to start Moray on %d', self.port);
            // XXX: retry
        } else {
            self.log.info('Moray listening on %d', self.port);
        }
    });
};


MorayServer.prototype.close = function close() {
    var self = this;

    vasync.parallel({
        funcs: [
            function (cb) {
                self.fast_socket.on('close', function () {
                    self.fast_server.close();

                    if (self.kang_server !== null) {
                        self.kang_server.close();
                    }

                    cb();
                });
                self.fast_socket.close();
            },
            function (cb) {
                self.db_conn.on('close', cb);
                self.db_conn.close();
            }
        ]
    }, function (err) {
        if (err) {
            throw err;
        }

        self.emit('close');
    });
};


function createServer(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');

    return new MorayServer(options);
}



///--- Exports

module.exports = {
    createServer: createServer
};



///--- Privates

var GET_TOKENS_ARGS = [
    { name: 'options', type: 'object' }
];

function getTokens(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');

    function _getTokens(rpc) {
        var argv = rpc.argv();
        if (control.invalidArgs(rpc, argv, GET_TOKENS_ARGS)) {
            return;
        }

        var opts = argv[0];
        var id = opts.req_id || libuuid.create();
        var log = options.log.child({
            req_id: id
        });

        log.debug('getTokens: entered');

        rpc.fail(new Error('Operation not supported'));
    }

    return _getTokens;
}
