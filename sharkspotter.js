/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2018, Joyent, Inc.
 */

var mod_assert = require('assert-plus');
var mod_bunyan = require('bunyan');
var mod_getopt = require('posix-getopt');
var mod_moray = require('moray');
var mod_vasync = require('vasync');

var mod_fs = require('fs');
var mod_util = require('util');
var EventEmitter = require('events').EventEmitter;

/*
 * sharkspotter.js - find objects that should belong on a shark
 *
 * The user provides a shark name (e.g. 2.stor) and a Moray shard name
 * (e.g. 3.moray). This script uses Moray's `findobjects` to query the backend
 * database for _all_ objects between certain chunks of _id/_idx values of the
 * Manta table. The objects are then filtered to find only those that include
 * the given shark's storage id. Those results are written line-by-line to a
 * file in the format:
 *      owner_uuid object_uuid shark_1 shark_2 ... shark_n
 *
 * This tool is meant to aid in running a manual audit of a shark without
 * relying on nightly database dumps.
 *
 * This tool runs incremental table and index scans against Postgres.
 */
function SharkSpotter(opts) {
	mod_assert.object(opts, 'opts');
	mod_assert.object(opts.log, 'opts.log');
	mod_assert.string(opts.moray, 'opts.moray');
	mod_assert.string(opts.shark, 'opts.shark');
	mod_assert.string(opts.nameservice, 'opts.nameservice');
	mod_assert.number(opts.begin, 'opts.begin');
	mod_assert.number(opts.end, 'opts.end');
	mod_assert.number(opts.chunk_size, 'opts.chunk_size');

	var self = this;

	this.sf_log = opts.log;
	this.sf_moray = opts.moray;
	this.sf_shark = opts.shark;
	this.sf_nameservice = opts.nameservice;
	this.sf_begin_id = opts.begin;
	this.sf_end_id = opts.end;
	this.sf_ids_to_go = this.sf_end_id - this.sf_begin_id + 1;
	this.sf_chunk_size = opts.chunk_size;

	/* open output file in current directory */
	this.sf_outfile = mod_util.format('./%s.%d.out', this.sf_moray,
	    process.pid);
	this.sf_morayclient = mod_moray.createClient({
		'log': opts.log.child({'component': 'moray-client-' +
		    this.sf_moray}),
		'srvDomain': this.sf_moray,
		'cueballOptions': {
			'resolvers': [ this.sf_nameservice ],
			'defaultPort': 2020
		}
	});

	this.sf_write_stream = mod_fs.createWriteStream(this.sf_outfile);
	this.sf_write_stream.on('error', function (err) {
		self.sf_log.error({
			'error': err
		}, 'error writing data file');
		this.emit('error', err);
	});

	this.sf_log.info({
		'filename': this.sf_outfile
	}, 'opened output file');

	this.sf_morayclient.on('connect', function () {
		self.emit('connect');
	});

	EventEmitter.call(this);
}
mod_util.inherits(SharkSpotter, EventEmitter);

/*
 * Invoke Moray's `findobjects` in a loop, writing discovered objects to a
 * file asynchronously.
 */
SharkSpotter.prototype.find = function find() {
	var self = this;
	var resp;
	var records = [];
	var end_id, begin_id;
	var chunk_size = this.sf_chunk_size;
	var filter;
	var start_wall_time, end_wall_time;
	var duration_ms;

	begin_id = this.sf_begin_id;
	end_id = begin_id + 1; /* so we get past the initial 'whilst' check */

	function read_chunk(cb) {
		if (self.sf_ids_to_go < chunk_size) {
			chunk_size = self.sf_ids_to_go;
		}

		end_id = begin_id + chunk_size - 1;
		filter = mod_util.format(
		    '(&(_id>=%d)(_id<=%d)(type=object))',
		    begin_id, end_id);

		self.sf_log.info({
			'filter': filter,
			'moray': self.sf_moray,
			'begin_id': begin_id,
			'end_id': end_id,
			'ids_to_go': self.sf_ids_to_go
		}, 'find: begin');

		start_wall_time = new Date();
		resp = self.sf_morayclient.findObjects('manta', filter, {
			'limit': chunk_size,
			'no_count': true
		});
		records = [];

		resp.on('record', function (record) {
			var keep = false;
			var sharks = record.value.sharks;
			var stor_ids = [];
			sharks.forEach(function (sharkObj) {
				stor_ids.push(sharkObj['manta_storage_id']);

				if (sharkObj['manta_storage_id']
				    === self.sf_shark) {

					keep = true;
				}
			});
			if (keep) {
				records.push(mod_util.format('%s %s %s',
				    record.value.owner, record.value.objectId,
				    stor_ids.join(' ')));
			}
		});

		resp.on('error', function (err) {
			self.sf_log.error({
				'moray': self.sf_moray,
				'begin_id': self.sf_begin_id,
				'end_id': self.sf_end_id,
				'error': err
			}, 'find: err');
			cb(err);
		});

		resp.on('end', function () {
			end_wall_time = new Date();
			/* write the object metadata to disk asynchronously */
			self.sf_write_stream.write(records.join('\n'));
			self.sf_write_stream.write('\n');

			self.sf_log.info({
				'filter': filter,
				'moray': self.sf_moray,
				'begin_id': begin_id,
				'end_id': end_id,
				'kept': records.length,
				'discarded': chunk_size - records.length,
				'duration_ms': end_wall_time - start_wall_time
			}, 'find: end');

			self.sf_ids_to_go -= chunk_size; /* finished chunk */
			begin_id = end_id + 1; /* move to the next chunk */
			cb();
		});
	}

	this.sf_start_time = new Date();
	this.sf_log.info({
		'start_time': this.sf_start_time
	}, 'shark spotter: begin');
	mod_vasync.whilst(
		function has_ids_left() {
			return (self.sf_ids_to_go > 0);
		},
		read_chunk,
		function done(err) {
			self.sf_end_time = new Date();
			duration_ms = self.sf_end_time - self.sf_start_time;
			self.sf_log.info({
				'start_time': self.sf_start_time,
				'end_time': self.sf_end_time,
				'duration_ms': duration_ms
			}, 'shark spotter: end');
			if (err) {
				self.emit('error', err);
				return;
			}
			self.emit('end');
		});
};

/*
 * End Moray connection, flush the output file.
 */
SharkSpotter.prototype.stop = function stop() {
	var self = this;
	this.sf_log.info('stopping');
	this.sf_write_stream.end();
	this.sf_write_stream.on('finish', function () {
		self.sf_log.info('write stream finished');
		self.sf_morayclient.close();
	});
};

function usage(msg) {
	var basename = 'sharkspotter';
	if (msg) {
		console.error(msg);
	}

	console.error([
		mod_util.format('%s', basename),
		'  -b, --begin		manta relation _id to start search',
		'			from',
		'  -e, --end		manta relation _id to end search at',
		'  -d, --domain		domain name of manta services',
		'			e.g. us-east.joyent.us',
		'  -m, --moray		moray shard to search',
		'			e.g. 2.moray',
		'  -s, --shark		shark to search objects in moray for',
		'			e.g. 3.stor',
		'  -c, --chunk-size	number of objects to search in each PG',
		'			query',
		'			default is 10000'
	].join('\n'));

	process.exit(1);

}

function main() {
	var log = mod_bunyan.createLogger({'name': 'sharkspotter'});

	var opts;
	var parser;
	var moray, shark, domain, begin, end, chunk_size;

	parser = new mod_getopt.BasicParser(
	    'm:(moray)b:(begin)e:(end)d:(domain)s:(shark)c:(chunk-size)h(help)',
	    process.argv);

	while ((option = parser.getopt()) !== undefined) {
		switch (option.option) {
		case 'm':
			moray = option.optarg;
			break;
		case 'd':
			domain = option.optarg;
			break;
		case 'b':
			begin = parseInt(option.optarg, 10);
			if (begin < 0) {
				usage('invalid starting ID');
			}
			break;
		case 'e':
			end = parseInt(option.optarg, 10);
			if (end < 1) {
				usage('invalid ending ID');
			}
			break;
		case 's':
			shark = option.optarg;
			break;
		case 'c':
			chunk_size = parseInt(option.optarg, 10);
			if (chunk_size < 1) {
				usage('chunk size must be greater than 0');
			}
			break;
		case 'h':
			usage();
			break;
		default:
			mod_assert.equal(option.optarg, '?');
			usage();
			break;
		}
	}

	/* Primitive input validation. */
	if (begin > end) {
		usage('starting ID is greater than ending ID');
	}
	if (begin === undefined || end === undefined) {
		usage('must provide beginning and ending IDs');
	}
	if (moray === undefined) {
		usage('must provide moray shard to search');
	}
	if (shark === undefined) {
		usage('must provide shark to search for');
	}
	if (domain === undefined) {
		usage('must provide domain name');
	}
	/* set default chunk size of 10k */
	if (chunk_size === undefined) {
		chunk_size = 10000;
	}

	opts = {
		'log': log,
		'moray': mod_util.format('%s.%s', moray, domain),
		'shark': mod_util.format('%s.%s', shark, domain),
		'nameservice': mod_util.format('%s.%s', 'nameservice', domain),
		'begin': begin,
		'end': end,
		'chunk_size': chunk_size
	};

	var sharkspotter = new SharkSpotter(opts);

	sharkspotter.on('connect', function () {
		sharkspotter.find();
	});

	/* If we run into an error, just stop */
	sharkspotter.on('error', function (err) {
		log.error('find objects err', err);
		sharkspotter.stop();
	});

	sharkspotter.on('end', function () {
		sharkspotter.stop();
	});
}

main();
