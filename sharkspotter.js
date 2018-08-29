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
var mod_jsprim = require('jsprim');
var mod_moray = require('moray');
var mod_vasync = require('vasync');

var mod_fs = require('fs');
var mod_util = require('util');
var EventEmitter = require('events').EventEmitter;

/*
 * sharkspotter.js - find objects that should belong on a shark
 *
 * The user provides a shark name (e.g. 2.stor) and a Moray shard name
 * (e.g. 3.moray). This script uses Moray's `sql` RPC to query the backend
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
	mod_assert.optionalNumber(opts.begin, 'opts.begin');
	mod_assert.optionalNumber(opts.end, 'opts.end');
	mod_assert.number(opts.chunk_size, 'opts.chunk_size');

	var self = this;

	this.sf_log = opts.log;
	this.sf_moray = opts.moray;
	this.sf_shark = opts.shark;
	this.sf_nameservice = opts.nameservice;
	this.sf_chunk_size = opts.chunk_size;
	this.sf_begin_id = opts.begin || 0;
	this.sf_end_id = opts.end;

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
		self.emit('error', err);
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
	var query;
	var start_wall_time, end_wall_time;
	var duration_ms;
	var id_column;


	function read_chunk(cb) {
		var records_seen = 0;
		chunk_size = self.sf_chunk_size;
		if (self.sf_ids_to_go < chunk_size) {
			/* there is only a partial chunk left */
			chunk_size = self.sf_ids_to_go;
		}

		end_id = begin_id + chunk_size - 1;
		query = mod_util.format('SELECT * FROM manta WHERE' +
		    ' %s >= %d AND %s <= %d AND type = \'object\';',
		    id_column, begin_id, id_column, end_id);

		self.sf_log.info({
			'query': query,
			'moray': self.sf_moray,
			'begin_id': begin_id,
			'end_id': end_id,
			'chunk_size': chunk_size,
			'ids_to_go': self.sf_ids_to_go,
			'id_column': id_column,
			'last_id': self.sf_end_id
		}, 'find %s: begin', id_column);

		start_wall_time = new Date();
		resp = self.sf_morayclient.sql(query, {
			'limit': chunk_size,
			'no_count': true
		});

		/*
		 * decide whether or not to record an object that we receive
		 * from moray
		 */
		function recordObject (record) {
			records_seen++;
			self.sf_log.debug({
				'moray': self.sf_moray,
				'records_seen': records_seen,
				'last_id': self.sf_end_id,
				'begin_id': begin_id,
				'end_id': end_id,
				'chunk_size': chunk_size
			}, 'record received');

			/* exit early if no mention of the target shark */
			if (record._value.indexOf(self.sf_shark) < 0) {
				return;
			}

			var value = JSON.parse(record._value);
			var sharks = value.sharks;

			var keep = false;
			var stor_ids = [];
			var arg = {
				'stor_ids': stor_ids,
				'keep': keep
			};
			sharks.forEach(checkForShark, arg);

			if (arg.keep) {
				records.push(mod_util.format('%s %s %s',
				    value.owner, value.objectId,
				    arg.stor_ids.join(' ')));
			}
		}

		/*
		 * look at the received record. If it includes the target shark
		 * in the storage ID array then mark that we would like to
		 * record the object.
		 */
		function checkForShark(sharkObj) {
			this.stor_ids.push(sharkObj['manta_storage_id']);

			if (sharkObj['manta_storage_id']
			    === self.sf_shark) {

				this.keep = true;
			}
		}

		resp.on('record', recordObject);

		resp.on('error', function (err) {
			end_wall_time = new Date();
			self.sf_log.error({
				'moray': self.sf_moray,
				'begin_id': begin_id,
				'end_id': end_id,
				'duration_ms': end_wall_time - start_wall_time,
				'error': err
			}, 'find %s: err', id_column);
			cb(err);
		});

		resp.on('end', function () {
			end_wall_time = new Date();
			/* write the object metadata to disk asynchronously */
			if (records.length > 0) {
				self.sf_write_stream.write(records.join('\n'));
				self.sf_write_stream.write('\n');
				records = [];
			}

			self.sf_ids_to_go -= chunk_size; /* finished chunk */

			self.sf_log.info({
				'query': query,
				'moray': self.sf_moray,
				'begin_id': begin_id,
				'end_id': end_id,
				'kept': records.length,
				'id_column': id_column,
				'ids_to_go': self.sf_ids_to_go,
				'discarded': chunk_size - records.length,
				'duration_ms': end_wall_time - start_wall_time,
				'last_id': self.sf_end_id
			}, 'find %s: end', id_column);

			begin_id = end_id + 1; /* move to the next chunk */
			cb();
		});
	}
	
	function find_largest__id_value(_, cb) {
		self.get_largest_id('_id', function (err, max) {
			if (err) {
				self.sf_log.fatal({
					'error': err
				}, 'could not get max _id value');
			} else {
				if (typeof(max) === 'number') {
					self.sf_max__id = max;
				} else {
					self.sf_max__id =
					    mod_jsprim.parseInteger(max);
				}
			}
			cb(err);
		});
	}

	function find_largest__idx_value(_, cb) {
		self.get_largest_id('_idx',
		    function (err, max) {

			if (err) {
				/* _idx won't exist in all Manta deployments */
				self.sf_log.warn({
					'error': err
				}, 'could not get max _idx value');
			} else {
				self.sf_max__idx = mod_jsprim.parseInteger(max);
			}
			cb();
		});
	}

	function iterate__ids(_, cb) {
		id_column = '_id';

		/* user didn't specify an end ID */
		if (self.sf_end_id === undefined) {
			/* pick the larger of _id and _idx */
			self.sf_end_id = self.sf_max__idx ?
			    self.sf_max__idx : self.sf_max__id;
		}
		begin_id = self.sf_begin_id;
		end_id = begin_id + 1; /* get past the initial 'whilst' check */

		var start_time = new Date();

		if (self.sf_end_id > self.sf_max__id) {
			/* user is scanning outside the range of _id values */
			self.sf_ids_to_go = self.sf_max__id -
			    self.sf_begin_id + 1;
		} else {
			/* user is scanning in the range of the _id values */
			self.sf_ids_to_go = self.sf_end_id -
			    self.sf_begin_id + 1;
		}
		self.sf_log.info({
			'start_time': start_time
		}, 'shark spotter _id: begin');

		mod_vasync.whilst(
			function has__ids_left() {
				return (self.sf_ids_to_go > 0);
			},
			read_chunk,
			function done(err) {
				end_time = new Date();
				duration_ms = end_time - start_time;
				self.sf_log.info({
					'start_time': start_time,
					'end_time': end_time,
					'duration_ms': duration_ms
				}, 'shark spotter _id: end');
				cb(err);
			});
	}

	function iterate__idxs(_, cb) {
		id_column = '_idx';
		var start_time = new Date();
		self.sf_log.info({
			'start_time': start_time
		}, 'shark spotter _idx: begin');

		/*
		 * we want this to scan from the greater of (max__id + 1)
		 * and (start_id).
		 */
		if (self.sf_max__id + 1 > self.sf_begin_id) {
			begin_id = self.sf_max__id + 1;
		} else {
			begin_id = self.sf_begin_id;
		}

		/*
		 * don't iterate through the _idx column if the user wants to
		 * stop before the _idx range begins
		 */
		if (begin_id > self.sf_end_id) {
			cb();
			return;
		}

		self.sf_ids_to_go = self.sf_max__idx - begin_id + 1;
		mod_vasync.whilst(
			function has__idxs_left() {
				return (self.sf_ids_to_go > 0);
			},
			read_chunk,
			function done(err) {
				end_time = new Date();
				duration_ms = end_time - start_time;
				self.sf_log.info({
					'start_time': start_time,
					'end_time': end_time,
					'duration_ms': duration_ms
				}, 'shark spotter _idx: end');
				cb(err);
			});
	}

	/*
	 * find the boundaries of _id and _idx, then iterate through them
	 * in succession
	 */
	mod_vasync.pipeline({
		'funcs': [
			find_largest__id_value,
			find_largest__idx_value,
			iterate__ids,
			iterate__idxs
		]
	}, function (err, results) {
		if (err) {
			self.sf_log.error({
				'error': err
			}, 'error finding objects');
			self.emit('error', err);
			return;
		}
		self.emit('end');
	});


};

SharkSpotter.prototype.get_largest_id = function get_largest_id(idstr, cb) {
	mod_assert.string(idstr, 'idstr');

	var resp, res;
	var self = this;

	var query = mod_util.format('SELECT MAX(%s) FROM manta;', idstr);
	resp = this.sf_morayclient.sql(query, {
		'limit': 1,
		'no_count': true
	});

	/* we only want one row, so only listen for this event once */
	resp.once('record', function (record) {
		res = record['max'];
	});

	resp.on('error', function (err) {
		cb(err);
	});

	resp.on('end', function () {
		cb(null, res);
	});
	
};

/*
 * End Moray connection, flush the output file.
 */
SharkSpotter.prototype.stop = function stop() {
	var self = this;
	this.sf_log.info('stopping');
	this.sf_morayclient.close();
	this.sf_write_stream.on('finish', function () {
		self.sf_log.info('write stream finished');
	});
	this.sf_write_stream.end();
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
		'			default is 0',
		'  -e, --end		manta relation _id to end search at',
		'			default is the larger of max(_id) and',
		'			max(_idx)',
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
	var start_time;

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

	/* Very primitive input validation. */
	if (begin > end) {
		usage('starting ID is greater than ending ID');
	}
	/*if (begin === undefined || end === undefined) {
		usage('must provide beginning and ending IDs');
	}*/
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
		start_time = new Date();
		log.info({
			'start_time': start_time,
			'options': opts
		}, 'sharkspotter: begin');
		sharkspotter.find();
	});

	/* If we run into an error, just stop */
	sharkspotter.on('error', function (err) {
		log.error('find objects err', err);
		sharkspotter.stop();
	});

	sharkspotter.on('end', function () {
		end_time = new Date();
		sharkspotter.stop();
		log.info({
			'end_time': end_time,
			'duration_ms': end_time - start_time
		}, 'sharkspotter: done');
	});
}

main();
