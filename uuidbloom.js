/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2019 Joyent, Inc.
 */

var mod_fs = require('fs');
var mod_vasync = require('vasync');

var BITS_IN_ADDRESS = 32;
var BITS_IN_MAP = Math.pow(2, BITS_IN_ADDRESS);
var BYTES_IN_MAP = BITS_IN_MAP / 8;

function uuidToAddresses(uuid) {
	var hexuuid = uuid.replace(/[^a-f0-9A-F]/g, '');
	var b = new Buffer(hexuuid, 'hex');
	if (b.length !== 16) {
		throw (new Error('UUID must be 16 bytes (128 bits), but ' +
		    'input is ' + b.length + ' bytes long ("' + uuid + '")'));
	}
	return ([
		b.readUInt32BE(0),
		b.readUInt32BE(4),
		b.readUInt32BE(8),
		b.readUInt32BE(12)
	]);
}

function UUIDBloomFilter(opts) {
	if (typeof (opts.path) !== 'string')
		throw (new Error('opts.path must be a string'));
	this.ubf_path = opts.path;
	try {
		this.ubf_fd = mod_fs.openSync(opts.path, 'wx+');
	} catch (_e) {
		this.ubf_fd = mod_fs.openSync(opts.path, 'r+');
	}
	var tempBuf = new Buffer(1);
	tempBuf[0] = 0;
	mod_fs.writeSync(this.ubf_fd, tempBuf, 0, 1, BYTES_IN_MAP);
}

UUIDBloomFilter.prototype.add = function (uuid) {
	var fd = this.ubf_fd;
	var addrs = uuidToAddresses(uuid);
	var tempBuf = new Buffer(1);
	addrs.forEach(function (addr) {
		var byteOffset = addr / 8;
		var bitOffset = addr % 8;
		mod_fs.readSync(fd, tempBuf, 0, 1, byteOffset);
		tempBuf[0] |= (1 << bitOffset);
		mod_fs.writeSync(fd, tempBuf, 0, 1, byteOffset);
	});
};

UUIDBloomFilter.prototype.check = function (uuid, cb) {
	var fd = this.ubf_fd;
	var addrs = uuidToAddresses(uuid);
	var tempBuf = new Buffer(1);
	var count = 0;
	addrs.forEach(function (addr) {
		var byteOffset = addr / 8;
		var bitOffset = addr % 8;
		mod_fs.readSync(fd, tempBuf, 0, 1, byteOffset);
		if (tempBuf[0] & (1 << bitOffset))
			++count;
	});
	if (count == addrs.length)
		return (true);
	return (false);
};

UUIDBloomFilter.prototype.checkAsync = function (uuid, cb) {
	var fd = this.ubf_fd;
	var addrs = uuidToAddresses(uuid);
	var count = 0;
	mod_vasync.forEachParallel({
		inputs: addrs,
		func: checkAddr
	}, function (err, res) {
		if (err) {
			cb(err);
			return;
		}
		if (count === addrs.length)
			cb(null, true);
		else
			cb(null, false);
	});
	function checkAddr(addr, addrCb) {
		var tempBuf = new Buffer(1);
		var byteOffset = addr / 8;
		var bitOffset = addr % 8;
		mod_fs.read(fd, tempBuf, 0, 1, byteOffset, function (err) {
			if (err) {
				addrCb(err);
				return;
			}
			if (tempBuf[0] & (1 << bitOffset))
				++count;
			addrCb();
		});
	}
};

UUIDBloomFilter.prototype.close = function () {
	mod_fs.closeSync(this.ubf_fd);
};

module.exports = UUIDBloomFilter;

