/**
 * Couchbase persistent storage adapter for Harō
 *
 * @author Jason Mulligan <jason.mulligan@avoidwork.com>
 * @copyright 2015
 * @license BSD-3-Clause
 * @link https://github.com/avoidwork/haro-couchbase
 * @version 1.0.2
 */
"use strict";

var Map = require("es6-map");
var Cluster = require("couchbase").Cluster;
var deferred = require("tiny-defer");

var registry = new Map();

function getClient(id, cfg) {
	if (!registry.has(id)) {
		registry.set(id, new Cluster(cfg).openBucket(id));
	}

	return registry.get(id);
}

function adapter(store, op, key, data) {
	var defer = deferred(),
	    record = key !== undefined,
	    config = store.adapters.couchbase,
	    prefix = config.prefix || store.id,
	    lkey = prefix + (record ? "_" + key : ""),
	    client = getClient(store.id, config.cluster);

	if (op === "get") {
		client.get(lkey, function (e, result) {
			if (e) {
				defer.reject(e);
			} else if (result.value) {
				defer.resolve(result.value);
			} else if (record) {
				defer.reject(new Error("Record not found in couchbase"));
			} else {
				defer.resolve([]);
			}
		});
	} else if (op === "remove") {
		client.remove(lkey, {}, function (e) {
			if (e) {
				defer.reject(e);
			} else {
				defer.resolve(true);
			}
		});
	} else if (op === "set") {
		client.upsert(lkey, data, {}, function (e) {
			if (e) {
				defer.reject(e);
			} else {
				defer.resolve(true);
			}
		});
	}

	return defer.promise;
}

module.exports = adapter;
