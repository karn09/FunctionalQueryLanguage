var fs = require('fs');
var _ = require('lodash')

// _readTable takes a string representing a table name
// and returns an array of objects, namely the rows.
// It does so by looking up actual files, reading them,
// and parsing them from JSON strings into JS objects.
function _readTable(tableName) {
	var folderName = __dirname + '/film-database/' + tableName;
	var fileNames = fs.readdirSync(folderName);
	var fileStrings = fileNames.map(function(fileName) {
		var filePath = folderName + '/' + fileName;
		return fs.readFileSync(filePath).toString();
	});
	var table = fileStrings.map(function(fileStr) {
		return JSON.parse(fileStr);
	});
	return table;
}

function merge(obj1, obj2) {
	return _.merge(obj1, obj2);
}


function FQL(table) {
	this.exec = function() {
		return table;
	};
	this.count = function() {
		return table.length;
	};
	this.limit = function(max) {
		return new FQL(table.slice(0, max));
	};
	this.where = function(queryObj) {
		var queryKeys = Object.keys(queryObj);

		function filterOn(data, key, val) {
			var results = [];
			var newData = data.filter(function(row) {
				if (typeof val !== 'function' && row[key] === val) {
					results.push(row);
				}

				if (typeof val === 'function') {
					if (val(row[key])) {
						results.push(row);
					}
				}

			});
			return results;
		}
		var tableArr = [];
		queryKeys.forEach(function(key) {
			var val = queryObj[key];

			if (tableArr.length === 0) {
				tableArr = filterOn(table, key, val);
			} else {
				tableArr = filterOn(tableArr, key, val);
			}
		});
		return new FQL(tableArr);
	};
	this.select = function(columns) {
		return new FQL(table.map(function(row) {
			var obj = {};
			columns.forEach(function(col) {
				obj[col] = row[col];
			});
			return obj;
		}));
	};
	this.order = function(orderBy) {
		return new FQL(table.sort(function(a, b) {
			if (a[orderBy] > b[orderBy]) {
				return 1;
			}
			if (a[orderBy] < b[orderBy]) {
				return -1;
			}
			return 0;
		}));
	}
}

module.exports = {
	FQL: FQL,
	merge: merge,
	_readTable: _readTable
};