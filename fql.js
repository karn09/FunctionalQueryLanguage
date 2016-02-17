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
};

function merge(obj1, obj2) {
  var merged = {};
  for (key in obj1) {
    merged[key] = obj1[key];
  }
  for (key in obj2) {
    merged[key] = obj2[key];
  }
  return merged;
};

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
};



function FQL(table) {
  var indicies = {};

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
    var tableArr = [];
    var curTable = this.exec();
		var that = this;
    // console.log(curTable.getIndicesOf('last_name', 'Russell'));
    // console.log(curTable.indicies)
    queryKeys.forEach(function(key) {
      var val = queryObj[key];
      if (indicies[key]) {
				that.getIndicesOf(key, val).forEach(function(idx) {
						tableArr.push(curTable[idx]);
				})

      } else {
        if (tableArr.length === 0) {
          tableArr = filterOn(curTable, key, val);
        } else {
          tableArr = filterOn(tableArr, key, val);
        }
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
  };

  this.left_join = function(table, cb) {
    var res = [];
    var left = table.exec();
    var right = this.exec();
    for (leftRow in left) {
      for (rightRow in right) {
        if (cb(right[rightRow], left[leftRow])) {
          //console.log(left[leftRow], right[rightRow])
          res.push(merge(right[rightRow], left[leftRow]));
        }
      }
    }
    return new FQL(res);
  };

  this.addIndex = function(column) {
    var table = this.exec();
    var indexedTable = [];
    for (var i = 0; i < table.length; i++) {
      indexedTable.push({
        id: i,
        [column]: table[i][column]
      });
    }
    indicies[column] = new FQL(indexedTable);
  };

  this.getIndicesOf = function(column, value) {
    if (!indicies[column]) {
      return undefined;
    }
    var idx = [];
    var table = indicies[column].exec();
    for (row in table) {
      if (table[row][column] === value) {
        idx.push(table[row].id)
      }
    }
    return idx;
  };

}


module.exports = {
  FQL: FQL,
  merge: merge,
  _readTable: _readTable
};
