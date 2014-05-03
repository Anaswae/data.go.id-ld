var fs = require('fs');
var csv = require('csv');
var _s = require('underscore.string');
var util = require('util');
var ckan = require('ckan');
var request = require('request');
var logger = require('winston');
var async = require('async');
var _ = require('lodash');

var BmDriverBase = require('benangmerah-driver-base');

var rdfNS = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#';
var rdfsNS = 'http://www.w3.org/2000/01/rdf-schema#';
var owlNS = 'http://www.w3.org/2002/07/owl#';
var xsdNS = 'http://www.w3.org/2001/XMLSchema#';
var ontNS = 'http://benangmerah.net/ontology/';
var placeNS = 'http://benangmerah.net/place/idn/';
var bpsNS = 'http://benangmerah.net/place/idn/bps/';
var geoNS = 'http://www.w3.org/2003/01/geo/wgs84_pos#';
var qbNS = 'http://purl.org/linked-data/cube#';

function DataGoIdDriver() {}

util.inherits(DataGoIdDriver, BmDriverBase);

module.exports = DataGoIdDriver;

DataGoIdDriver.prototype.setOptions = function(options) {
  var self = this;

  self.options = _.extend({}, options);

  if (!self.options.ignoredFields) {
    // Ignore these fields, because we're using wilayah URIs
    // TODO have a flag to disable ignoring these fields
    self.options.ignoredFields = [ 'kode_provinsi', 'nama_provinsi', 'kode_kabkota', 'nama_kabkota', 'tahun' ];
  }

  if (!self.options.ckanURL) {
    self.options.ckanURL = 'http://data.ukp.go.id/';
  }
  if (!self.options.base) {
    self.options.base = self.options.ckanURL + 'dataset/' + self.options.datasetId + '#';
  }
  if (!self.options.dsd) {
    self.options.dsd = self.options.base + 'dsd';

    if (self.options.generateDSD) {
      self.generateDSD(self.options);
    }
  }
}

DataGoIdDriver.prototype.fetchFromCkan = function(callback) {
  var self = this;

  logger.info('Fetching from CKAN...');

  var client = new ckan.Client(self.options.ckanURL);

  client.action('package_show', { id: self.options.datasetId }, function(err, data) {
    if (err) {
      callback(err);
    }
    else {
      var resources = data.result.resources;
      // The CSV we want should be at index 0
      var firstResource = resources[0];

      self.options.csvUrl = firstResource.url;
      self.options.datasetMeta = data.result;
      self.options.org = data.result.organization;

      callback();
    }
  });
}

DataGoIdDriver.prototype.getCsvString = function(callback) {
  var self = this;

  logger.info('Fetching CSV contents...');

  request(self.options.csvUrl, function(err, response, body) {
    if (err) {
      callback(err);
    }
    else {
      self.options.csvString = body;
      callback();
    }
  })
}

DataGoIdDriver.prototype.makeRowObject = function(rowArray, headerArray) {
  var rowObject = {};
  rowArray.forEach(function(value, idx) {
    var key = headerArray[idx];
    rowObject[key] = value;
  });

  return rowObject;
}

DataGoIdDriver.prototype.getRowObjects = function(callback) {
  var self = this;
  
  logger.info('Parsing CSV...');

  csv()
  .from.string(self.options.csvString)
  .to.array(function(rows) {
    self.options.headerArray = rows.shift();
    self.options.rowObjects = [];

    rows.forEach(function(row) {
      var rowObject = self.makeRowObject(row, self.options.headerArray);
      self.options.rowObjects.push(rowObject);
    });

    callback();
  })
}

DataGoIdDriver.prototype.generateDSD = function() {
  var self = this;
  
  var base = self.options.base;
  var triples = self.options.triples;

  self.addTriple(base + 'dsd', rdfNS + 'type', qbNS + 'DataStructureDefinition');
  self.addTriple('_:dsd-refArea', qbNS + 'dimension', ontNS + 'refArea');
  self.addTriple('_:dsd-refArea', qbNS + 'order', '"1"');

  var order = 2;

  self.options.headerArray.forEach(function(header) {
    if (self.options.ignoredFields.indexOf(header) === -1) {
      self.addTriple(base + 'dsd', qbNS + 'component', '_:dsd-' + header);
      self.addTriple('_:dsd-' + header, qbNS + 'measure', base + header);
      self.addTriple('_:dsd-' + header, qbNS + 'order', '"' + order + '"');

      self.addTriple(base + header, rdfNS + 'type', owlNS + 'DatatypeProperty');
      self.addTriple(base + header, rdfNS + 'type', qbNS + 'MeasureProperty');
    }
  });
}

DataGoIdDriver.prototype.initDataset = function(callback) {
  var self = this;
  
  logger.info('Adding dataset definition...');

  var base = self.options.base;
  var triples = self.options.triples;

  self.addTriple(base, rdfNS + 'type', qbNS + 'DataSet');
  self.addTriple(base, rdfsNS + 'label', '"' + self.options.datasetMeta.title + '"');
  self.addTriple(base, rdfsNS + 'comment', '"' + self.options.datasetMeta.notes + '"');
  self.addTriple(base, qbNS + 'structure', self.options.dsd);

  callback();
}

DataGoIdDriver.prototype.addObservation = function(rowObject, idx) {
  var self = this;

  var base = self.options.base;
  var triples = self.options.triples;

  if (self.options.generateObservationURI) {
    var observationURI = self.options.generateObservationURI(rowObject, idx);
  }
  else {
    var observationURI = base + 'observation/' + idx;
  }

  self.addTriple(observationURI, rdfNS + 'type', qbNS + 'Observation');
  self.addTriple(observationURI, qbNS + 'dataSet', base);
  self.addTriple(observationURI, ontNS + 'refArea', bpsNS + rowObject.kode_kabkota);

  if (rowObject.tahun && self.options.ignoredFields.indexOf('tahun') !== -1) {
    self.addTriple(observationURI, ontNS + 'refPeriod', '"' + rowObject.tahun + '"^^<' + xsdNS + 'gYear>');
  }

  Object.keys(rowObject).forEach(function(key) {
    if (self.options.ignoredFields.indexOf(key) === -1) {
      if (self.options.transformValue) {
        var value = self.options.transformValue(key, rowObject[key]);
      }
      else {
        var value = '"' + rowObject[key] + '"';
      }
      self.addTriple(observationURI, base + key, value);
    }
  });
}

DataGoIdDriver.prototype.addObservations = function(callback) {
  var self = this;
  
  logger.info('Adding observations...');

  self.options.rowObjects.forEach(function(rowObject, idx) {
    self.addObservation(rowObject, idx, self.options);
  });

  callback();
}

DataGoIdDriver.prototype.fetch = function() {
  var self = this;

  if (!self.options.datasetId) {
    self.error('No dataset was specified.');
    return;
  }

  async.waterfall([
    self.fetchFromCkan.bind(self),
    self.getCsvString.bind(self),
    self.getRowObjects.bind(self),
    self.initDataset.bind(self),
    self.addObservations.bind(self)
  ], function(err, params) {
    if (err) {
      self.error(err);
    }
    else {
      self.finish();
    }
  });
}

BmDriverBase.handleCLI({});