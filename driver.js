var fs = require('fs');
var _s = require('underscore.string');
var util = require('util');
var ckan = require('ckan');
var request = require('request');
var async = require('async');
var _ = require('lodash');
var csvParser = require('csv-parser');

var BmDriverBase = require('benangmerah-driver-base');

var RDF_NS = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#';
var RDFS_NS = 'http://www.w3.org/2000/01/rdf-schema#';
var OWL_NS = 'http://www.w3.org/2002/07/owl#';
var XSD_NS = 'http://www.w3.org/2001/XMLSchema#';
var BM_NS = 'http://benangmerah.net/ontology/';
var PLACE_NS = 'http://benangmerah.net/place/idn/';
var BPS_NS = 'http://benangmerah.net/place/idn/bps/';
var GEO_NS = 'http://www.w3.org/2003/01/geo/wgs84_pos#';
var QB_NS = 'http://purl.org/linked-data/cube#';

function DataGoIdDriver() {}

util.inherits(DataGoIdDriver, BmDriverBase);

module.exports = DataGoIdDriver;

DataGoIdDriver.prototype.setOptions = function(options) {
  var self = this;

  self.options = _.extend({}, options);

  if (!self.options.ignoredFields) {
    // Ignore these fields, because we're using wilayah URIs
    // TODO have a flag to disable ignoring these fields
    self.options.ignoredFields =
      [ 'kode_provinsi', 'nama_provinsi', 'kode_kabkota', 'koordinat_provinsi',
        'koordinat_kabkota', 'nama_kabkota', 'tahun' ];
  }

  if (!self.options.ckanURL) {
    self.options.ckanURL = 'http://data.ukp.go.id/';
  }
  if (!self.options.base) {
    self.options.base = self.options.ckanURL + 'dataset/' +
                        self.options.datasetId + '#';
  }
  if (!self.options.dsd) {
    self.options.dsd = self.options.base + 'dsd';
  }

  self.info(self.options);
};

DataGoIdDriver.prototype.fetchFromCkan = function(callback) {
  var self = this;

  self.info('Fetching from CKAN...');

  var client = new ckan.Client(self.options.ckanURL);

  client.action('package_show', { id: self.options.datasetId },
    function(err, data) {
      if (err) {
        callback(err);
      }
      else {
        var resources = data.result.resources;
        // The CSV we want should be at index 0
        var firstResource = resources[0];

        self.csvUrl = firstResource.url;
        self.datasetMeta = data.result;
        self.org = data.result.organization;

        callback();
      }
    });
};

DataGoIdDriver.prototype.generateDSD = function(callback) {
  var self = this;

  if (!self.options.generateDSD) {
    return callback();
  }
  
  self.info('Generating data structure definition...');

  var base = self.options.base;
  var dsdUri = base + 'dsd';

  self.addTriple(dsdUri, RDF_NS + 'type',
                 QB_NS + 'DataStructureDefinition');
  self.addTriple('_:dsd-refArea', QB_NS + 'dimension', BM_NS + 'refArea');
  self.addTriple('_:dsd-refArea', QB_NS + 'order', '"1"');

  var order = 2;

  self.headerArray.forEach(function(header) {
    if (self.options.ignoredFields.indexOf(header) === -1) {
      self.addTriple(dsdUri, QB_NS + 'component', '_:dsd-' + header);
      self.addTriple('_:dsd-' + header, QB_NS + 'measure', base + header);
      self.addTriple('_:dsd-' + header, QB_NS + 'order', '"' + order + '"');

      self.addTriple(base + header, RDF_NS + 'type',
                     OWL_NS + 'DatatypeProperty');
      self.addTriple(base + header, RDF_NS + 'type', QB_NS + 'MeasureProperty');
      self.addTriple(base + header, RDFS_NS + 'label',
                     '"' + _s.titleize(_s.humanize(header)) + '"');
    }
    if (header === 'tahun') {
      self.addTriple(dsdUri, QB_NS + 'component', '_:dsd-' + header);
      self.addTriple('_:dsd-' + header, QB_NS + 'dimension',
                     BM_NS + 'refPeriod');
      self.addTriple('_:dsd-' + header, QB_NS + 'order', '"' + order + '"');
    }
  });

  callback();
};

DataGoIdDriver.prototype.initDataset = function(callback) {
  var self = this;
  
  self.info('Adding dataset definition...');

  var base = self.options.base;

  self.addTriple(base, RDF_NS + 'type', QB_NS + 'DataSet');
  self.addTriple(base, RDFS_NS + 'label',
                 '"' + self.datasetMeta.title + '"');
  self.addTriple(base, RDFS_NS + 'comment',
                 '"' + self.datasetMeta.notes + '"');
  self.addTriple(base, QB_NS + 'structure', self.options.dsd);

  callback();
};

DataGoIdDriver.prototype.addObservation = function(rowObject, idx) {
  var self = this;

  var base = self.options.base;

  var observationURI;
  if (self.options.generateObservationURI) {
    observationURI = self.options.generateObservationURI(rowObject, idx);
  }
  else {
    observationURI = base + 'observation/' + idx;
  }

  self.addTriple(observationURI, RDF_NS + 'type', QB_NS + 'Observation');
  self.addTriple(observationURI, QB_NS + 'dataSet', base);
  self.addTriple(observationURI, BM_NS + 'refArea',
                 BPS_NS + rowObject.kode_kabkota);

  if (rowObject.tahun && self.options.ignoredFields.indexOf('tahun') !== -1) {
    self.addTriple(observationURI, BM_NS + 'refPeriod',
                   '"' + rowObject.tahun + '"^^<' + XSD_NS + 'gYear>');
  }

  Object.keys(rowObject).forEach(function(key) {
    if (self.options.ignoredFields.indexOf(key) === -1) {
      var value;
      if (self.options.transformValue) {
        value = self.options.transformValue(key, rowObject[key]);
      }
      else {
        value = '"' + rowObject[key] + '"';
      }
      self.addTriple(observationURI, base + key, value);
    }
  });
};

DataGoIdDriver.prototype.addObservations = function(callback) {
  var self = this;
  
  self.info('Fetching from CSV and adding observations...');

  var i = 0;
  request(self.csvUrl)
    .pipe(csvParser())
    .once('data', function(firstRow) {
      self.headerArray = Object.keys(firstRow);
    })
    .on('data', function(row) {
      self.addObservation(row, ++i);
    })
    .on('end', callback)
    .on('error', callback);
};

DataGoIdDriver.prototype.fetch = function() {
  var self = this;

  if (!self.options.datasetId) {
    self.error('No dataset was specified.');
    return;
  }

  async.waterfall([
    self.fetchFromCkan.bind(self),
    self.initDataset.bind(self),
    self.addObservations.bind(self),
    self.generateDSD.bind(self)
  ], function(err, params) {
    if (err) {
      self.error(err);
    }
    else {
      self.finish();
    }
  });
};

BmDriverBase.handleCLI({});