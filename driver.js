var fs = require('fs');
var _s = require('underscore.string');
var util = require('util');
var ckan = require('ckan');
var request = require('request');
var async = require('async');
var _ = require('lodash');
var csvParser = require('csv-parser');
var isNumeric = require('isnumeric');

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
var ORG_NS = 'http://www.w3.org/ns/org#';
var DCT_NS = 'http://purl.org/dc/terms/';
var SKOS_NS = 'http://www.w3.org/2004/02/skos/core#';

var yearRegex = /^[0-9]{4}$/;
var yearMonthRegex = /^[0-9]{4}-[0-9]{2}$/;

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
        'koordinat_kabkota', 'nama_kabkota', 'tahun', 'bulan',
        '', 'orderer', 'latitude', 'longitude' ];
  }

  if (!self.options.ckanURL) {
    self.options.ckanURL = 'http://data.ukp.go.id/';
  }
  if (!self.options.base) {
    self.options.base = self.options.ckanURL + 'dataset/' +
                        self.options.datasetId + '#';
  }
  if (!self.options.dsd) {
    self.options.dsd = self.options.base + '_dsd';
  }
  if (_.isUndefined(self.options.generateDSD)) {
    self.options.generateDSD = true;
  }

  self.datasetUri = self.options.ckanURL + 'dataset/' +
                    self.options.datasetId;

  self.info(self.options);
};

DataGoIdDriver.prototype.fetch = function() {
  var self = this;

  if (!self.options.datasetId) {
    self.error('No dataset was specified.');
    return;
  }

  async.series([
    self.fetchFromCkan.bind(self),
    self.addMeta.bind(self),
    self.fetchCsv.bind(self)
  ], function(err, params) {
    if (err) {
      self.error(err);
    }
    else {
      self.finish();
    }
  });
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

        callback();
      }
    });
};

DataGoIdDriver.prototype.addMeta = function(callback) {
  var self = this;

  self.info('Generating dataset metadata...');

  var datasetUri = self.datasetUri;
  var meta = self.datasetMeta;

  self.addTriple(datasetUri, RDF_NS + 'type', QB_NS + 'DataSet');
  self.addTriple(datasetUri, RDFS_NS + 'label', '"' + meta.title + '"');
  self.addTriple(datasetUri, RDFS_NS + 'comment', '"' + meta.notes + '"');
  self.addTriple(datasetUri, QB_NS + 'structure', self.options.dsd);

  // Dates
  self.addTriple(datasetUri, DCT_NS + 'modified',
    '"' + meta.metadata_modified + '"');

  // License
  self.addTriple(datasetUri, DCT_NS + 'license', meta.license_url);
  self.addTriple(meta.license_url, RDFS_NS + 'label', 
    '"' + meta.license_title + '"');

  // Publishing organization
  var orgBase = self.options.ckanURL + 'organization/';

  var org = meta.organization;
  var orgUri = orgBase + org.name;

  self.addTriple(datasetUri, DCT_NS + 'publisher', orgUri);
  self.addTriple(orgUri, RDF_NS + 'type', ORG_NS + 'Organization');
  self.addTriple(orgUri, RDFS_NS + 'label', '"' + org.title + '"');
  self.addTriple(orgUri, RDFS_NS + 'comment', '"' + org.description + '"');

  // Groups
  var groupBase = self.options.ckanURL + 'group/';

  _.forEach(meta.groups, function(group) {
    var groupUri = groupBase + group.name;

    self.addTriple(datasetUri, DCT_NS + 'subject', groupUri);
    self.addTriple(groupUri, RDF_NS + 'type', BM_NS + 'Topic');
    self.addTriple(groupUri, RDFS_NS + 'label',
      '"' + group.title + '"');
    self.addTriple(groupUri, RDFS_NS + 'comment',
      '"' + group.description + '"');
  });

  // Tags
  var tagBase = self.options.ckanURL + 'dataset?tags=';

  _.forEach(meta.groups, function(tag) {
    var tagUri = tagBase + encodeURIComponent(tag.name);

    self.addTriple(datasetUri, DCT_NS + 'subject', tagUri);
    self.addTriple(tagUri, RDF_NS + 'type', BM_NS + 'Tag');
    self.addTriple(tagUri, RDFS_NS + 'label', '"' + tag.display_name + '"');
  });

  // Extras
  var extraBase = self.options.ckanURL + '#meta-';

  _.forEach(meta.extras, function(extra) {
    var extraUri = extraBase + _s.dasherize(extra.key.toLowerCase());
    self.addTriple(datasetUri, extraUri, '"' + extra.value + '"');
    self.addTriple(extraUri, RDFS_NS + 'label', '"' + extra.key + '"');

    if (extra.key === 'Rujukan' && _s.startsWith(extra.value, 'http')) {
      self.addTriple(datasetUri, RDFS_NS + 'seeAlso', extra.value);
    }
  });

  callback();
};

DataGoIdDriver.prototype.fetchCsv = function(callback) {
  var self = this;
  
  self.info('Fetching from CSV and adding observations...');

  var i = 0;
  request(self.csvUrl)
    .pipe(csvParser())
    .once('data', function(firstRow) {
      self.addDsd(firstRow);
    })
    .on('data', function(row) {
      self.addObservation(row, ++i);
    })
    .on('end', callback)
    .on('error', callback);
};

DataGoIdDriver.prototype.addDsd = function(firstRow) {
  var self = this;

  if (self.options.generateDSD === false) {
    return;
  }

  self.info('Generating data structure definition...');

  var ignoredFields = self.options.ignoredFields;
  var headerArray = Object.keys(firstRow);

  var dimensions = [];

  _.forEach(firstRow, function(value, idx) {
    if (!_.contains(ignoredFields, idx) && !isNumeric(value)) {
      dimensions.push(idx);
    }
  });

  var base = self.options.base;
  var dsdUri = base + '_dsd';

  self.addTriple(dsdUri, RDF_NS + 'type',
                 QB_NS + 'DataStructureDefinition');

  var order = 0;

  if (_.contains(headerArray, 'kode_provinsi') ||
      _.contains(headerArray, 'kode_kabkota')) {
    self.addTriple(
      dsdUri + '-refArea', QB_NS + 'dimension', BM_NS + 'refArea');
    self.addTriple(
      dsdUri + '-refArea', QB_NS + 'order', '"' + order + '"');
    ++order;
  }
  if (_.contains(headerArray, 'tahun')) {
      self.addTriple(
        dsdUri, QB_NS + 'component', dsdUri + '-refPeriod');
      self.addTriple(
        dsdUri + '-refPeriod', QB_NS + 'dimension', BM_NS + 'refPeriod');
      self.addTriple(
        dsdUri + '-refPeriod', QB_NS + 'order', '"' + order + '"');
    ++order;
  }

  _.forEach(headerArray, function(header) {
    if (_.contains(ignoredFields, header)) {
      return;
    }
    var componentType = 'MeasureProperty';
    var componentAttribute = 'measure';
    if (_.contains(dimensions, header)) {
      componentType = 'DimensionProperty';
      componentAttribute = 'dimension';
    }

    self.addTriple(dsdUri, QB_NS + 'component', dsdUri + '-' + header);
    self.addTriple(dsdUri + '-' + header, QB_NS + componentAttribute,
                   base + header);
    self.addTriple(dsdUri + '-' + header, QB_NS + 'order',
                   '"' + order + '"');

    self.addTriple(base + header, RDF_NS + 'type', QB_NS + componentType);

    // This won't resolve since qb:MeasureProperty
    // isn't related to owl:DatatypeProperty
    // self.addTriple(base + header, RDF_NS + 'type',
    //                OWL_NS + 'DatatypeProperty');

    var humanTitle = _s.humanize(header).split(' ');
    humanTitle = _.map(humanTitle, function(word) {
      var isAbbreviation =
        !!word.match(/^([bcdfghjklmnpqrstvwxyz]+|[a-z]{1,3})$/i);
      return isAbbreviation ? word.toUpperCase() : _s.titleize(word);
    });
    humanTitle = humanTitle.join(' ');
    self.addTriple(base + header, RDFS_NS + 'label', '"' + humanTitle + '"');

    ++order;
  });
};

DataGoIdDriver.prototype.addObservation = function(rowObject, idx) {
  var self = this;

  var base = self.options.base;
  var datasetUri = self.datasetUri;

  var observationURI;
  if (self.options.generateObservationURI) {
    observationURI = self.options.generateObservationURI(rowObject, idx);
  }
  else {
    observationURI = base + '_' + idx;
  }

  self.addTriple(observationURI, RDF_NS + 'type', QB_NS + 'Observation');
  self.addTriple(observationURI, QB_NS + 'dataSet', datasetUri);

  if (rowObject.kode_kabkota) {
    self.addTriple(observationURI, BM_NS + 'refArea',
                   BPS_NS + rowObject.kode_kabkota);
  }
  else if (rowObject.kode_provinsi) {
    self.addTriple(observationURI, BM_NS + 'refArea',
                   BPS_NS + rowObject.kode_provinsi);
  }

  if (rowObject.tahun && rowObject.bulan) {
    var period = rowObject.tahun + '-' + _s.pad(rowObject.bulan, 2, '0');
    self.addTriple(observationURI, BM_NS + 'refPeriod',
                   '"' + period + '"^^<' + XSD_NS + 'gYearMonth>');
  }
  else if (rowObject.tahun) {
    if (yearRegex.test(rowObject.tahun)) {
      self.addTriple(observationURI, BM_NS + 'refPeriod',
                     '"' + rowObject.tahun + '"^^<' + XSD_NS + 'gYear>');
    }
    else {
      self.addTriple(observationURI, BM_NS + 'refPeriod',
                     '"' + rowObject.tahun + '"');
    }
  }

  Object.keys(rowObject).forEach(function(key) {
    if (self.options.ignoredFields.indexOf(key) === -1) {
      var value;
      if (self.options.transformValue) {
        value = self.options.transformValue(key, rowObject[key]);
      }
      else if (isNumeric(rowObject[key])) {
        var numericValue = parseFloat(rowObject[key]).toString();
        value = '"' + numericValue + '"^^<' + XSD_NS + 'decimal>';
      }
      else {
        value = '"' + rowObject[key] + '"';
      }
      self.addTriple(observationURI, base + key, value);
    }
  });
};

BmDriverBase.handleCLI();