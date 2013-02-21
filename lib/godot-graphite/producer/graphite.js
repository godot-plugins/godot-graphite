/*
 * graphite.js: Producer event from Graphite.
 *
 * @obazoud
 *
 *
 */

var utile       = require('utile'),
    https       = require('https'),
    http        = require('http'),
    path        = require('path');

godotPath       = path.dirname(require.resolve('godot'));
Producer        = require(godotPath + '/godot/producer').Producer;

//
// ### function Graphite (options)
// #### @options {Object} Options for fetching data to Graphite.
// ####   @options.event      {Object} Event stereotype.
// ####   @options.request    {Object} Request configuration to access graphite webapp.
// ####   @options.graphite   {Object} Graphite metric to fetch.
// Constructor function for the Graphite object responsible
// for creating events to process.
//
var Graphite = module.exports = function Graphite(options) {
  if (!options || !options.event || !options.request || !options.graphite) {
    throw new Error('options.event, options.request and options.graphite are required');
  }

  Producer.call(this, options);

  this.event     = options.event;
  this.request   = options.request;
  this.graphite  = options.graphite;
};

//
// Inherit from Producer.
//
utile.inherits(Graphite, Producer);

//
// ### function produce ()
// Emits the data for this instance
//
Graphite.prototype.produce = function () {
  var self = this
      currentRequest = this.clone(this.request);

  currentRequest.path = this.path(currentRequest);

  var data = {
    host:        this.event.host         || this.values.host,
    service:     this.event.service      || this.values.service,
    state:       this.event.state        || this.values.state,
    time:        Date.now(),
    description: this.event.description  || '',
    tags:        this.event.tags         || this.values.tags,
    metric:      this.event.metric       || this.values.metric,
    ttl:         this.event.ttl          || this.values.ttl
  };

  var httpModule = currentRequest.isSecure ? https : http;
  var request = httpModule.request(currentRequest, function(response) {
    if (response.statusCode == 200) {
      var body = '';
      response.on('data', function(chunk) {
        body += chunk;
      });
      response.on('end', function () {
        data.metric = self.summarize(body);
        return self.emit('data', data);
      });
    } else {
      data.state = 'warning';
      data.description = "http status code: " + response.statusCode;
      return self.emit('data', data);
    }
  })
  .on('error', function(e) {
    data.state = 'critical';
    data.description = utile.format("message: %s, code: %s", e.message, e.code);
    return self.emit('data', data);
  })
  .end();
}

Graphite.prototype.path = function (currentRequest) {
  var path = utile.format("%s/?format=json", currentRequest.path);
  if (this.graphite.from)  {
    path = utile.format("%s&from=%s", path, this.graphite.from);
  }
  if (this.graphite.until)  {
    path = utile.format("%s&until=%s", path, this.graphite.until);
  }
  this.graphite.targets.forEach(function(metric) {
    path = utile.format("%s&target=%s", path, metric);
  });

  return path;
}

Graphite.prototype.summarize = function (body) {
  var self = this;
  body = JSON.parse(body);
  var metrics = {};
  metrics["total"] = 0;
  body.forEach(function(target) {
    metrics[target.target] = 0;
    count = 0;

    target.datapoints.filter(function(datapoint) {
      return typeof datapoint[0] === 'number' && typeof datapoint[1] === 'number';
    })/*.slice(-self.graphite.max)*/.forEach(function (datapoint) {
      metrics[target.target] += datapoint[0];
      count++;
    });

    metrics[target.target] /= count;
    metrics["total"] += metrics[target.target];
  });
  return metrics["total"];
}

Graphite.prototype.clone = function (data) {
  return Object.keys(data).reduce(function (obj, key) {
    obj[key] = data[key];
    return obj;
  }, {});
}

