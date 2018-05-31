/*
 * http_client.js: Http client for influxdb
 *
 * (C) 2015 Brandon Hamilton
 * MIT LICENCE
 *
 */

'use strict';

var request = require('request');

var HTTPClient = function(options) {
  options = options || {}
  this.host = options.host || '127.0.0.1';
  this.port = options.port || 8086;
  this.database = options.database;
  this.user = options.user;
  this.password = options.password;
  this.precision = options.precision;
  this.timeout = options.httpTimeout || 5000;
  this.dbusername = options.dbusername,
  this.dbpassword = options.dbpassword,
  this.callback = options.callback;
  this.protocol = options.protocol;
  if (options.consistency) {
    if (['one','quorum','all','any'].indexOf(options.consistency) < 0) {
      throw new Error('Consistency must be one of [one,quorum,all,any]');
    }
    this.consistency = options.consistency;
  }
}

HTTPClient.prototype.send = function(buf, offset, length, callback) {
  callback = callback || this.callback;
  var self = this;
  var qs = '?db=' + this.database;
  if (this.dbusername) { qs += '&u=' + this.dbusername; }
  if (this.dbpassword) { qs += '&p=' + this.dbpassword; }
  if (this.precision) { qs += '&precision=' + this.precision; }
    if (this.consistency) { qs += '&consistency=' + this.consistency; }
    const reqOptions = { url: `${this.protocol}://${this.host}:${this.port}/write${qs}`,
                         method : 'POST',
                         body : buf.slice(offset, length),
                         timeout: this.timeout,
                       };
    if(this.user && this.password ) {
        reqOptions.auth = { user : this.user,
                            password : this.password,
                            sendImmediately : true}
    }
    const req = request(reqOptions).on('response', (res) => {

    if (res.statusCode == 204) {
      if (typeof(callback) == 'function') {
        callback();
      }
      return;
    }
    switch (res.statusCode) {
      case 200:
        handleSendError(res, "Problem with request", callback);
        break;
      case 401:
        handleSendError(res, "Unauthorized user", callback);
        return;
      case 400:
        handleSendError(res, "Invalid syntax in: " + buf.slice(offset, length), callback);
        return;
      default:
        handleSendError(res, "Unknown response status: " + res.statusCode, callback);
        return;
    }
  }).on('error', function(e) { if (typeof(callback) == 'function') { callback(e); } else { console.log(e); } });
}

function handleSendError(res, errorMessage, callback) {
    var data = '';
    res.on('data', function (chunk) { data += chunk; });

    res.on('end', function() {
    if (typeof(callback) == 'function') {
        callback(new Error(errorMessage + " Response: " + data));
    } else {
        console.log(errorMessage  + " Response: " + data);
    }
  });
}

HTTPClient.prototype.writePoints = function(batches) {
  for (var i = 0; i < batches.length; i++) {
    var batch = batches[i];
    var buf = new Buffer(batch.join('\n'));
    this.send(buf, 0, buf.length);
  }
}

module.exports = HTTPClient;
