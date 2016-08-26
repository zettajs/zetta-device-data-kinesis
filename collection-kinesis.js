var util = require('util');
var Rx = require('rx');
var StatsCollector = require('zetta-device-data-collection');
var AWS = require('aws-sdk');

var DEFAULT_MAX_SIZE = 25000;

// kinesisConfig must conform with http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/Kinesis.html#constructor-property
var KinesisCollector = module.exports = function(kinesisConfig, options) {
    var self = this;
    StatsCollector.call(this);
    options = options || {};

    if (!kinesisConfig) {
        throw new Error('Must supply Kinesis config');
    }

    this.kinesis = new AWS.Kinesis(kinesisConfig);

    var windowMs = options.windowMs || 30000;

    Rx.Observable.fromEvent(this, 'event')
        .window(function() { return Rx.Observable.timer(windowMs); })
        .flatMap(function(e) { return e.toArray(); })
        .filter(function(arr) { return arr.length > 0 })
        .subscribe(function (data) {
            var groups = [[]];
            var idx = 0;
            var currentSize = 0;
            data.forEach(function(x) {
                try {
                    var len = new Buffer(JSON.stringify(x)).length;
                    if (len > DEFAULT_MAX_SIZE) {
                        return;
                    } else if (currentSize + len + 1 > DEFAULT_MAX_SIZE - 2 ) {
                        idx++;
                        currentSize = len;
                        groups[idx] = [x];
                    } else {
                        groups[idx].push(x);
                        currentSize += len;
                        if (groups[idx].length > 1) {
                            currentSize += 1;
                        }
                    }
                } catch (err) {
                }
            });

            groups.forEach(function(group, i) {
                var params = {
                    Data: JSON.stringify(group),
                };
                self.kinesis.putRecord(params, function(err, data) {
                    if (err) {
                        self.server.error('Failed to send stats to kinesis, ' + err);
                    }
                });
            });
        });
};
util.inherits(KinesisCollector, StatsCollector);